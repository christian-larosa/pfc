# PFC 2.0 DAG Architecture & Design Decisions

**Version:** 2.0  
**Last Updated:** 2026-04-27  
**Author:** Christian La Rosa  
**Audience:** Data Engineers, Analytics, Finance Operations  

---

## Executive Summary

PFC 2.0 (Promo Funding Calculation) is a BigQuery ETL pipeline that calculates supplier funding for promotional campaigns across all QCommerce markets in a **single execution**, processing multiple countries in parallel. It replaces the legacy PFC v1 (Djini-based funding) with a flexible, configuration-driven approach that supports different funding strategies per country.

**Key Innovation:** All T1-T5 transformations read from a centralized `pfc_config` table, eliminating the need for per-country DAG runs and enabling dynamic parameter changes without code modifications.

---

## Architecture Overview

### Multi-Country Single Execution Model

```
┌─────────────────────────────────────────────────────────┐
│              pfc_config (Centralized)                   │
│  ┌──────────┬──────────┬──────────┐                     │
│  │  PY_PE   │  TB_BH   │  TB_AE   │  ← Active countries │
│  └──────────┴──────────┴──────────┘                     │
└─────────────────────────────────────────────────────────┘
           ▲            ▲             ▲
           │            │             │
    ┌──────┴────┬───────┴───┬────────┴───┐
    │           │           │            │
   T1: Campaigns_Utilized  T2: Campaign_Funding_Rules  T3: Daily_Funding
    │           │           │            │
    └──────┬────┴───────┬───┴────────┬───┘
           │           │             │
           └───────────┴──── T4: Order_Funding ────┐
                                    │              │
                                    ▼              │
                            ┌──────────────┐      │
                            │  pfc_output  │◄─────┘
                            │     (T5)     │
                            └──────────────┘
```

**Why single execution?** Each country's parameters live in `pfc_config`. T1-T5 CTEs `INNER JOIN` with `config WHERE is_active = TRUE`, so a single run processes all countries. This eliminates the old pattern of running T1 for PY_PE, T2 for PY_PE, T1 for TB_AE, T2 for TB_AE, etc.

---

## Parameters: Universal vs. Country-Specific

### Universal Parameters (Apply to All Countries)

These are **declared once** and used uniformly across all transformations:

```sql
DECLARE param_date_in DATE DEFAULT DATE('2025-01-01');
DECLARE date_fin DATE DEFAULT CURRENT_DATE();
```

- `param_date_in`: Historical backfill start date (globally consistent)
- `date_fin`: End date for the processing window (typically today)

**Business Rationale:** All countries must report on the same time horizon for consolidated reporting and reconciliation.

### Country-Specific Parameters (From pfc_config)

Each country has a row in `pfc_config` with its own funding strategy:

```sql
SELECT
  global_entity_id,        -- e.g., 'PY_PE', 'TB_BH', 'TB_AE'
  country_code,            -- e.g., 'py', 'bh', 'ae'
  join_strategy,           -- 'date_warehouse_sku' or 'campaign_id'
  require_discount_to_charge,    -- T/F: must have discount to pay funding
  missing_contract_fallback,     -- 'skip' or 'full_discount'
  funding_value_convention,      -- 'normalized' or 'per_benefit'
  funding_source,          -- 'negotiated' or 'promotool'
  param_billing_period,    -- 'order_date' or 'campaign_end_date'
  is_active
FROM pfc_config
```

**Business Rationale:** Different markets have different supplier agreements, contract terms, and operational capabilities. Centralization allows adding a country with a single config row instead of modifying code.

---

## Transformation Pipeline (T1-T5)

### T1: pfc_campaigns_utilized

**Purpose:** Identify which campaigns have DMART SKUs and extract campaign-level metadata.

**Key Logic:**
```sql
WITH config AS (SELECT * FROM pfc_config WHERE is_active = TRUE)
, dmart_skus AS (...)  -- SKUs available in DMART for each country
, campaigns_with_benefits AS (...)  -- Campaigns with supplier funding + benefits
SELECT DISTINCT campaign_id, sku, trigger_qty_threshold, benefit_qty_limit
FROM campaigns INNER JOIN dmart_skus  -- Only campaigns with DMART SKUs
```

**Output:** ~36K (PY_PE), ~72K (TB_AE), ~3K (TB_BH) rows per country, partitioned by campaign × country.

**Business Decision:** Only processes `state = 'READY'` campaigns to exclude drafts and failed creations.

---

### T2: pfc_campaign_funding_rules

**Purpose:** Calculate contract status and funding unit values per campaign × SKU.

**Key Logic:**
```sql
-- Resolve contract status
CASE
  WHEN supplier_funding_type IS NULL THEN 'missing'      -- No contract
  WHEN supplier_funding_value = 0 THEN 'explicit_zero'   -- Zero contract
  WHEN supplier_funding_type = 'ABSOLUTE' THEN 'configured'
  ELSE 'missing'
END AS contract_status

-- Extract funding unit value (only for ABSOLUTE type)
CASE
  WHEN supplier_funding_type = 'ABSOLUTE' AND supplier_funding_value > 0
    THEN supplier_funding_value
  ELSE NULL
END AS funding_unit_value
```

**Output:** ~4.9M (PY_PE), ~66M (TB_AE), ~1.3M (TB_BH) rows (expanded by benefit SKUs).

**Business Decision:** Only processes benefits where `is_deleted = FALSE` to respect campaign deletions.

---

### T3: pfc_daily_funding

**Purpose:** Create a daily temporal grid: campaign × SKU × warehouse × date.

**Key Logic:**
```sql
-- Expand by vendor_products and campaign vendors
INNER JOIN vendor_warehouse USING (vendor_id)

-- Expand by date (one row per campaign-active day)
LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(campaign_start, campaign_end)) AS order_date

-- Result: one row per (campaign, sku, warehouse, date) combination
```

**Output:** ~69M (PY_PE), ~1.3B (TB_AE), ~209M (TB_BH) rows (massive due to date expansion).

**Business Decision:** This grid enables the LEFT JOIN in T4 to be **deterministic and efficient**—instead of risk subqueries or self-joins, we simply match (date, warehouse, sku). The temporal dimension is pre-built.

---

### T4: pfc_order_funding ⭐ (Most Complex)

**Purpose:** Merge orders with campaign funding and calculate final compensation per order line.

**Architecture:**

**Orders CTE:** Extract order items, discounts, and legacy v1 funding
```sql
SELECT order_id, sku, quantity_sold, unit_price_listed_lc, unit_discount_lc, funding_v1_lc
FROM qc_orders INNER JOIN config  -- Filter by country
```

**Orders with Funding CTE:** LEFT JOIN T3 to attach campaign metadata
```sql
SELECT o.*, t3.funding_unit_value, t3.trigger_qty_threshold, t3.benefit_qty_limit, ...
FROM orders o
LEFT JOIN config cfg  -- Pass config through pipeline
LEFT JOIN pfc_daily_funding t3  -- Campaign match
WHERE (cfg.join_strategy != 'campaign_id' OR o.campaign_id = t3.campaign_id)
```

**Orders Dedup CTE:** De-duplicate overlapping campaigns (take MAX funding) and calculate funding
```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, sku ORDER BY funding_unit_value DESC) = 1

-- Calculate funding_total_lc based on funding_value_convention + funding_source
CASE
  WHEN require_discount_to_charge AND has_discount = FALSE THEN 0.0
  WHEN contract_status = 'missing' AND missing_contract_fallback = 'skip' THEN 0.0
  WHEN contract_status = 'missing' AND missing_contract_fallback = 'full_discount'
    THEN unit_discount_lc * quantity_sold
  WHEN contract_status = 'explicit_zero' THEN 0.0
  WHEN funding_unit_value IS NULL THEN 0.0
  WHEN funding_value_convention = 'normalized'
    THEN funding_unit_value * quantity_sold
  WHEN funding_value_convention = 'per_benefit'
    THEN funding_unit_value * LEAST(
      FLOOR(quantity_sold / trigger_qty_threshold),
      COALESCE(NULLIF(benefit_qty_limit, 0), FLOOR(quantity_sold / trigger_qty_threshold))
    )
  ELSE 0.0
END AS funding_total_lc
```

**Final SELECT:** Switch on funding_source
```sql
CASE d.funding_source
  WHEN 'negotiated' THEN d.funding_total_lc        -- T4 calculated
  WHEN 'promotool'  THEN COALESCE(d.funding_v1_lc, 0.0)  -- Legacy v1
END AS pfc_funding_amount_lc
```

**Key Fields Added:**
- `net_sales_lc`: `(unit_price_listed_lc - unit_discount_lc) * quantity_sold` — for financial reconciliation
- `campaign_subtype`: Type of campaign (Strikethrough, SameItemBundle, etc.)
- `funding_total_lc`: Calculated funding before source switching
- `pfc_funding_amount_lc`: Final funding amount (post-source decision)
- `delta_lc`: Difference between v2 and v1 funding

**Output:** ~10.3M (PY_PE), ~123.7M (TB_AE), ~24.8M (TB_BH) rows at order item level.

**Business Decisions:**

1. **Join Strategy:** `date_warehouse_sku` (default) vs. `campaign_id` (explicit matching)
   - Default: Allows campaign matches even if `campaign_id` is NULL or different (handles legacy/unmapped orders)
   - `campaign_id`: Stricter matching for markets requiring explicit campaign linkage

2. **Discount Requirement:** `require_discount_to_charge`
   - PY_PE: TRUE → Only orders with visible discount get funding (marketing requirement)
   - TB_BH, TB_AE: TRUE → Same requirement (supplier agreements)

3. **Missing Contract Fallback:** `missing_contract_fallback`
   - `'skip'`: No funding if contract missing (strict)
   - `'full_discount'`: Fallback to full discount amount (lenient)

4. **Funding Value Convention:**
   - `'normalized'`: Per-unit payment (suitable for most suppliers)
   - `'per_benefit'`: Threshold-based (buy 2+ get 1 free = fund only "free" units)
     - Formula: `unit_value × LEAST(blocks, limit)` where:
       - `blocks = FLOOR(qty_sold / trigger_qty_threshold)` — how many times trigger met
       - `limit = benefit_qty_limit` — max units eligible per order
       - If `benefit_qty_limit = 0`: Treat as "no limit" → use all blocks

5. **Funding Source:** `'negotiated'` vs. `'promotool'`
   - `'negotiated'`: T4 calculates funding via contracts in T2 (future-facing, contract-based)
   - `'promotool'`: Falls back to legacy Djini v1_lc (transition period, operational funding)

---

### T5: pfc_output

**Purpose:** Aggregate T4 to monthly billing statements by (country, supplier, billing_month).

**Key Logic:**
```sql
-- Determine billing period (order-date or campaign-end-date driven)
CASE cfg.param_billing_period
  WHEN 'order_date' THEN pof.order_date
  WHEN 'campaign_end_date' THEN pof.campaign_end_date
END AS billing_month  → DATE_TRUNC(..., MONTH)

-- Filter to funded rows only (pfc_funding_amount_lc > 0)
WHERE pfc_funding_amount_lc > 0  -- Prevents credit notes with 0 amounts

-- Aggregate
GROUP BY global_entity_id, billing_month, supplier_id, supplier_name, warehouse_id, warehouse_name
```

**Outputs:**
- `total_funding_v2_lc`: Sum of pfc_funding_amount_lc (new T4 calculation)
- `total_funding_v1_lc`: Sum of legacy v1_lc (audit trail)
- `total_delta_lc`: Difference (v2 - v1) for reconciliation
- `total_orders`, `total_skus`, `total_campaigns`: Cardinality for validation

**Business Rationale:** Monthly billing cadence aligns with supplier invoicing cycles. V1 preservation enables side-by-side reconciliation with legacy system during migration.

---

## Funding Formula Breakdown

### Normalized (Per-Unit)

```
funding = unit_value × quantity_sold

Example:
- Supplier funds 0.50 per unit
- Customer buys 10 units
- Funding = 0.50 × 10 = 5.00
```

**When:** Standard supplier agreements, straightforward per-unit compensation.

### Per-Benefit (Threshold-Based)

```
funding = unit_value × LEAST(
  FLOOR(quantity_sold / trigger_qty_threshold),
  COALESCE(NULLIF(benefit_qty_limit, 0), FLOOR(quantity_sold / trigger_qty_threshold))
)

Example (3x2 = buy 3 pay for 2):
- trigger_qty_threshold = 3 (need 3 to activate)
- benefit_qty_limit = 1 (fund 1 free unit)
- unit_value = price_of_1_unit
- Customer buys 8 units

Blocks = FLOOR(8 / 3) = 2 (can do the 3x2 twice)
Limit = 1 (only 1 per transaction)
Result = LEAST(2, 1) = 1 block funded

funding = unit_value × 1
```

**When:** Promotional mechanics with min purchase thresholds (BOGO, bundles, etc.). Each "block" is one threshold met; benefit_qty_limit caps how many times it applies per order.

**Special Case:** If `benefit_qty_limit = 0` (not filled in):
```
Treated as "no limit" → use all blocks
LEAST(2, NULL) evaluates as LEAST(2, 2) = 2
```

---

## Data Flow & Dependencies

```
┌─────────────────────────────────────────────────────────────────┐
│ Source Systems                                                  │
├─────────────────────────────────────────────────────────────────┤
│ qc_orders         → Order headers + items + discounts + v1_lc   │
│ qc_campaigns      → Campaign metadata + benefits + triggers     │
│ _spfc_products    → Supplier info (supplier_id, supplier_name)  │
│ qc_catalog_products → Vendor-product mappings                  │
└─────────────────────────────────────────────────────────────────┘
                          ↓
                    ┌─────────────┐
                    │ pfc_config  │ (Centralized parameters)
                    └─────────────┘
                          ↓
    ┌─────────────────────┼─────────────────────┐
    │                     │                     │
   T1                    T2                    T3
   ↓                     ↓                     ↓
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐
│campaigns_    │  │campaign_funding_ │  │daily_funding │
│utilized      │  │rules             │  │              │
└──────────────┘  └──────────────────┘  └──────────────┘
    │                     │                     │
    └─────────────────────┼─────────────────────┘
                          ↓
                    ┌─────────────┐
                    │qc_orders    │
                    │(copy)       │
                    └─────────────┘
                          ↓
                         T4
                          ↓
                    ┌──────────────┐
                    │order_funding │
                    └──────────────┘
                          ↓
                         T5
                          ↓
                    ┌──────────────┐
                    │pfc_output    │
                    │(Billing)     │
                    └──────────────┘
```

---

## Configuration Example: TB_BH Case Study

**Problem:** TB_BH campaigns had 99.96% with `benefit_qty_limit = 0`, causing all per_benefit calculations to multiply by 0 → zero funding.

**Root Cause:** Campaign configuration in Promo Tool didn't populate benefit_qty_limit for these campaigns.

**Solution:** Treat `benefit_qty_limit = 0` as "no limit"
```sql
COALESCE(NULLIF(benefit_qty_limit, 0), FLOOR(quantity_sold / trigger_qty_threshold))
```

**Config for TB_BH:**
```
global_entity_id: TB_BH
funding_value_convention: 'per_benefit'  -- Uses threshold logic
funding_source: 'negotiated'             -- T4 calculates (not v1_lc)
benefit_qty_limit = 0 → Interpreted as: use all blocks that meet threshold
```

**Result:**
- Before: 24.8M orders, 0 with funding_total_lc > 0
- After: 24.8M orders, 4.6M with funding_total_lc > 0, total: 1.3M LTC

---

## Deployment & Execution

### Schedule
- **Frequency:** Daily (via Airflow / Cloud Scheduler)
- **Window:** Processes all data from `param_date_in` (2025-01-01) to `date_fin` (CURRENT_DATE)
- **Duration:** ~2-3 minutes (parallel across 3 countries)

### Cluster Configuration
```sql
CREATE OR REPLACE TABLE pfc_order_funding
CLUSTER BY global_entity_id, order_date, supplier_id
```
- **Why:** Queries typically filter by country + date range + supplier, so clustering on these keys reduces scan costs.
- **Scale:** 160M+ rows, but CLUSTER BY limits scans to relevant partitions.

### Idempotency
- All tables are `CREATE OR REPLACE`, so re-running is safe.
- No incremental logic; always processes full window.
- Good for backfills and fixing historical data.

---

## Monitoring & Validation

### Key Metrics (post-run)

| Metric | Expected | Red Flag |
|--------|----------|----------|
| Total rows (T4) | ~160M | <100M (major data loss) |
| Rows with funding_total_lc > 0 | 25-30% | <15% (contract issue) |
| funding_v1_lc sum | ~8-80M per country | 0 (legacy system failure) |
| delta_lc (v2 - v1) | ±20% | >50% delta (reconciliation risk) |
| billing_rows (T5) | 10-100K | <1K (aggregation issue) |

### Audit Trail
- `funding_v1_lc`: Always preserved for reconciliation
- `delta_lc`: Signals shift from legacy to new methodology
- `contract_status`: Flags 'missing' contracts for ops review
- `join_method_used`, `funding_value_convention_used`: Metadata for debugging

---

## Future Enhancements

1. **Campaign Exclusion List:** Config parameter to blacklist campaigns by ID (damaged/fraudulent)
2. **Dynamic Thresholds:** Allow trigger_qty_threshold and benefit_qty_limit to vary by date
3. **Multi-Tier Benefits:** Support stacked discounts (buy 2 get 10%, buy 5 get 20%)
4. **VAT Handling:** Add country-specific VAT logic (included vs. excluded in funding)
5. **Incremental Refresh:** Process only new/changed orders (reduces runtime for stable historical data)

---

## Contacts & Escalations

- **Data Engineering:** Christian La Rosa (christian.la@deliveryhero.com)
- **Finance Ops:** Brenda (for onboarding checklist)
- **Promo Tool Team:** For campaign/benefit metadata issues
- **Fulfillment DWH:** For qc_orders / qc_campaigns schema changes

---

## Appendix: SQL Patterns Used

### INNER JOIN with Config for Multi-Country Processing
```sql
WITH config AS (
  SELECT * FROM pfc_config WHERE is_active = TRUE
)
SELECT ...
FROM source_table st
INNER JOIN config cfg ON st.global_entity_id = cfg.global_entity_id
WHERE st.country_code = cfg.country_code
```
Result: Automatic filtering + parameter access. Single run processes all active countries.

### Conditional Logic Based on Config
```sql
CASE
  WHEN cfg.funding_source = 'promotool' THEN v1_amount
  WHEN cfg.funding_value_convention = 'normalized' THEN unit_value * qty
  WHEN cfg.funding_value_convention = 'per_benefit' THEN unit_value * LEAST(...)
  ELSE 0.0
END
```
Result: Config parameter drives calculation entirely. No hardcoded logic per country.

### De-duplication with QUALIFY
```sql
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id, sku
  ORDER BY funding_unit_value DESC NULLS LAST
) = 1
```
Result: When multiple campaigns match same order+sku, pick highest funding (Marko's logic).

---

**Document Version:** 1.0  
**Last Reviewed:** 2026-04-27  
**Next Review:** 2026-06-27 (post-first-month-live)
