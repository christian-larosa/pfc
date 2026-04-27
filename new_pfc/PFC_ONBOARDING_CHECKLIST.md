# PFC 2.0 Country Onboarding Checklist

**Purpose:** Questions for Finance & Ops teams when onboarding a new country into PFC 2.0.  
**Owner:** Brenda (Finance Operations)  
**Used By:** Implementation team to configure `pfc_config` table  
**Last Updated:** 2026-04-27

---

## Overview

This checklist ensures that before a country enters PFC 2.0, Finance & Ops have mapped their promotional funding process. The answers directly determine country-specific parameters in `pfc_config`.

---

## Section 1: Promotion Setup & Tools

### Question 1.1: Do you manage sell-out based promotions?
**Context:** PFC is designed for supplier-funded promotions where a supplier pays for customer discount (sell-out).  
**Impact:** If answer is NO, this country may not be a candidate for PFC.  
**Config Fields:** N/A (governance question)  

**Options:**
- [ ] Yes, we manage sell-out promotions
- [ ] No, promotions are managed differently
- [ ] Partial (mix of sell-out and other models)

**Notes:**
```
_____________________________________________________________________
```

---

### Question 1.2: Do you populate supplier funding in your promo tool?
**Context:** PFC sources funding from campaign benefits in the Promo Tool (supplier_funding_type, supplier_funding_value).  
**Impact:** If NO, we fall back to `funding_source = 'promotool'` (legacy v1_lc), not T4 calculations.  
**Config Fields:** `funding_source` (negotiated vs. promotool)  

**Options:**
- [ ] Yes, suppliers enter funding in Promo Tool at campaign creation
- [ ] No, funding is tracked externally (SRM, spreadsheet, etc.)
- [ ] Partial (some campaigns have funding, others don't)

**If YES:** What data quality checks do you have for supplier funding entries?
```
_____________________________________________________________________
```

**If NO:** How do you currently track and calculate supplier compensation?
```
_____________________________________________________________________
```

---

### Question 1.3: Which systems do you use to track promo funding?
**Context:** Understanding the tool landscape affects data source decisions.  
**Impact:** Determines if we need additional data integrations.  
**Config Fields:** N/A (informational)  

**Checkboxes (select all that apply):**
- [ ] Promo Tool (campaign benefits)
- [ ] SRM (Supplier Relationship Management)
- [ ] SCMD (Campaign Management)
- [ ] Spreadsheets / manual tracking
- [ ] Other: ________________________

**Primary System:** ________________________  
**Backup System:** ________________________

**Notes:**
```
_____________________________________________________________________
```

---

## Section 2: Document Management

### Question 2.1: What document type do you issue to request promo funding?
**Context:** Legal/billing document that gets sent to supplier.  
**Impact:** Affects how we group/label the output in pfc_output.  
**Config Fields:** N/A (output documentation)  

**Options:**
- [ ] Debit Note (standard)
- [ ] Invoice
- [ ] Claim Form
- [ ] Credit Note
- [ ] Other: ________________________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 2.2: Does the document follow a fixed numbering sequence?
**Context:** Determines if we need to generate sequential document IDs or inherit from external system.  
**Impact:** T5 output may need to include document numbering logic.  
**Config Fields:** N/A (output generation)  

**Options:**
- [ ] Yes, fixed sequence (e.g., DNxxxx-001, DNxxxx-002)
- [ ] No, generated externally
- [ ] Not applicable, we don't issue formal documents

**If YES, format:** ________________________

**Starting number:** ________________________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 2.3: Does the supplier need to sign/stamp the document before returning it?
**Context:** Process requirement that affects workflow outside of PFC (PFC just generates the document).  
**Impact:** Informational for ops team; no config change needed.  
**Config Fields:** N/A  

**Options:**
- [ ] Yes, document must be signed/stamped before acceptance
- [ ] No, document is accepted as-is
- [ ] Depends on supplier tier

**If depends on tier:** Which suppliers require signatures?
```
_____________________________________________________________________
```

**Notes:**
```
_____________________________________________________________________
```

---

## Section 3: Supplier Classification

### Question 3.1: Do you work with suppliers offering QC-as-a-service?
**Context:** Some suppliers manage their own inventory/ordering (fulfillment partner model).  
**Impact:** These suppliers may need different funding rules (flat fee vs. per-item).  
**Config Fields:** May need separate `pfc_config` rows per supplier class  

**Options:**
- [ ] Yes, we have QC-as-a-service suppliers
- [ ] No, all suppliers are traditional
- [ ] Partial, only specific categories

**If YES, approximate % of spend:** ____________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 3.2: If yes, how do you flag these accounts in your systems?
**Context:** Need to identify QC-as-service suppliers in the data.  
**Impact:** May need custom filters in T1-T3 or separate funding logic.  
**Config Fields:** Additional WHERE clause filters in config  

**System that flags them:** ________________________  
**Flag name/field:** ________________________  
**Flag value:** ________________________

**Example:** `supplier.service_type = 'QC_AS_SERVICE'`

**Notes:**
```
_____________________________________________________________________
```

---

## Section 4: Roles & Responsibilities

### Question 4.1: Who calculates the total compensation amount?
**Context:** Determines who "owns" the calculation and how T4 output is used.  
**Impact:** May reveal if we're calculating per spec or if there are manual adjustments post-PFC.  
**Config Fields:** N/A (ownership documentation)  

**Options:**
- [ ] Finance team (manual spreadsheet)
- [ ] Analytics team (using data pipeline)
- [ ] Promo Tool (automated at campaign level)
- [ ] Supplier (self-reported)
- [ ] Hybrid (multiple parties, different campaigns)

**Current process owner:** ________________________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 4.2: Who creates and tracks the funding claim?
**Context:** Who is responsible for issuance and follow-up.  
**Impact:** Determines if PFC T5 output is sufficient or if downstream systems are needed.  
**Config Fields:** N/A (process ownership)  

**Person/Team:** ________________________

**System used to track:** ________________________

**SLA (turnaround time):** ________________________

**Notes:**
```
_____________________________________________________________________
```

---

## Section 5: Process Exceptions

### Question 5.1: Do some suppliers have exceptions to your standard funding process?
**Context:** Not all suppliers follow the same rules.  
**Impact:** May need custom funding_source or funding_value_convention per supplier.  
**Config Fields:** May add supplier-level overrides to pfc_config  

**Options:**
- [ ] Yes, we have exceptions (describe below)
- [ ] No, all suppliers follow the same process
- [ ] TBD, need to audit

**Count of affected suppliers:** ____________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 5.2: If yes, please describe the exceptions and affected suppliers
**Context:** Specific exceptions that need to be coded.  
**Impact:** Directly affects `pfc_config` design.  
**Config Fields:** May need `exception_type`, `exception_supplier_id` columns  

| Supplier(s) | Exception Type | Funding Rule | Notes |
|-------------|----------------|-------------|-------|
| | | | |
| | | | |
| | | | |

**Example Exception Type:**
- No discount requirement (vs. require_discount_to_charge=TRUE)
- Flat fee per campaign (not per-item)
- Quarterly billing (not monthly)
- Capped at X amount per month
- Excluded SKU categories

**Notes:**
```
_____________________________________________________________________
```

---

## Section 6: Claiming Cadence & Method

### Question 6.1: How often do you claim funding?
**Context:** Determines the `param_billing_period` aggregation level.  
**Impact:** Drives whether T5 is monthly, quarterly, annual, or campaign-end-triggered.  
**Config Fields:** `param_billing_period`  

**Options:**
- [ ] Monthly (standard)
- [ ] Quarterly
- [ ] Annually
- [ ] Per campaign end
- [ ] Ad-hoc / as-needed

**Selected frequency:** ________________________

**If monthly:** Do all months follow the same calendar (e.g., 1st to last day)?
- [ ] Yes
- [ ] No, we use a different period (describe):
```
_____________________________________________________________________
```

**Notes:**
```
_____________________________________________________________________
```

---

### Question 6.2: How do you group claims?
**Context:** Determines aggregation level in T5.  
**Impact:** May need to aggregate by supplier, brand, category, or all combined.  
**Config Fields:** T5 GROUP BY dimensions  

**Options (select all that apply):**
- [ ] One claim per supplier per period
- [ ] One claim per brand per supplier per period
- [ ] One claim per category per period
- [ ] One claim per campaign
- [ ] Consolidated (all suppliers/brands into one document)

**Current grouping logic:**
```
_____________________________________________________________________
```

---

### Question 6.3: For multi-month campaigns, do you claim monthly or only at campaign end?
**Context:** Campaign duration vs. billing frequency alignment.  
**Impact:** Determines if we use `order_date` (monthly snapshot) or `campaign_end_date` (wait for campaign completion) for billing_period.  
**Config Fields:** `param_billing_period`  

**Options:**
- [ ] Monthly (claim every month while campaign is active)
- [ ] Campaign end only (claim after campaign closes)
- [ ] Campaign end, but issue advance monthly estimates
- [ ] Depends on campaign duration (describe):
```
_____________________________________________________________________
```

**Notes:**
```
_____________________________________________________________________
```

---

## Section 7: Document & Sales Tracking

### Question 7.1: Can you send one document per campaign, or must you consolidate all into one?
**Context:** Whether T5 output (one row per supplier/month) suffices or needs further consolidation.  
**Impact:** May need downstream consolidation logic.  
**Config Fields:** N/A (output formatting)  

**Options:**
- [ ] One document per campaign (preferred)
- [ ] Must consolidate all campaigns into one document per supplier per period
- [ ] Hybrid (some campaigns consolidated, others separate)

**Notes:**
```
_____________________________________________________________________
```

---

### Question 7.2: How do you gather total quantity sold per campaign?
**Context:** Validates that our qty_sold from qc_orders matches your records.  
**Impact:** May reveal data quality gaps or mapping issues.  
**Config Fields:** N/A (validation)  

**System of record:** ________________________

**Calculation method:**
```
_____________________________________________________________________
```

**Validation approach:** (Do you reconcile against supplier data?)
- [ ] Yes, we reconcile with supplier POS data
- [ ] No, we trust qc_orders as source of truth
- [ ] Yes, reconcile with Promo Tool data
- [ ] Other: ________________________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 7.3: How do you link customer orders to promotions?
**Context:** PFC uses campaign_id from qc_orders.campaign_info. Some markets may not have this populated.  
**Impact:** Determines join_strategy (date_warehouse_sku vs. campaign_id).  
**Config Fields:** `join_strategy`  

**Options:**
- [ ] campaign_id from Promo Tool (required match)
- [ ] date + warehouse + sku (fuzzy match, no explicit campaign_id)
- [ ] Both (try campaign_id first, fall back to date_warehouse_sku)
- [ ] External mapping table

**If using mapping table, location:** ________________________

**Data quality:** What % of orders have campaign_id populated?
- [ ] >90% (use campaign_id join)
- [ ] 50-90% (use hybrid/fuzzy join)
- [ ] <50% (use date_warehouse_sku only)

**Config decision:** join_strategy = ________________________

**Notes:**
```
_____________________________________________________________________
```

---

## Section 8: VAT & Taxation

### Question 8.1: Do you include VAT in the funding document?
**Context:** Affects how total compensation is calculated and reported.  
**Impact:** May need to add VAT amount to T5 output.  
**Config Fields:** `include_vat` (TBD, new field if needed)  

**Options:**
- [ ] Yes, include VAT in compensation amount
- [ ] No, exclude VAT (pre-tax)
- [ ] Depends on supplier type (describe):
```
_____________________________________________________________________
```

---

### Question 8.2: If yes, how is VAT calculated?
**Context:** Tax rules vary by country and supplier status.  
**Impact:** Determines VAT formula in T5.  
**Config Fields:** `vat_rate`, `vat_treatment`  

**Options:**
- [ ] Fixed VAT rate (%) applied to base amount
- [ ] Reverse charge (supplier calculates their own VAT)
- [ ] Exempt (supplier is tax-exempt)
- [ ] Tiered by supplier classification

**VAT rate (if fixed):** ____________ %

**VAT basis (if not total compensation):**
- [ ] Compensation only
- [ ] Compensation + discount
- [ ] Compensation only (exclude promos)
- [ ] Other: ________________________

**Notes:**
```
_____________________________________________________________________
```

---

### Question 8.3: If total compensation, what is the VAT basis?
**Context:** Whether VAT is on the full compensation or only part of it.  
**Impact:** Changes T5 calculation.  
**Config Fields:** `vat_basis`  

**Options:**
- [ ] Full pfc_funding_amount_lc
- [ ] Base without certain adjustments (specify):
```
_____________________________________________________________________
```

---

### Question 8.4: If other, please explain your VAT approach
**Notes:**
```
_____________________________________________________________________
```

---

## Section 9: Compensation & Quantity Management

### Question 9.1: Do you need to adjust unit compensation per SKU over time?
**Context:** Supplier funding rates may change mid-campaign or seasonally.  
**Impact:** May need to support time-based funding tiers.  
**Config Fields:** `allow_dynamic_unit_value` (TBD)  

**Options:**
- [ ] Yes, we adjust funding mid-campaign
- [ ] No, rates are fixed per campaign
- [ ] Rarely, only for exceptions

**How often do adjustments occur?** ________________________

**Use case:**
```
_____________________________________________________________________
```

---

### Question 9.2: Do you need to adjust sold quantities over time?
**Context:** Refunds, returns, or inventory corrections may change the qty sold after initial claim.  
**Impact:** May need to support funding recalculations / credit notes.  
**Config Fields:** `allow_qty_recalculation` (TBD)  

**Options:**
- [ ] Yes, we adjust quantities post-claim (returns, corrections)
- [ ] No, quantities are final once claimed
- [ ] Yes, but rare (exceptions only)

**Typical adjustment scenario:**
```
_____________________________________________________________________
```

**Current process:** (How do you currently handle adjustments?)
```
_____________________________________________________________________
```

---

### Question 9.3: If yes, what's the use case? (e.g., returns, corrections, inventory adjustments)

| Use Case | Frequency | Approval Required? | Deadline |
|----------|-----------|-------------------|----------|
| Returns | | | |
| Corrections | | | |
| Inventory Adj | | | |
| Other: | | | |

**Notes:**
```
_____________________________________________________________________
```

---

## Section 10: Supplier & Brand Segmentation

### Question 10.1: Do you split compensation documents by brand for large suppliers/distributors?
**Context:** Large suppliers like P&G may distribute products across brands. Do they want separate documents per brand?  
**Impact:** May need to add brand dimension to T5 GROUP BY.  
**Config Fields:** `split_by_brand` (TBD)  

**Options:**
- [ ] Yes, always split by brand
- [ ] No, one document per supplier regardless of brands
- [ ] Only for suppliers above X size

**Largest supplier count of brands:** ____________

**Why split?** (Invoice/accounting requirement, or supplier preference?)
```
_____________________________________________________________________
```

**Notes:**
```
_____________________________________________________________________
```

---

## Section 11: Campaign Modifications

### Question 11.1: When a campaign ends early, when must the funding document be created?
**Context:** Early end = campaign is stopped before originally planned end date.  
**Impact:** May need to recalculate and issue credit note if funding was based on projected quantities.  
**Config Fields:** `early_end_policy` (TBD)  

**Options:**
- [ ] Immediately when campaign ends (ASAP)
- [ ] On the originally planned end date (no change to timeline)
- [ ] At next billing cycle
- [ ] Depends on reason for early end (describe):
```
_____________________________________________________________________
```

**Current process:**
```
_____________________________________________________________________
```

---

### Question 11.2: When a campaign is extended, what's your funding document timeline?
**Context:** Extended = campaign is active longer than originally planned.  
**Impact:** May need to issue supplemental claim or extend billing.  
**Config Fields:** `extension_policy` (TBD)  

**Options:**
- [ ] Issue supplemental claim immediately upon extension
- [ ] Include extended period in next billing cycle
- [ ] Wait until campaign actually ends, then claim for full period
- [ ] Depends on extension length (describe):
```
_____________________________________________________________________
```

**Current process:**
```
_____________________________________________________________________
```

---

## Section 12: References & Templates

### Question 12.1: Link to calculation spreadsheet example
**Context:** Sample of how compensation is currently calculated (for validation/reverse-engineering).  
**Impact:** Helps align PFC formula with actual business logic.  
**Config Fields:** N/A (reference)  

**Link:** ________________________

**Last updated:** ________________________

**Owner:** ________________________

**Accessible to:** [ ] Engineering  [ ] Finance  [ ] Both

---

### Question 12.2: Link to document template
**Context:** The actual debit note/invoice/claim form that PFC output should populate.  
**Impact:** Ensures T5 output has all required fields.  
**Config Fields:** N/A (reference)  

**Link:** ________________________

**Last updated:** ________________________

**Owner:** ________________________

**Required fields:**
```
_____________________________________________________________________
```

---

## Sign-Off

**Country:** ________________________  
**Date Completed:** ________________________  

**Finance Lead Name:** ________________________  
**Signature:** ________________________  

**Ops Lead Name:** ________________________  
**Signature:** ________________________  

**Implementation Engineer:** ________________________  
**Signature:** ________________________  

**Notes/Decisions Made:**
```
_____________________________________________________________________
_____________________________________________________________________
_____________________________________________________________________
```

---

## Implementation Checklist (For Engineering)

Once the above is completed, use the answers to populate `pfc_config`:

- [ ] `join_strategy` = ________________________
- [ ] `require_discount_to_charge` = ________________________
- [ ] `missing_contract_fallback` = ________________________
- [ ] `funding_value_convention` = ________________________
- [ ] `funding_source` = ________________________
- [ ] `param_billing_period` = ________________________
- [ ] Any supplier-specific exceptions documented and coded
- [ ] VAT logic (if needed) integrated into T5
- [ ] Test T1-T5 on 2 weeks of sample data
- [ ] Reconcile T4/T5 output against manual calculations
- [ ] Finance sign-off on first-month output before production

---

**Document Version:** 1.0  
**Last Updated:** 2026-04-27  
**Next Review:** After first country onboarding complete
