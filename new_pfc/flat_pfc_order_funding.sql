-- ============================================================
-- PFC 2.0 — T4: pfc_order_funding
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS (read from pfc_config)
--   param_global_entity_id : Entity code (e.g., PY_PE, TB_BH, TB_AE)
-- Parámetros universales:
--   param_date_in : 2025-01-01
--   date_fin      : CURRENT_DATE()
-- ============================================================

-- ── Params de entidad ─────────────────────────────────────────
DECLARE param_global_entity_id       STRING;
DECLARE param_date_in                DATE    DEFAULT DATE('2025-01-01');
DECLARE date_fin                     DATE    DEFAULT CURRENT_DATE();

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_order_funding`
CLUSTER BY global_entity_id, order_date, supplier_id
AS

-- Lee configuración desde pfc_config
WITH config AS (
  SELECT
    global_entity_id
    , country_code
    , join_strategy
    , require_discount_to_charge
    , missing_contract_fallback
    , funding_value_convention
    , funding_source
  FROM `dh-darkstores-live.csm_automated_tables.pfc_config`
  WHERE global_entity_id = param_global_entity_id
    AND is_active = TRUE
)

,

orders AS (
  SELECT
    qo.order_id
    , DATE(qo.order_created_date_lt)                          AS order_date
    , qo.global_entity_id
    , qo.warehouse_id
    , i.sku
    , i.quantity_sold
    , i.value_lc.unit_price_listed_lc                         AS unit_price_listed_lc
    , i.value_lc.unit_discount_lc                             AS unit_discount_lc
    , i.value_lc.unit_discount_lc > 0                         AS has_discount
    , i.value_lc.djini_order_items_supplier_funded_lc         AS funding_v1_lc
    , ci.campaign_id                                           AS campaign_id
  FROM `fulfillment-dwh-production.cl_dmart.qc_orders` AS qo
  LEFT JOIN UNNEST(qo.items) AS i
  LEFT JOIN UNNEST(i.campaign_info) AS ci
  INNER JOIN config cfg
    ON qo.global_entity_id = cfg.global_entity_id
  WHERE qo.global_entity_id                    = param_global_entity_id
    AND qo.country_code                        = cfg.country_code
    AND qo.is_dmart                            = TRUE
    AND qo.is_successful                       = TRUE
    AND qo.is_failed                           = FALSE
    AND qo.is_cancelled                        = FALSE
    AND i.quantity_sold                        > 0
    AND DATE(qo.order_created_date_lt) BETWEEN param_date_in AND date_fin
)

, supplier_info AS (
  SELECT DISTINCT
    spfc.global_entity_id
    , spfc.sku
    , spfc.warehouse_id
    , CAST(spfc.supplier_id AS STRING)  AS supplier_id
    , spfc.supplier_name
  FROM `fulfillment-dwh-production.cl_dmart._spfc_products` AS spfc
)

-- JOIN strategy: date_warehouse_sku
-- TODO: agregar rama campaign_id cuando haya país que lo requiera
, orders_with_funding AS (
  SELECT
    o.order_id
    , o.order_date
    , o.global_entity_id
    , o.warehouse_id
    , o.sku
    , o.quantity_sold
    , o.unit_price_listed_lc
    , o.unit_discount_lc
    , o.has_discount
    , o.funding_v1_lc
    , o.campaign_id
    , t3.campaign_id                AS funding_campaign_id
    , t3.campaign_type
    , t3.contract_status
    , t3.supplier_funding_type
    , t3.supplier_funding_value
    , t3.funding_unit_value
    , t3.trigger_qty_threshold
    , t3.benefit_qty_limit
    , t3.discount_type_resolved
    , t3.discount_value_resolved
    , t3.campaign_end_date
    , t3.warehouse_name
  FROM orders AS o
  LEFT JOIN `dh-darkstores-live.csm_automated_tables.pfc_daily_funding` AS t3
    ON  o.global_entity_id = t3.global_entity_id
    AND o.order_date        = t3.order_date
    AND o.warehouse_id      = t3.warehouse_id
    AND o.sku               = t3.sku
    AND (
      (SELECT join_strategy FROM config LIMIT 1) != 'campaign_id'
      OR o.campaign_id = t3.campaign_id
    )
)

-- Resolver overlaps: MAX(funding_unit_value) por order_id × sku — lógica Marko
, orders_dedup AS (
  SELECT
    order_id
    , order_date
    , global_entity_id
    , warehouse_id
    , sku
    , quantity_sold
    , unit_price_listed_lc
    , unit_discount_lc
    , has_discount
    , funding_v1_lc
    , campaign_id
    , funding_campaign_id
    , campaign_type
    , contract_status
    , supplier_funding_type
    , supplier_funding_value
    , funding_unit_value
    , trigger_qty_threshold
    , benefit_qty_limit
    , discount_type_resolved
    , discount_value_resolved
    , campaign_end_date
    , warehouse_name
    , CASE
        WHEN (SELECT require_discount_to_charge FROM config LIMIT 1) = TRUE
         AND has_discount = FALSE               THEN 0.0
        WHEN contract_status = 'missing'
         AND (SELECT missing_contract_fallback FROM config LIMIT 1) = 'skip' THEN 0.0
        WHEN contract_status = 'missing'
         AND (SELECT missing_contract_fallback FROM config LIMIT 1) = 'full_discount'
          THEN ROUND(unit_discount_lc * quantity_sold, 2)
        WHEN contract_status = 'explicit_zero'  THEN 0.0
        WHEN funding_unit_value IS NULL         THEN 0.0
        WHEN (SELECT funding_value_convention FROM config LIMIT 1) = 'normalized'
          THEN ROUND(funding_unit_value * quantity_sold, 2)
        WHEN (SELECT funding_value_convention FROM config LIMIT 1) = 'per_benefit'
          THEN ROUND(
                 funding_unit_value
                 * FLOOR(quantity_sold / NULLIF(trigger_qty_threshold, 0))
                 * benefit_qty_limit
               , 2)
        ELSE 0.0
      END AS funding_total_lc
  FROM orders_with_funding
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id, sku
    ORDER BY funding_unit_value DESC NULLS LAST
  ) = 1
)

SELECT
  d.global_entity_id
  , d.order_id
  , d.order_date
  , d.warehouse_id
  , s.supplier_id
  , s.supplier_name
  , d.sku
  , d.campaign_id
  , d.funding_campaign_id
  , qc_audit.root_id                           AS root_id
  , qc_funding.root_id                         AS funding_root_id
  , d.campaign_type
  , d.quantity_sold
  , d.unit_price_listed_lc
  , d.unit_discount_lc
  , d.has_discount
  , d.contract_status
  , d.supplier_funding_type
  , d.supplier_funding_value
  , d.funding_unit_value
  , d.trigger_qty_threshold
  , d.benefit_qty_limit
  , d.discount_type_resolved
  , d.discount_value_resolved
  , d.campaign_end_date
  , d.warehouse_name

  -- funding_total_lc — calculado en orders_dedup
  , d.funding_total_lc

  -- pfc_funding_amount_lc — switch gobernado por funding_source
  , CASE (SELECT funding_source FROM config LIMIT 1)
      WHEN 'negotiated' THEN d.funding_total_lc
      WHEN 'promotool'  THEN COALESCE(d.funding_v1_lc, 0.0)
    END AS pfc_funding_amount_lc

  -- PFC v1 para audit
  , COALESCE(d.funding_v1_lc, 0.0)             AS funding_v1_lc

  -- delta_lc: pfc_funding_amount_lc - funding_v1_lc
  , CASE (SELECT funding_source FROM config LIMIT 1)
      WHEN 'negotiated' THEN d.funding_total_lc - COALESCE(d.funding_v1_lc, 0.0)
      WHEN 'promotool'  THEN 0.0
    END AS delta_lc

  -- Flags audit
  , d.contract_status = 'missing'              AS fallback_applied
  , (SELECT join_strategy FROM config LIMIT 1)                              AS join_method_used
  , (SELECT funding_value_convention FROM config LIMIT 1)                   AS funding_value_convention_used
  , CURRENT_TIMESTAMP()                        AS ingested_at

FROM orders_dedup AS d
LEFT JOIN supplier_info AS s
  ON  d.global_entity_id = s.global_entity_id
  AND d.warehouse_id     = s.warehouse_id
  AND d.sku              = s.sku
LEFT JOIN `fulfillment-dwh-production.cl_dmart.qc_campaigns` AS qc_audit
  ON d.global_entity_id = qc_audit.global_entity_id
  AND d.campaign_id     = qc_audit.campaign_id
LEFT JOIN `fulfillment-dwh-production.cl_dmart.qc_campaigns` AS qc_funding
  ON d.global_entity_id    = qc_funding.global_entity_id
  AND d.funding_campaign_id = qc_funding.campaign_id