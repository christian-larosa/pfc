-- ============================================================
-- PFC 2.0 — T3: pfc_daily_funding
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS (read from pfc_config)
--   param_global_entity_id : Entity code (e.g., PY_PE, TB_BH, TB_AE)
-- ============================================================

DECLARE param_global_entity_id  STRING;

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_daily_funding`
CLUSTER BY global_entity_id, order_date, sku
AS

-- Lee configuración desde pfc_config
WITH config AS (
  SELECT
    global_entity_id
    , country_code
  FROM `dh-darkstores-live.csm_automated_tables.pfc_config`
  WHERE global_entity_id = param_global_entity_id
    AND is_active = TRUE
)

, vendor_warehouse AS (
  SELECT DISTINCT
    qcp.global_entity_id
    , vp.catalog_global_vendor_id
    , vp.warehouse_id
    , vp.warehouse_name
  FROM `fulfillment-dwh-production.cl_dmart.qc_catalog_products` AS qcp
  LEFT JOIN UNNEST(qcp.vendor_products) AS vp
  WHERE vp.is_dmart     = TRUE
    AND vp.warehouse_id IS NOT NULL
    AND vp.warehouse_id != ''
)

-- Expansión temporal: campaign × sku × date × warehouse
-- Una fila por cada día que la campaña estuvo activa en cada warehouse
, daily_grid AS (
  SELECT
    t2.global_entity_id
    , t2.country_code
    , t2.campaign_id
    , t2.campaign_type
    , t2.sku
    , t2.contract_status
    , t2.supplier_funding_type
    , t2.supplier_funding_value
    , t2.funding_unit_value
    , t2.discount_type_resolved
    , t2.discount_value_resolved
    , t2.trigger_qty_threshold
    , t2.benefit_qty_limit
    , vw.warehouse_id
    , vw.warehouse_name
    , order_date
    , DATE(qc.end_at_utc) AS campaign_end_date

  FROM `dh-darkstores-live.csm_automated_tables.pfc_campaign_funding_rules` AS t2

  -- Expandir vendors de la campaña
  INNER JOIN `fulfillment-dwh-production.cl_dmart.qc_campaigns` AS qc
    ON t2.global_entity_id = qc.global_entity_id
    AND t2.campaign_id     = qc.campaign_id
  LEFT JOIN UNNEST(qc.vendors) AS v

  -- Resolver vendor → warehouse (vendors cerrados quedan fuera aquí)
  INNER JOIN vendor_warehouse AS vw
    ON qc.global_entity_id         = vw.global_entity_id
    AND v.catalog_global_vendor_id = vw.catalog_global_vendor_id

  -- Expandir fechas: una fila por día activo de la campaña
  LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(
    DATE(qc.start_at_utc)
    , DATE(qc.end_at_utc)
  )) AS order_date
  INNER JOIN config cfg
    ON qc.global_entity_id = cfg.global_entity_id
  WHERE qc.country_code = cfg.country_code
)

SELECT
  global_entity_id
  , country_code
  , campaign_id
  , campaign_type
  , sku
  , order_date
  , warehouse_id
  , warehouse_name
  , contract_status
  , supplier_funding_type
  , supplier_funding_value
  , funding_unit_value
  , discount_type_resolved
  , discount_value_resolved
  , trigger_qty_threshold
  , benefit_qty_limit
  , campaign_end_date
  , CURRENT_TIMESTAMP() AS ingested_at

FROM daily_grid
WHERE warehouse_id IS NOT NULL
  AND order_date   IS NOT NULL