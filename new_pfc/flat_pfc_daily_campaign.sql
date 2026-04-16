-- ============================================================
-- PFC 2.0 — T3: pfc_daily_funding
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS PY_PE
--   global_entity_id         : PY_PE
--   country_code             : pe
--   funding_source           : marko
--   funding_value_convention : normalized
--   missing_contract_fallback: skip
--
-- TODO: date_in / date_fin hardcodeados para validación marzo 2026.
--       En pipeline productivo derivar del scheduler.
--       T1 y T2 ya corregidos manualmente a CURRENT_DATE().
-- ============================================================

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_daily_funding`
CLUSTER BY global_entity_id, order_date, sku
AS

WITH

-- Resolución vendor → warehouse para PY_PE
-- Solo vendors activos (84 de 119 — 35 vendors cerrados se descartan naturalmente)
vendor_warehouse AS (
  SELECT DISTINCT
    qcp.global_entity_id
    , vp.catalog_global_vendor_id
    , vp.warehouse_id
  FROM `fulfillment-dwh-production.cl_dmart.qc_catalog_products` AS qcp
  LEFT JOIN UNNEST(qcp.vendor_products) AS vp
  WHERE qcp.global_entity_id = 'PY_PE'
    AND vp.is_dmart          = TRUE
    AND vp.warehouse_id      IS NOT NULL
    AND vp.warehouse_id      != ''
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
    , v.catalog_global_vendor_id
    , vw.warehouse_id
    , order_date

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

  WHERE qc.country_code = 'pe'
)

SELECT
  global_entity_id
  , country_code
  , campaign_id
  , campaign_type
  , sku
  , order_date
  , warehouse_id
  , contract_status
  , supplier_funding_type
  , supplier_funding_value
  , funding_unit_value
  , discount_type_resolved
  , discount_value_resolved
  , trigger_qty_threshold
  , benefit_qty_limit
  , CURRENT_TIMESTAMP() AS ingested_at

FROM daily_grid
WHERE warehouse_id IS NOT NULL
  AND order_date   IS NOT NULL