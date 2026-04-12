-- ============================================================
-- PFC 2.0 — T1: pfc_campaigns_utilized
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS PY_PE
--   global_entity_id : PY_PE
--   country_code     : pe
--   date_in          : 2026-03-01
--   date_fin         : 2026-03-31
--   funding_source   : marko
--   join_strategy    : date_warehouse_sku
--   billing_period   : order_date
--
-- TODO: date_in / date_fin hardcodeados para validación marzo 2026.
--       En pipeline productivo derivar del scheduler.
-- ============================================================

DECLARE date_in  DATE DEFAULT DATE('2026-03-01');
DECLARE date_fin DATE DEFAULT DATE('2026-03-31');

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_campaigns_utilized`
CLUSTER BY global_entity_id
AS

WITH dmart_skus AS (
  SELECT DISTINCT
    qcp.global_entity_id
    , qcp.sku
  FROM `fulfillment-dwh-production.cl_dmart.qc_catalog_products` AS qcp
  LEFT JOIN UNNEST(qcp.vendor_products) AS vp
  WHERE qcp.global_entity_id = 'PY_PE'
    AND vp.is_dmart = TRUE
    AND vp.warehouse_id IS NOT NULL
    AND vp.warehouse_id != ''
)

SELECT DISTINCT
  qc.global_entity_id
  , qc.country_code
  , qc.campaign_id
  , qc.root_id
  , qc.campaign_name
  , qc.campaign_type
  , qc.campaign_subtype
  , qc.discount_type
  , qc.discount_value
  , qc.start_at_utc
  , qc.end_at_utc
  , qc.state
  , qc.is_valid
  , qc.externally_funded_percentage
  , qc.external_funder
  , CURRENT_TIMESTAMP() AS ingested_at

FROM `fulfillment-dwh-production.cl_dmart.qc_campaigns` AS qc
LEFT JOIN UNNEST(qc.benefits) AS b
INNER JOIN dmart_skus AS ds
  ON qc.global_entity_id = ds.global_entity_id
  AND b.sku = ds.sku
WHERE qc.global_entity_id = 'PY_PE'
  AND qc.country_code = 'pe'
  AND qc.state = 'READY'
  AND qc.is_valid = TRUE
  AND qc.start_at_utc <= TIMESTAMP(date_fin)
  AND qc.end_at_utc   >= TIMESTAMP(date_in)
