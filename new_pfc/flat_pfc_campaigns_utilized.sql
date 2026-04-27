-- ============================================================
-- PFC 2.0 — T1: pfc_campaigns_utilized
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS: Dinámicos desde pfc_config
-- Parámetro requerido:
--   param_global_entity_id : Entity code (e.g., PY_PE, TB_BH, TB_AE)
-- Parámetros universales:
--   param_date_in : 2025-01-01
--   date_fin      : CURRENT_DATE()
-- ============================================================

-- Parámetros universales
DECLARE param_global_entity_id  STRING  DEFAULT 'PY_PE';
DECLARE param_date_in           DATE    DEFAULT DATE('2025-01-01');
DECLARE date_fin                DATE    DEFAULT CURRENT_DATE();

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_campaigns_utilized`
CLUSTER BY global_entity_id
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

, dmart_skus AS (
  SELECT DISTINCT
    qcp.global_entity_id
    , qcp.sku
  FROM `fulfillment-dwh-production.cl_dmart.qc_catalog_products` AS qcp
  LEFT JOIN UNNEST(qcp.vendor_products) AS vp
  INNER JOIN config cfg
    ON qcp.global_entity_id = cfg.global_entity_id
  WHERE vp.is_dmart = TRUE
    AND vp.warehouse_id       IS NOT NULL
    AND vp.warehouse_id       != ''
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
  , qc.trigger_qty_threshold
  , qc.benefit_qty_limit
  , CURRENT_TIMESTAMP() AS ingested_at

FROM `fulfillment-dwh-production.cl_dmart.qc_campaigns` AS qc
LEFT JOIN UNNEST(qc.benefits) AS b
INNER JOIN dmart_skus AS ds
  ON qc.global_entity_id = ds.global_entity_id
  AND b.sku               = ds.sku
INNER JOIN config cfg
  ON qc.global_entity_id = cfg.global_entity_id
WHERE qc.country_code = cfg.country_code
  AND qc.state        = 'READY'
  AND qc.is_valid     = TRUE
  AND qc.start_at_utc <= TIMESTAMP(date_fin)
  AND qc.end_at_utc   >= TIMESTAMP(param_date_in)