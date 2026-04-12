-- ============================================================
-- PFC 2.0 — T2: pfc_campaign_funding_rules
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS PY_PE
--   global_entity_id          : PY_PE
--   country_code               : pe
--   date_in                    : 2026-03-01
--   date_fin                   : 2026-03-31
--   funding_source             : marko
--   missing_contract_fallback  : skip
--
-- TODO: date_in / date_fin hardcodeados para validación marzo 2026.
--       En pipeline productivo derivar del scheduler.
-- ============================================================

DECLARE date_in  DATE DEFAULT DATE('2026-03-01');
DECLARE date_fin DATE DEFAULT DATE('2026-03-31');

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_campaign_funding_rules`
CLUSTER BY global_entity_id, campaign_id
AS

WITH campaigns_with_benefits AS (

  SELECT
    qc.global_entity_id
    , qc.country_code
    , qc.campaign_id
    , qc.campaign_type
    , qc.discount_type   AS discount_type_global
    , qc.discount_value  AS discount_value_global
    , b.sku
    , b.supplier_funding_type
    , b.supplier_funding_value
    , b.discount_type    AS discount_type_sku
    , b.discount_value   AS discount_value_sku
    , b.is_deleted
  FROM `fulfillment-dwh-production.cl_dmart.qc_campaigns` AS qc
  LEFT JOIN UNNEST(qc.benefits) AS b
  INNER JOIN `dh-darkstores-live.csm_automated_tables.pfc_campaigns_utilized` AS t1
    ON qc.global_entity_id = t1.global_entity_id
    AND qc.campaign_id     = t1.campaign_id
  WHERE qc.country_code = 'pe'
    AND qc.start_at_utc <= TIMESTAMP(date_fin)
    AND qc.end_at_utc   >= TIMESTAMP(date_in)

)

SELECT
  global_entity_id
  , country_code
  , campaign_id
  , campaign_type
  , sku
  , supplier_funding_type
  , supplier_funding_value
  , CASE
      WHEN supplier_funding_type IS NULL
       AND supplier_funding_value IS NULL  THEN 'missing'
      WHEN supplier_funding_value = 0      THEN 'explicit_zero'
      WHEN supplier_funding_type IS NOT NULL
       AND supplier_funding_value > 0      THEN 'configured'
      ELSE 'missing'
    END AS contract_status
  , CASE
      WHEN supplier_funding_type = 'ABSOLUTE'
       AND supplier_funding_value > 0  THEN supplier_funding_value
      WHEN supplier_funding_value = 0  THEN 0.0
      ELSE NULL
    END AS funding_unit_value
  , COALESCE(discount_type_sku,  discount_type_global)  AS discount_type_resolved
  , COALESCE(discount_value_sku, discount_value_global) AS discount_value_resolved
  , CURRENT_TIMESTAMP() AS ingested_at

FROM campaigns_with_benefits
WHERE sku IS NOT NULL
  AND is_deleted = FALSE
