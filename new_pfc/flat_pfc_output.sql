-- ============================================================
-- PFC 2.0 — T5: pfc_output
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS PY_PE
--   param_global_entity_id : PY_PE
--   param_country_code     : pe
--   date_in                : 2026-03-01
--   date_fin               : CURRENT_DATE()
--   billing_period         : order_date
--   show_brand             : false
--   show_warehouse         : true
--
-- TODO: date_in hardcodeado para validación marzo 2026.
--       En pipeline productivo derivar del scheduler.
-- ============================================================

DECLARE param_global_entity_id  STRING  DEFAULT 'PY_PE';
DECLARE param_country_code      STRING  DEFAULT 'pe';
DECLARE date_in                 DATE    DEFAULT DATE('2026-03-01');
DECLARE date_fin                DATE    DEFAULT CURRENT_DATE();
DECLARE param_billing_period    STRING  DEFAULT 'order_date';
DECLARE param_show_brand        BOOL    DEFAULT FALSE;
DECLARE param_show_warehouse    BOOL    DEFAULT TRUE;

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_output`
CLUSTER BY global_entity_id, billing_month, supplier_id
AS

WITH pre_agg AS (
  SELECT
    *
    , CASE WHEN param_show_warehouse = TRUE THEN warehouse_id ELSE NULL END AS warehouse_id_output
    , CASE WHEN param_show_brand     = TRUE THEN NULL         ELSE NULL END AS brand_name_output
    , DATE_TRUNC(
        CASE param_billing_period
          WHEN 'order_date' THEN order_date
          -- campaign_end_date: TODO cuando haya país que lo requiera
          -- necesita traer end_at_utc desde T3 via T4
        END
      , MONTH
      ) AS billing_month
  FROM `dh-darkstores-live.csm_automated_tables.pfc_order_funding`
  WHERE global_entity_id = param_global_entity_id
    AND order_date BETWEEN date_in AND date_fin
    AND funding_total_lc > 0  -- solo filas con funding real → credit note no incluye ceros
)

SELECT
  global_entity_id
  , billing_month
  , supplier_id
  , supplier_name
  , brand_name_output                     AS brand_name
  , warehouse_id_output                   AS warehouse_id
  , COUNT(DISTINCT order_id)              AS total_orders
  , COUNT(DISTINCT sku)                   AS total_skus
  , COUNT(DISTINCT campaign_id)           AS total_campaigns
  , ROUND(SUM(funding_total_lc), 2)       AS total_funding_v2_lc
  , ROUND(SUM(funding_v1_lc), 2)          AS total_funding_v1_lc
  , ROUND(SUM(delta_lc), 2)               AS total_delta_lc
  , COUNTIF(fallback_applied = TRUE)      AS fallback_orders
  , CURRENT_DATE()                        AS run_date
  , 'PFC_2.0'                             AS pfc_version

FROM pre_agg
GROUP BY
  global_entity_id
  , billing_month
  , supplier_id
  , supplier_name
  , brand_name_output
  , warehouse_id_output