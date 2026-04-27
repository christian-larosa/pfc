-- ============================================================
-- PFC 2.0 — T5: pfc_output
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- PARAMS (read from pfc_config)
--   param_global_entity_id : Entity code (e.g., PY_PE, TB_BH, TB_AE)
-- Parámetros universales:
--   date_in                : 2025-01-01
--   date_fin               : CURRENT_DATE()
-- ============================================================

DECLARE param_global_entity_id  STRING;
DECLARE date_in                 DATE    DEFAULT DATE('2025-01-01');
DECLARE date_fin                DATE    DEFAULT CURRENT_DATE();

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_output`
CLUSTER BY global_entity_id, billing_month, supplier_id
AS

-- Lee configuración desde pfc_config
WITH config AS (
  SELECT
    global_entity_id
    , billing_period
  FROM `dh-darkstores-live.csm_automated_tables.pfc_config`
  WHERE global_entity_id = param_global_entity_id
    AND is_active = TRUE
)

, pre_agg AS (
  SELECT
    *
    , warehouse_id AS warehouse_id_output
    , warehouse_name AS warehouse_name_output
    , brand_name AS brand_name_output
    , DATE_TRUNC(
        CASE (SELECT billing_period FROM config LIMIT 1)
          WHEN 'order_date'        THEN order_date
          WHEN 'campaign_end_date' THEN campaign_end_date
        END
      , MONTH
      ) AS billing_month
  FROM `dh-darkstores-live.csm_automated_tables.pfc_order_funding`
  WHERE global_entity_id = param_global_entity_id
    AND CASE (SELECT billing_period FROM config LIMIT 1)
          WHEN 'order_date'        THEN order_date
          WHEN 'campaign_end_date' THEN campaign_end_date
        END BETWEEN date_in AND date_fin
    AND pfc_funding_amount_lc > 0  -- solo filas con funding real → credit note no incluye ceros
)

SELECT
  global_entity_id
  , billing_month
  , supplier_id
  , supplier_name
  , brand_name_output                     AS brand_name
  , warehouse_id_output                   AS warehouse_id
  , warehouse_name_output                 AS warehouse_name
  , COUNT(DISTINCT order_id)              AS total_orders
  , COUNT(DISTINCT sku)                   AS total_skus
  , COUNT(DISTINCT campaign_id)           AS total_campaigns
  , ROUND(SUM(pfc_funding_amount_lc), 2)  AS total_funding_v2_lc
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
  , warehouse_name_output