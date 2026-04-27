-- ============================================================
-- PFC 2.0 — T5: pfc_output
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Autor: Christian La Rosa
-- ============================================================
-- Procesa TODOS los países activos en pfc_config en una sola ejecución
-- Parámetros universales:
--   date_in                : 2025-01-01
--   date_fin               : CURRENT_DATE()
-- ============================================================

DECLARE date_in                 DATE    DEFAULT DATE('2025-01-01');
DECLARE date_fin                DATE    DEFAULT CURRENT_DATE();

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_output`
CLUSTER BY global_entity_id, billing_month, supplier_id
AS

-- Lee configuración desde pfc_config para todos los países activos
WITH config AS (
  SELECT
    global_entity_id
    , param_billing_period AS billing_period
  FROM `dh-darkstores-live.csm_automated_tables.pfc_config`
  WHERE is_active = TRUE
)

, pre_agg AS (
  SELECT
    pof.*
    , pof.warehouse_id AS warehouse_id_output
    , pof.warehouse_name AS warehouse_name_output
    , DATE_TRUNC(
        CASE cfg.billing_period
          WHEN 'order_date'        THEN pof.order_date
          WHEN 'campaign_end_date' THEN pof.campaign_end_date
        END
      , MONTH
      ) AS billing_month
  FROM `dh-darkstores-live.csm_automated_tables.pfc_order_funding` AS pof
  INNER JOIN config cfg
    ON pof.global_entity_id = cfg.global_entity_id
  WHERE CASE cfg.billing_period
          WHEN 'order_date'        THEN pof.order_date
          WHEN 'campaign_end_date' THEN pof.campaign_end_date
        END BETWEEN date_in AND date_fin
    AND pof.pfc_funding_amount_lc > 0  -- solo filas con funding real → credit note no incluye ceros
)

SELECT
  global_entity_id
  , billing_month
  , supplier_id
  , supplier_name
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
  , warehouse_id_output
  , warehouse_name_output