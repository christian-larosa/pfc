-- ============================================================
-- PFC 2.0 — pfc_config
-- Dataset destino: dh-darkstores-live.csm_automated_tables
-- Descripción: Configuración centralizada de parámetros por país
-- ============================================================

CREATE OR REPLACE TABLE `dh-darkstores-live.csm_automated_tables.pfc_config`
DESCRIPTION "Configuración centralizada de parámetros por país para PFC 2.0"
(
  global_entity_id STRING DESCRIPTION "Entity code (PY_PE, TB_BH, TB_AE, etc.)",
  country_code STRING DESCRIPTION "ISO country code (pe, bh, ae, etc.)",
  join_strategy STRING DESCRIPTION "Estrategia de matching: 'date_warehouse_sku' | 'campaign_id'. Define cómo emparejar órdenes con campañas en T3.",
  require_discount_to_charge BOOL DESCRIPTION "¿Requiere descuento para cobrar funding? TRUE=solo si hay descuento, FALSE=cobrar siempre.",
  missing_contract_fallback STRING DESCRIPTION "Fallback si contrato falta: 'skip'=no cobrar, 'full_discount'=cobrar monto completo del descuento.",
  funding_value_convention STRING DESCRIPTION "Convención de cálculo: 'normalized'=X LC/unidad × cantidad, 'per_benefit'=FLOOR(qty/threshold) × benefit_qty × unit_value.",
  funding_source STRING DESCRIPTION "Fuente de verdad para montos: 'negotiated'=T4 funding_total_lc (contratos), 'promotool'=PFC v1 funding_v1_lc (Djini legacy).",
  param_billing_period STRING DESCRIPTION "Período de agregación en T5: 'order_date'=agrupar por fecha de orden, 'campaign_end_date'=agrupar por fecha fin campaña.",
  is_active BOOL DESCRIPTION "¿Está esta configuración activa? TRUE=usar, FALSE=ignorar.",
  updated_at TIMESTAMP DESCRIPTION "Timestamp de última actualización."
)
AS
SELECT
  'PY_PE' AS global_entity_id,
  'pe' AS country_code,
  'date_warehouse_sku' AS join_strategy,
  TRUE AS require_discount_to_charge,
  'skip' AS missing_contract_fallback,
  'normalized' AS funding_value_convention,
  'negotiated' AS funding_source,
  'order_date' AS param_billing_period,
  TRUE AS is_active,
  CURRENT_TIMESTAMP() AS updated_at
UNION ALL
SELECT
  'TB_BH',
  'bh',
  'date_warehouse_sku',
  TRUE,
  'skip',
  'per_benefit',
  'negotiated',
  'campaign_end_date',
  TRUE,
  CURRENT_TIMESTAMP()
UNION ALL
SELECT
  'TB_AE',
  'ae',
  'date_warehouse_sku',
  TRUE,
  'skip',
  'per_benefit',
  'negotiated',
  'campaign_end_date',
  TRUE,
  CURRENT_TIMESTAMP()
