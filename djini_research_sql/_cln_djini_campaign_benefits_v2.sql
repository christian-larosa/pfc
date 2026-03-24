CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaign_benefits_v2`
CLUSTER BY country_code, campaign_id
AS
WITH old_benefits_table AS (

  SELECT
    dj_cbo.country_code
    , dj_cbo.global_entity_id
    , dj_cbo.campaign_id
    , dj_cbo.sku
    , dj_cbo.discount_type
    , dj_cbo.discount_value
    , dj_cbo.max_qty
    , dj_cbo.remaining_qty
    , dj_cbo.supplier_funding_type
    , dj_cbo.supplier_funding_value
    , dj_cbo.created_at_utc
    , dj_cbo.updated_at_utc
    , dj_cbo.is_deleted
    , 'Old benefits' AS src
    , 2 AS src_priority
  FROM `{{ params.project_id }}.{{ params.dataset.static_cl }}._cln_djini_campaign_benefits` AS dj_cbo

), new_benefits_table AS (

  SELECT
    dj_cb.country_code
    , dj_cb.global_entity_id
    , dj_cb.campaign_id
    , dj_cb.sku
    , dj_cb.discount_type
    , dj_cb.discount AS discount_value
    , dj_cb.max_qty
    , dj_cb.remaining_qty
    , dj_cb.supplier_fund_type AS supplier_funding_type
    , dj_cb.supplier_fund_value AS supplier_funding_value
    , dj_cb.created_at AS created_at_utc
    , dj_cb.modified_at AS updated_at_utc
    , dj_cb.is_deleted
    , 'New benefits' AS src
    , 1 AS src_priority
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_benefit_skus` AS dj_cb
  QUALIFY ROW_NUMBER() OVER last_updated = 1
  WINDOW last_updated AS (
    PARTITION BY dj_cb.country_code, dj_cb.global_entity_id, dj_cb.campaign_id, dj_cb.sku
    ORDER BY dj_cb.modified_at DESC
  )

), all_campaign_benefits AS (

  SELECT
    dj_cbo.country_code
    , dj_cbo.global_entity_id
    , dj_cbo.campaign_id
    , dj_cbo.sku
    , dj_cbo.discount_type
    , dj_cbo.discount_value
    , dj_cbo.max_qty
    , dj_cbo.remaining_qty
    , dj_cbo.supplier_funding_type
    , dj_cbo.supplier_funding_value
    , dj_cbo.created_at_utc
    , dj_cbo.updated_at_utc
    , dj_cbo.is_deleted
    , dj_cbo.src
    , dj_cbo.src_priority
  FROM old_benefits_table AS dj_cbo

  UNION ALL

  SELECT
    dj_cb.country_code
    , dj_cb.global_entity_id
    , dj_cb.campaign_id
    , dj_cb.sku
    , dj_cb.discount_type
    , dj_cb.discount_value
    , dj_cb.max_qty
    , dj_cb.remaining_qty
    , dj_cb.supplier_funding_type
    , dj_cb.supplier_funding_value
    , dj_cb.created_at_utc
    , dj_cb.updated_at_utc
    , dj_cb.is_deleted
    , dj_cb.src
    , dj_cb.src_priority
  FROM new_benefits_table AS dj_cb

), campaign_benefits AS (

  SELECT
    dj_cb.country_code
    , dj_cb.global_entity_id
    , dj_cb.campaign_id
    , dj_cb.sku
    , dj_cb.discount_type
    , dj_cb.discount_value
    , dj_cb.max_qty
    , dj_cb.remaining_qty
    , dj_cb.supplier_funding_type
    , dj_cb.supplier_funding_value
    , dj_cb.created_at_utc
    , dj_cb.updated_at_utc
    , dj_cb.is_deleted
    , dj_cb.src
    , dj_cb.src_priority
  FROM all_campaign_benefits AS dj_cb
  QUALIFY ROW_NUMBER() OVER newest_record = 1
  WINDOW newest_record AS (
    PARTITION BY dj_cb.country_code, dj_cb.global_entity_id, dj_cb.campaign_id, dj_cb.sku
    ORDER BY dj_cb.src_priority ASC
  )

)
SELECT
  dj_cb.country_code
  , dj_cb.global_entity_id
  , dj_cb.campaign_id
  , dj_cb.sku
  , dj_cb.discount_type
  , dj_cb.discount_value
  , dj_cb.max_qty
  , dj_cb.remaining_qty
  , dj_cb.supplier_funding_type
  , dj_cb.supplier_funding_value
  , dj_cb.created_at_utc
  , dj_cb.updated_at_utc
  , dj_cb.is_deleted
  , dj_cb.src
  , dj_cb.src_priority
FROM campaign_benefits AS dj_cb
