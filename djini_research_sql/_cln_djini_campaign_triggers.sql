CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.static_cl }}._cln_djini_campaign_triggers`
CLUSTER BY country_code, campaign_id
AS
WITH old_triggers_table AS (

  SELECT DISTINCT
    dj_cto.country_code
    , dj_cto.global_entity_id
    , dj_cto.campaign_id
    -- `product_id` is obviously not the same as `sku`, but there are NULL values for 1 campaign in the `sku` column.
    -- If we don't fill the values with unique values, the whole campaign is reduced to one single line.
    -- Problematic campaign:
    --   created_date = '2022-06-22'
    --   country_code = 'pk'
    --   campaign_id = '784be058-3113-4c7d-b871-d3a45afb527f'
    , COALESCE(dj_cto.sku, dj_cto.product_id) AS sku
    , dj_cto.created_at AS created_at_utc
    , dj_cto.modified_at AS updated_at_utc
    , dj_cto.is_deleted
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_campaign_triggers` AS dj_cto
  QUALIFY ROW_NUMBER() OVER last_updated = 1
  WINDOW last_updated AS (
    PARTITION BY dj_cto.country_code, dj_cto.global_entity_id, dj_cto.campaign_id, dj_cto.product_id, dj_cto.vendor_id
    ORDER BY dj_cto.modified_at DESC
  )

)
SELECT
  dj_ct.country_code
  , dj_ct.global_entity_id
  , dj_ct.campaign_id
  , dj_ct.sku
  , dj_ct.created_at_utc
  , dj_ct.updated_at_utc
  , dj_ct.is_deleted
FROM old_triggers_table AS dj_ct
