CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaign_vendors`
CLUSTER BY country_code, campaign_id
AS
SELECT
  dj_cv.country_code
  , dj_cv.global_entity_id
  , dj_cv.campaign_id
  , dj_cv.vendor_id AS catalog_global_vendor_id
  , dj_cv.creation_date AS created_at_utc
  -- This COALESCE is used to solve the null values present in some old campaign-vendor pairs.
  , COALESCE(dj_cv.updated_date, dj_cv.creation_date) AS updated_at_utc
FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_campaign_vendors` AS dj_cv
QUALIFY ROW_NUMBER() OVER last_updated = 1
WINDOW last_updated AS (
  PARTITION BY dj_cv.country_code, dj_cv.global_entity_id, dj_cv.campaign_id, dj_cv.vendor_id
  ORDER BY dj_cv.updated_date DESC
)
