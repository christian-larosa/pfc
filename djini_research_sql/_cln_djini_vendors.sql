CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_vendors`
AS
SELECT
  dj_v.country_code
  , dj_v.global_entity_id
  , dj_v.vendor_id AS catalog_global_vendor_id
  , CASE
      WHEN NOT (
        dj_v.global_entity_id LIKE 'PY_%'
        OR dj_v.global_entity_id LIKE 'TB_%'
        OR dj_v.global_entity_id LIKE 'HS_%'
      ) THEN dj_v.vendor_code
    END AS catalog_remote_vendor_id
  , CASE
      WHEN (
        dj_v.global_entity_id LIKE 'PY_%'
        OR dj_v.global_entity_id LIKE 'TB_%'
        OR dj_v.global_entity_id LIKE 'HS_%'
      ) THEN dj_v.vendor_code
    END AS catalog_additional_remote_vendor_id
  , dj_v.name
  , dj_v.created_at AS created_at_utc
  , dj_v.modified_at AS updated_at_utc
  , dj_v.deleted_at AS deleted_at_utc
  , dj_v.synchronised_at AS synchronized_at_utc
FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_vendors` AS dj_v
WHERE dj_v.global_entity_id NOT IN ('TEST_VA')
QUALIFY ROW_NUMBER() OVER last_synchronized = 1
WINDOW last_synchronized AS (
  PARTITION BY dj_v.country_code, dj_v.global_entity_id, dj_v.vendor_id
  ORDER BY dj_v.synchronised_at DESC
)
