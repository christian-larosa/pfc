CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._dns_djini_campaigns_v2`
CLUSTER BY country_code
AS
WITH campaign_benefits AS (

  SELECT
    dj_cb.country_code
    , dj_cb.campaign_id
    , ARRAY_AGG(
        STRUCT(
          dj_cb.sku
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
        )
      ) AS benefits
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaign_benefits_v2` AS dj_cb
  GROUP BY 1, 2

), campaign_triggers AS (

  SELECT
    dj_ct.country_code
    , dj_ct.campaign_id
    , ARRAY_AGG(
        STRUCT(
          dj_ct.sku
          , dj_ct.created_at_utc
          , dj_ct.updated_at_utc
          , dj_ct.is_deleted
          , dj_ct.src
          , dj_ct.src_priority
        )
      ) AS triggers
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaign_triggers_v2` AS dj_ct
  GROUP BY 1, 2

), campaign_vendors AS (

  SELECT
    dj_cv.country_code
    , dj_cv.campaign_id
    , ARRAY_AGG(
        STRUCT(
          dj_cv.catalog_global_vendor_id
          , dj_cv.created_at_utc
          , dj_cv.updated_at_utc
        )
      ) AS vendors
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaign_vendors` AS dj_cv
  GROUP BY 1, 2

)
SELECT
  dj_c.country_code
  , dj_c.global_entity_id
  , dj_c.campaign_id
  , dj_c.campaign_name
  , dj_c.campaign_type
  , dj_c.campaign_subtype
  , dj_c.target_audience
  , dj_c.target_audience_id
  , dj_c.applies_to
  , dj_c.discount_type
  , dj_c.discount_value
  , dj_c.global_usage_limit
  , dj_c.user_usage_limit
  , dj_c.budget
  , dj_c.total_trigger_threshold
  , dj_c.externally_funded_percentage
  , dj_c.created_at_utc
  , dj_c.updated_at_utc
  , dj_c.start_at_utc
  , dj_c.end_at_utc
  , dj_c.is_active
  , dj_c.descriptions
  , dj_c.display_names
  , dj_c.benefit_qty_limit
  , dj_c.trigger_qty_threshold
  , dj_c.cart_item_usage_limit
  , dj_c.reason
  , dj_c.root_id
  , dj_c.deactivated_at_utc
  , dj_c.root_creation_at_utc
  , dj_c.fixed_bundle_price
  , dj_c.experiment_id
  , dj_c.variation_name
  , dj_c.creation_source
  , dj_c.external_funder
  , dj_c.state
  , dj_c.is_valid
  , dj_cb.benefits
  , dj_ct.triggers
  , dj_cv.vendors
FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaigns` AS dj_c
LEFT JOIN campaign_benefits AS dj_cb
  ON dj_c.country_code = dj_cb.country_code
  AND dj_c.campaign_id = dj_cb.campaign_id
LEFT JOIN campaign_triggers AS dj_ct
  ON dj_c.country_code = dj_ct.country_code
  AND dj_c.campaign_id = dj_ct.campaign_id
LEFT JOIN campaign_vendors AS dj_cv
  ON dj_c.country_code = dj_cv.country_code
  AND dj_c.campaign_id = dj_cv.campaign_id
