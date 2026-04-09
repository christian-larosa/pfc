CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.qc_campaigns`
CLUSTER BY country_code
AS
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
  , dj_c.benefits
  , dj_c.triggers
  , dj_c.vendors
FROM `{{ params.project_id }}.{{ params.dataset.cl }}._dns_djini_campaigns_v2` AS dj_c
