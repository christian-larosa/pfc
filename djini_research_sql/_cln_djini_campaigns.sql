CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_campaigns`
CLUSTER BY country_code
AS
WITH djini_campaigns AS (

  SELECT
    dj_c.country_code
    , dj_c.global_entity_id
    , dj_c.global_id AS campaign_id
    , dj_c.name AS campaign_name
    , CASE
        WHEN (
          dj_c.type = 'Generic'
          OR dj_c.type = 'GENERIC'
          OR dj_c.type = 'OldFashioned Campaign Type'
        ) THEN 'Generic'
        ELSE dj_c.type
      END AS campaign_type
    , CASE
        WHEN (
          dj_c.target_audience IS NOT NULL
          AND dj_c.target_audience NOT IN ('RETURNING', 'ALL', 'SUBSCRIBED_USER', 'NEW_USER')
        ) THEN 'SEGMENT_USER'
        ELSE dj_c.target_audience
      END AS target_audience
    , CASE
        WHEN (
          dj_c.target_audience IS NOT NULL
          AND dj_c.target_audience NOT IN ('RETURNING', 'ALL', 'SUBSCRIBED_USER', 'NEW_USER')
        ) THEN dj_c.target_audience
      END AS target_audience_id
    , dj_c.applies_to
    , JSON_VALUE(dj_c.discount, '$.type') AS discount_type
    , SAFE_CAST(JSON_VALUE(dj_c.discount, '$.value') AS FLOAT64) AS discount_value
    , dj_c.global_usage_limit
    , dj_c.user_usage_limit
    , dj_c.budget
    , dj_c.total_trigger_threshold
    , dj_c.externally_funded_percentage
    , dj_c.creation_date AS created_at_utc
    , dj_c.updated_date AS updated_at_utc
    , dj_c.start_time AS start_at_utc
    , dj_c.end_time AS end_at_utc
    , dj_c.active AS is_active
    --* Need to extract the values in local language and english
    , dj_c.description AS descriptions
    , dj_c.display_name AS display_names
    , dj_c.benefit_qty_limit
    , dj_c.trigger_qty_threshold
    , dj_c.cart_item_usage_limit
    , dj_c.reason
    , dj_c.root_id
    , dj_c.deactivated_at AS deactivated_at_utc
    , dj_c.root_creation_date AS root_creation_at_utc
    , dj_c.fixed_bundle_price
    , dj_c.experiment_id
    , dj_c.variation_name
    , dj_c.creation_source
    , dj_c.external_funder
    , dj_c.state
    --* If a campaign is being deactivated, `updated_date` can also be used as the deactivation date (since no other
    --* changes are done after)
    , CASE
        WHEN (dj_c.updated_date < dj_c.start_time AND dj_c.active IS FALSE) THEN FALSE
        ELSE TRUE
      END AS is_valid
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_campaigns` AS dj_c
  QUALIFY ROW_NUMBER() OVER campaign_last_updated_at = 1
  WINDOW campaign_last_updated_at AS (
    PARTITION BY dj_c.country_code, dj_c.global_id
    ORDER BY dj_c.creation_date DESC
  )

)
SELECT
  dj_c.country_code
  , dj_c.global_entity_id
  , dj_c.campaign_id
  , dj_c.campaign_name
  , dj_c.campaign_type
  , CASE
      WHEN (
        dj_c.campaign_type = 'Strikethrough'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'ABSOLUTE'
      ) THEN 'Strikethrough $'
      WHEN (
        dj_c.campaign_type = 'Strikethrough'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'PERCENTAGE'
      ) THEN 'Strikethrough %'
      WHEN (
        dj_c.campaign_type = 'SameItemBundle'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'PERCENTAGE'
        AND dj_c.discount_value = 100.0
      ) THEN 'Multibuy: Same Item - Free Item'
      WHEN (
        dj_c.campaign_type = 'SameItemBundle'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'PERCENTAGE'
        AND dj_c.discount_value < 100.0
      ) THEN 'Multibuy: Same Item %'
      WHEN (
        dj_c.campaign_type = 'SameItemBundle'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'ABSOLUTE'
      ) THEN 'Multibuy: Same Item $'
      WHEN (
        dj_c.campaign_type = 'SameItemBundle'
        AND dj_c.applies_to = 'DELIVERY_FEE'
      ) THEN 'Multibuy: Same Item - Delivery Fee Discount'
      WHEN (
        dj_c.campaign_type = 'MixAndMatch'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'PERCENTAGE'
        AND dj_c.discount_value = 100.0
      ) THEN 'Multibuy: Mix & Match - Free Item'
      WHEN (
        dj_c.campaign_type = 'MixAndMatch'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'PERCENTAGE'
        AND dj_c.discount_value < 100.0
      ) THEN 'Multibuy: Mix & Match %'
      WHEN (
        dj_c.campaign_type = 'MixAndMatch'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'ABSOLUTE'
      ) THEN 'Multibuy: Mix & Match $'
      WHEN (
        dj_c.campaign_type = 'MixAndMatch'
        AND dj_c.applies_to = 'DELIVERY_FEE'
      ) THEN 'Multibuy: Mix & Match - Delivery Fee Discount'
      WHEN (
        dj_c.campaign_type = 'BasketValue'
        AND dj_c.applies_to = 'DELIVERY_FEE'
      ) THEN 'Basket Discount - Delivery'
      WHEN (
        dj_c.campaign_type = 'BasketValue'
        AND (dj_c.applies_to = 'PRODUCTS' OR dj_c.applies_to = 'CHEAPEST_PRODUCTS')
        AND dj_c.discount_type = 'PERCENTAGE'
        AND dj_c.discount_value = 100.0
      ) THEN 'Basket Discount - Free Item'
      WHEN (
        dj_c.campaign_type = 'BasketValue'
        AND dj_c.applies_to = 'BASKET'
        AND dj_c.discount_type = 'PERCENTAGE'
      ) THEN 'Basket Discount %'
      WHEN (
        dj_c.campaign_type = 'BasketValue'
        AND dj_c.applies_to = 'BASKET'
        AND dj_c.discount_type = 'ABSOLUTE'
      ) THEN 'Basket Discount $'
      WHEN dj_c.campaign_type = 'Generic'
        THEN dj_c.campaign_type
    END AS campaign_subtype
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
FROM djini_campaigns AS dj_c
