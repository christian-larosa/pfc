WITH djini_cart AS (

  SELECT
    djc_oc.created_date
    , djc_oc.country_code
    , djc_oc.global_entity_id
    , djc_oc.order_id AS djini_order_id
    , djc_oc.campaign_id
    , djc_oc.product_id
    , djc_oc.parent_product_id
    , IF(djc_oc.parent_product_id IS NOT NULL, TRUE, FALSE) AS is_combo_product
    , djc_oc.discount AS discount_lc
    , djc_oc.created_at AS created_at_utc
    , djc_oc.modified_at AS updated_at_utc
    , djc_oc.applied_times
    , djc_oc.supplier_funded_amount
    , djc_oc.benefit_qty
    , djc_oc.trigger_qty
    , djc_oc.ds_synced
    , djc_oc.external_funder
    , 'djini_cart' AS src
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_cart_order_campaigns` AS djc_oc
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(djc_oc.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(djc_oc.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), djini_app AS (

  SELECT
    dja_oc.created_date
    , dja_oc.country_code
    , dja_oc.global_entity_id
    , dja_oc.order_id AS djini_order_id
    , dja_oc.campaign_id
    , dja_oc.product_id
    , CAST(NULL AS STRING) AS parent_product_id
    , CAST(NULL AS BOOLEAN) AS is_combo_product
    , dja_oc.discount AS discount_lc
    , dja_oc.created_at AS created_at_utc
    , dja_oc.modified_at AS updated_at_utc
    , dja_oc.applied_times
    , CAST(NULL AS NUMERIC) AS supplier_funded_amount
    , CAST(NULL AS BOOLEAN) AS ds_synced
    , CAST(NULL AS STRING) AS external_funder
    , CAST(NULL AS NUMERIC) AS benefit_qty
    , CAST(NULL AS NUMERIC) AS trigger_qty
    , 'djini_app' AS src
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_order_campaigns` AS dja_oc
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(dja_oc.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(dja_oc.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), djini_order_campaigns AS (

  WITH all_order_campaigns AS (

    SELECT
      djc_oc.created_at_utc
      , djc_oc.country_code
      , djc_oc.djini_order_id
      , djc_oc.campaign_id
      , djc_oc.product_id
    FROM djini_cart AS djc_oc

    UNION ALL

    SELECT
      dja_oc.created_at_utc
      , dja_oc.country_code
      , dja_oc.djini_order_id
      , dja_oc.campaign_id
      , dja_oc.product_id
    FROM djini_app AS dja_oc

  )
  SELECT
    dj_oc.created_at_utc
    , dj_oc.country_code
    , dj_oc.djini_order_id
    , dj_oc.campaign_id
    , dj_oc.product_id
  FROM all_order_campaigns AS dj_oc
  QUALIFY ROW_NUMBER() OVER de_dup_order_campaigns = 1
  WINDOW de_dup_order_campaigns AS (
    PARTITION BY dj_oc.country_code, dj_oc.djini_order_id, dj_oc.campaign_id, dj_oc.product_id
    ORDER BY dj_oc.created_at_utc DESC
  )

)
SELECT
  COALESCE(djc_oc.created_date, dja_oc.created_date) AS created_date
  , dj_oc.country_code
  , COALESCE(djc_oc.global_entity_id, dja_oc.global_entity_id) AS global_entity_id
  , dj_oc.djini_order_id
  , dj_oc.campaign_id
  , dj_oc.product_id
  , COALESCE(djc_oc.parent_product_id, dja_oc.parent_product_id) AS parent_product_id
  , COALESCE(djc_oc.is_combo_product, dja_oc.is_combo_product) AS is_combo_product
  , COALESCE(djc_oc.discount_lc, dja_oc.discount_lc) AS discount_lc
  , dj_oc.created_at_utc
  , COALESCE(djc_oc.updated_at_utc, dja_oc.updated_at_utc) AS updated_at_utc
  , COALESCE(djc_oc.applied_times, dja_oc.applied_times) AS applied_times
  , COALESCE(djc_oc.benefit_qty, dja_oc.benefit_qty) AS benefit_qty
  , COALESCE(djc_oc.trigger_qty, dja_oc.trigger_qty) AS trigger_qty
  , COALESCE(djc_oc.supplier_funded_amount, dja_oc.supplier_funded_amount) AS supplier_funded_amount
  , COALESCE(djc_oc.ds_synced, dja_oc.ds_synced) AS ds_synced
  , COALESCE(djc_oc.external_funder, dja_oc.external_funder) AS external_funder
  , COALESCE(djc_oc.src, dja_oc.src) AS src
FROM djini_order_campaigns AS dj_oc
LEFT JOIN djini_cart AS djc_oc
  ON dj_oc.created_at_utc = djc_oc.created_at_utc
  AND dj_oc.country_code = djc_oc.country_code
  AND dj_oc.djini_order_id = djc_oc.djini_order_id
  AND dj_oc.campaign_id = djc_oc.campaign_id
  AND dj_oc.product_id = djc_oc.product_id
LEFT JOIN djini_app AS dja_oc
  ON dj_oc.created_at_utc = dja_oc.created_at_utc
  AND dj_oc.country_code = dja_oc.country_code
  AND dj_oc.djini_order_id = dja_oc.djini_order_id
  AND dj_oc.campaign_id = dja_oc.campaign_id
  AND dj_oc.product_id = dja_oc.product_id
