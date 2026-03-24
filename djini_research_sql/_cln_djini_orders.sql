WITH djini_cart AS (

  SELECT
    djc_o.created_date
    , djc_o.country_code
    , djc_o.region
    , djc_o.global_entity_id
    , djc_o.order_id AS djini_order_id
    , djc_o.platform_order_code AS order_id
    , djc_o.cart_id
    , djc_o.vendor_id
    , djc_o.target_audience
    , djc_o.absolute_discount AS absolute_discount_lc
    , djc_o.delivery_absolute_discount AS delivery_absolute_discount_lc
    , djc_o.total
    , djc_o.subtotal
    , djc_o.delivery_fee
    , djc_o.delivery_total
    , djc_o.minimum_order_value
    , djc_o.created_at AS created_at_utc
    , djc_o.modified_at AS updated_at_utc
    , djc_o.status
    , JSON_VALUE(djc_o.client_info, '$.client_id') AS client_id
    , JSON_VALUE(djc_o.client_info, '$.session_id') AS session_id
    , 'djini_cart' AS src
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_cart_orders` AS djc_o
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(djc_o.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(djc_o.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), djini_app AS (

  SELECT
    dja_o.created_date
    , dja_o.country_code
    , dja_o.region
    , dja_o.global_entity_id
    , dja_o.order_id AS djini_order_id
    , dja_o.platform_order_code AS order_id
    , dja_o.cart_id
    , dja_o.vendor_id
    , dja_o.target_audience
    , dja_o.absolute_discount AS absolute_discount_lc
    , dja_o.delivery_absolute_discount AS delivery_absolute_discount_lc
    , dja_o.total
    , dja_o.subtotal
    , dja_o.delivery_fee
    , dja_o.delivery_total
    , dja_o.minimum_order_value
    , dja_o.created_at AS created_at_utc
    , dja_o.modified_at AS updated_at_utc
    , dja_o.status
    , CAST(NULL AS STRING) AS client_id
    , CAST(NULL AS STRING) AS session_id
    , 'djini_app' AS src
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_orders` AS dja_o
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(dja_o.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(dja_o.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), djini_orders AS (

  WITH all_orders AS (

    SELECT
      djc_o.created_at_utc
      , djc_o.country_code
      , djc_o.region
      , djc_o.djini_order_id
    FROM djini_cart AS djc_o

    UNION ALL

    SELECT
      dja_o.created_at_utc
      , dja_o.country_code
      , dja_o.region
      , dja_o.djini_order_id
    FROM djini_app AS dja_o

  )
  SELECT
    dj_o.created_at_utc
    , dj_o.country_code
    , dj_o.region
    , dj_o.djini_order_id
  FROM all_orders AS dj_o
  QUALIFY ROW_NUMBER() OVER de_dup_orders = 1
  WINDOW de_dup_orders AS (
    PARTITION BY dj_o.country_code, dj_o.djini_order_id
    ORDER BY dj_o.created_at_utc DESC
  )

)
SELECT
  COALESCE(djc_o.created_date, dja_o.created_date) AS created_date
  , dj_o.country_code
  , dj_o.region
  , COALESCE(djc_o.global_entity_id, dja_o.global_entity_id) AS global_entity_id
  , dj_o.djini_order_id
  , COALESCE(djc_o.order_id, dja_o.order_id) AS order_id
  , COALESCE(djc_o.cart_id, dja_o.cart_id) AS cart_id
  , COALESCE(djc_o.vendor_id, dja_o.vendor_id) AS vendor_id
  , COALESCE(djc_o.target_audience, dja_o.target_audience) AS target_audience
  , COALESCE(djc_o.absolute_discount_lc, dja_o.absolute_discount_lc) AS absolute_discount_lc
  , COALESCE(djc_o.delivery_absolute_discount_lc, dja_o.delivery_absolute_discount_lc) AS delivery_absolute_discount_lc
  , COALESCE(djc_o.total, dja_o.total) AS total
  , COALESCE(djc_o.subtotal, dja_o.subtotal) AS subtotal
  , COALESCE(djc_o.delivery_fee, dja_o.delivery_fee) AS delivery_fee
  , COALESCE(djc_o.delivery_total, dja_o.delivery_total) AS delivery_total
  , COALESCE(djc_o.minimum_order_value, dja_o.minimum_order_value) AS minimum_order_value
  , dj_o.created_at_utc
  , COALESCE(djc_o.updated_at_utc, dja_o.updated_at_utc) AS updated_at_utc
  , COALESCE(djc_o.status, dja_o.status) AS status
  , COALESCE(djc_o.client_id, dja_o.client_id) AS client_id
  , COALESCE(djc_o.session_id, dja_o.session_id) AS session_id
  , COALESCE(djc_o.src, dja_o.src) AS src
FROM djini_orders AS dj_o
LEFT JOIN djini_cart AS djc_o
  ON dj_o.created_at_utc = djc_o.created_at_utc
  AND dj_o.country_code = djc_o.country_code
  AND dj_o.djini_order_id = djc_o.djini_order_id
LEFT JOIN djini_app AS dja_o
  ON dj_o.created_at_utc = dja_o.created_at_utc
  AND dj_o.country_code = dja_o.country_code
  AND dj_o.djini_order_id = dja_o.djini_order_id
