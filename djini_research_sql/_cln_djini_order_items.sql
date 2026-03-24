WITH djini_cart AS (

  SELECT
    djc_oi.created_date
    , djc_oi.country_code
    , djc_oi.global_entity_id
    , djc_oi.order_id AS djini_order_id
    , djc_oi.order_item_id AS djini_order_item_id
    , djc_oi.product_id
    , djc_oi.parent_product_id
    , IF(djc_oi.parent_product_id IS NOT NULL, TRUE, FALSE) AS is_combo_product
    , djc_oi.qty AS product_qty
    , djc_oi.price AS product_unit_price
    , djc_oi.absolute_discount AS absolute_discount_lc
    , djc_oi.total
    , djc_oi.subtotal
    , djc_oi.created_at AS created_at_utc
    , djc_oi.modified_at AS updated_at_utc
    , djc_oi.free_qty
    , djc_oi.platform_product_id
    , djc_oi.distributed_discounted_price
    , djc_oi.weight
    , 'djini_cart' AS src
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_cart_order_items` AS djc_oi
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(djc_oi.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(djc_oi.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), djini_app AS (

  SELECT
    dja_oi.created_date
    , dja_oi.country_code
    , dja_oi.global_entity_id
    , dja_oi.order_id AS djini_order_id
    , CAST(NULL AS STRING) AS djini_order_item_id
    , dja_oi.product_id
    , CAST(NULL AS STRING) AS parent_product_id
    , CAST(NULL AS BOOLEAN) AS is_combo_product
    , dja_oi.qty AS product_qty
    , dja_oi.price AS product_unit_price
    , dja_oi.absolute_discount AS absolute_discount_lc
    , dja_oi.total
    , dja_oi.subtotal
    , dja_oi.created_at AS created_at_utc
    , dja_oi.modified_at AS updated_at_utc
    , dja_oi.free_qty
    , CAST(NULL AS STRING) AS platform_product_id
    , CAST(NULL AS NUMERIC) AS distributed_discounted_price
    , CAST(NULL AS NUMERIC) AS weight
    , 'djini_app' AS src
  FROM `{{ params.project_id }}.{{ params.dataset.dl }}.djini_app_data_order_items` AS dja_oi
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(dja_oi.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(dja_oi.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}

), djini_order_items AS (

  WITH all_order_items AS (

    SELECT
      djc_oi.created_at_utc
      , djc_oi.country_code
      , djc_oi.djini_order_id
      , djc_oi.product_id
    FROM djini_cart AS djc_oi

    UNION ALL

    SELECT
      dja_oi.created_at_utc
      , dja_oi.country_code
      , dja_oi.djini_order_id
      , dja_oi.product_id
    FROM djini_app AS dja_oi

  )
  SELECT
    dj_oi.created_at_utc
    , dj_oi.country_code
    , dj_oi.djini_order_id
    , dj_oi.product_id
  FROM all_order_items AS dj_oi
  QUALIFY ROW_NUMBER() OVER de_dup_order_items = 1
  WINDOW de_dup_order_items AS (
    PARTITION BY dj_oi.country_code, dj_oi.djini_order_id, dj_oi.product_id
    ORDER BY dj_oi.created_at_utc DESC
  )

)
SELECT
  COALESCE(djc_oi.created_date, dja_oi.created_date) AS created_date
  , dj_oi.country_code
  , COALESCE(djc_oi.global_entity_id, dja_oi.global_entity_id) AS global_entity_id
  , dj_oi.djini_order_id
  , COALESCE(djc_oi.djini_order_item_id, dja_oi.djini_order_item_id) AS djini_order_item_id
  , dj_oi.product_id
  , COALESCE(djc_oi.parent_product_id, dja_oi.parent_product_id) AS parent_product_id
  , COALESCE(djc_oi.is_combo_product, dja_oi.is_combo_product) AS is_combo_product
  , COALESCE(djc_oi.product_qty, dja_oi.product_qty) AS product_qty
  , COALESCE(djc_oi.product_unit_price, dja_oi.product_unit_price) AS product_unit_price
  , COALESCE(djc_oi.absolute_discount_lc, dja_oi.absolute_discount_lc) AS absolute_discount_lc
  , COALESCE(djc_oi.total, dja_oi.total) AS total
  , COALESCE(djc_oi.subtotal, dja_oi.subtotal) AS subtotal
  , dj_oi.created_at_utc
  , COALESCE(djc_oi.updated_at_utc, dja_oi.updated_at_utc) AS updated_at_utc
  , COALESCE(djc_oi.free_qty, dja_oi.free_qty) AS free_qty
  , COALESCE(djc_oi.platform_product_id, dja_oi.platform_product_id) AS platform_product_id
  , COALESCE(djc_oi.distributed_discounted_price, dja_oi.distributed_discounted_price) AS distributed_discounted_price
  , COALESCE(djc_oi.weight, dja_oi.weight) AS weight
  , COALESCE(djc_oi.src, dja_oi.src) AS src
FROM djini_order_items AS dj_oi
LEFT JOIN djini_cart AS djc_oi
  ON dj_oi.created_at_utc = djc_oi.created_at_utc
  AND dj_oi.country_code = djc_oi.country_code
  AND dj_oi.djini_order_id = djc_oi.djini_order_id
  AND dj_oi.product_id = djc_oi.product_id
LEFT JOIN djini_app AS dja_oi
  ON dj_oi.created_at_utc = dja_oi.created_at_utc
  AND dj_oi.country_code = dja_oi.country_code
  AND dj_oi.djini_order_id = dja_oi.djini_order_id
  AND dj_oi.product_id = dja_oi.product_id
