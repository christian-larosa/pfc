WITH djini_order_campaigns AS (

  SELECT
    dj_oc.country_code
    , dj_oc.djini_order_id
    , dj_oc.product_id
    , dj_oc.parent_product_id
    --* This table contains only the products for which campaigns were applied. Therefore, the flag should always be
    --* TRUE for products that are here.
    , TRUE AS product_has_campaigns
    , ARRAY_AGG(
        STRUCT(
          dj_oc.campaign_id
          , dj_oc.discount_lc
          , dj_oc.applied_times
          , dj_oc.supplier_funded_amount
          , dj_oc.external_funder
          , dj_oc.benefit_qty
          , dj_oc.trigger_qty
        )
      ) AS campaigns
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_order_campaigns` AS dj_oc
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(dj_oc.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(dj_oc.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}
  GROUP BY 1, 2, 3, 4

), djini_order_items AS (

  SELECT
    dj_oi.country_code
    , dj_oi.djini_order_id
    , ANY_VALUE(ARRAY_LENGTH(dj_oc.campaigns) > 0) AS order_has_campaigns
    , ARRAY_AGG(
        STRUCT(
          dj_oi.djini_order_item_id
          , dj_oi.product_id
          , dj_oi.parent_product_id
          , dj_oi.is_combo_product
          , dj_oi.platform_product_id
          , dj_oi.created_at_utc
          , dj_oi.product_qty
          , dj_oi.product_unit_price
          , dj_oi.absolute_discount_lc
          , dj_oi.total
          , dj_oi.subtotal
          , dj_oi.free_qty
          , dj_oi.distributed_discounted_price
          , dj_oi.weight
          , COALESCE(dj_oc.product_has_campaigns, FALSE) AS product_has_campaigns
          , dj_oc.campaigns
        )
      ) AS products
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_order_items` AS dj_oi
  LEFT JOIN djini_order_campaigns AS dj_oc
    ON dj_oi.country_code = dj_oc.country_code
    AND dj_oi.djini_order_id = dj_oc.djini_order_id
    AND dj_oi.product_id = dj_oc.product_id
    AND COALESCE(dj_oi.parent_product_id, 'X') = COALESCE(dj_oc.parent_product_id, 'X')
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(dj_oi.created_date) BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
      AND
      '{{ next_ds }}'
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(dj_oi.created_date) BETWEEN
      '{{ params.backfill_start_date }}'
      AND
      '{{ params.backfill_end_date }}'
    {%- endif %}
  GROUP BY 1, 2

), fx_rate AS (

  SELECT
    fx_rates.country_code
    , fx_rates.currency_code
    , fx_rates.exchange_rate_date
    , fx_rates.exchange_rate_value
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._tmp_fx_rates` AS fx_rates
  WHERE fx_rates.country_code IS NOT NULL

)
SELECT
  dj_o.created_date
  , dj_o.country_code
  , dj_o.region
  , dj_o.global_entity_id
  , dj_o.djini_order_id
  , dj_o.order_id
  , COALESCE(dj_oi.order_has_campaigns, FALSE) AS order_has_campaigns
  , dj_o.cart_id
  , dj_o.vendor_id
  , dj_o.target_audience
  , fx_rate.currency_code
  , fx_rate.exchange_rate_value
  , dj_o.absolute_discount_lc
  , dj_o.delivery_absolute_discount_lc
  , dj_o.total
  , dj_o.subtotal
  , dj_o.delivery_fee
  , dj_o.delivery_total
  , dj_o.minimum_order_value
  , dj_o.created_at_utc
  , dj_o.updated_at_utc
  , dj_o.status
  , dj_o.client_id
  , dj_o.session_id
  , dj_oi.products
FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_djini_orders` AS dj_o
LEFT JOIN djini_order_items AS dj_oi
  ON dj_o.country_code = dj_oi.country_code
  AND dj_o.djini_order_id = dj_oi.djini_order_id
LEFT JOIN fx_rate
  ON dj_o.country_code = fx_rate.country_code
  AND dj_o.created_date = fx_rate.exchange_rate_date
WHERE TRUE
  {%- if not params.backfill %}
  AND DATE(dj_o.created_date) BETWEEN
    DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
    AND
    '{{ next_ds }}'
  {%- elif params.is_backfill_chunks_enabled %}
  AND DATE(dj_o.created_date) BETWEEN
    '{{ params.backfill_start_date }}'
    AND
    '{{ params.backfill_end_date }}'
  {%- endif %}
