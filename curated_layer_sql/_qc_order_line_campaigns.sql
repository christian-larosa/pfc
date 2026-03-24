WITH products AS (

  SELECT
    cp.global_entity_id
    , cp.sku
    , vp.global_catalog_id
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS cp
  LEFT JOIN UNNEST(cp.vendor_products) AS vp
  WHERE TRUE
    AND vp.global_catalog_id IS NOT NULL

), latam_supplier_funded_value AS (

  SELECT
    sf.global_entity_id
    , sf.order_id
    , sf.campaign_id
    , sf.sku
    , sf.abs_discount_value_lt
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._cln_latam_supplier_funding` AS sf

)
SELECT
  dj_o.created_date AS order_created_date_utc
  , dj_o.global_entity_id
  , dj_o.country_code
  , dj_o.order_id
  , p.product_id AS global_catalog_id
  , p.platform_product_id
  , CASE
      --* See the column description in `_cln_pelican_order_items` for more information on why this is being done. We
      --* will have to add the other regions as they roll out.
      WHEN (
        dj_o.region = 'ap'
        AND dj_o.created_at_utc >= TIMESTAMP('2024-02-07 11:36:00 Europe/Berlin')
      ) THEN p.djini_order_item_id
      WHEN (
        dj_o.region = 'eu'
        AND LOWER(SPLIT(dj_o.global_entity_id, '_')[OFFSET(1)]) IN (
          'at', 'cz', 'de', 'fi', 'hu', 'no', 'se', 'sk', 'tr'
        )
        AND dj_o.created_at_utc >= TIMESTAMP('2024-02-07 11:36:00 Europe/Berlin')
      ) THEN p.djini_order_item_id
    END AS djini_order_item_id
  , dj_o.exchange_rate_value
  , SUM(p.product_qty) AS quantity_sold
  , SUM(p.subtotal) AS djini_order_items_before_discount_lc
  , (SUM(p.subtotal) / dj_o.exchange_rate_value) AS djini_order_items_before_discount_eur
  , SUM(p.absolute_discount_lc) AS djini_order_items_discount_lc
  , (SUM(p.absolute_discount_lc) / dj_o.exchange_rate_value) AS djini_order_items_discount_eur
  , SUM(p.total) AS djini_order_items_paid_lc
  , (SUM(p.total) / dj_o.exchange_rate_value) AS djini_order_items_paid_eur
  , COALESCE(SUM(c.supplier_funded_amount), 0) AS djini_order_items_supplier_funded_lc
  , (COALESCE(SUM(c.supplier_funded_amount), 0) / dj_o.exchange_rate_value) AS djini_order_items_supplier_funded_eur
  , COALESCE(SUM(fv.abs_discount_value_lt), SUM(c.supplier_funded_amount), 0) AS order_items_supplier_funded_lc
  , (
      COALESCE(SUM(fv.abs_discount_value_lt), SUM(c.supplier_funded_amount), 0) / dj_o.exchange_rate_value
    ) AS order_items_supplier_funded_eur
  , ARRAY_AGG(
      STRUCT(
        c.campaign_id
        , p.product_unit_price AS product_unit_price_lc
        , (p.product_unit_price / dj_o.exchange_rate_value) AS product_unit_price_eur
        , c.discount_lc AS campaign_discount_amt_lc
        , (c.discount_lc / dj_o.exchange_rate_value) AS campaign_discount_amt_eur
        , CAST(
            COALESCE(fv.abs_discount_value_lt, c.supplier_funded_amount) AS NUMERIC
          ) AS campaign_supplier_funded_amt_lc
        , CAST(
            COALESCE(
              (fv.abs_discount_value_lt / dj_o.exchange_rate_value)
              , (c.supplier_funded_amount / dj_o.exchange_rate_value)
            ) AS FLOAT64
          ) AS campaign_supplier_funded_amt_eur
        , c.trigger_qty
        , c.benefit_qty
      )
    ) AS campaign_info
  , ARRAY_AGG(
      STRUCT(
        p.parent_product_id AS combo_product_id
        , p.product_qty AS combo_qty
        , p.subtotal AS combo_amt_paid_lc
        , (p.subtotal / dj_o.exchange_rate_value) AS combo_amt_paid_eur
        , p.distributed_discounted_price AS distributed_discounted_price_lc
        , (p.distributed_discounted_price / dj_o.exchange_rate_value) AS distributed_discounted_price_eur
      )
    ) AS combo_products
FROM `{{ params.project_id }}.{{ params.dataset.cl }}._dns_djini_orders` AS dj_o
LEFT JOIN UNNEST(dj_o.products) AS p
LEFT JOIN UNNEST(p.campaigns) AS c
LEFT JOIN products AS cp
  ON cp.global_entity_id = dj_o.global_entity_id
  AND cp.global_catalog_id = p.product_id
LEFT JOIN latam_supplier_funded_value AS fv
  ON fv.global_entity_id = dj_o.global_entity_id
  AND fv.order_id = dj_o.order_id
  AND fv.sku = cp.sku
  AND fv.campaign_id = c.campaign_id
WHERE TRUE
  {%- if not params.backfill %}
  AND dj_o.created_date BETWEEN
    DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY)
    AND
    '{{ next_ds }}'
  {%- elif params.is_backfill_chunks_enabled %}
  AND dj_o.created_date BETWEEN
    '{{ params.backfill_start_date }}'
    AND
    '{{ params.backfill_end_date }}'
  {%- endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
