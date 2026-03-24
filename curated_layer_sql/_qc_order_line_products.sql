WITH product_stream AS (

  SELECT
    ps.global_entity_id
    , ps.platform_vendor_id
    , ps.sku
    , ps.platform_product_id
    , ps.global_catalog_id
    , ps.valid_from_utc AS start_timestamp_utc
    , ps.valid_to_utc AS end_timestamp_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_products_sku_history` AS ps
  WHERE TRUE
    AND ps.valid_from_utc >= '2019-01-01'

), qc_catalog_products AS (

  SELECT
    qc_p.global_entity_id
    , vp.platform_vendor_id
    , qc_p.sku
    , vp.platform_product_id
    , vp.global_catalog_id
    , vp.tags
    , vp.vendor_product_created_at_utc AS start_timestamp_utc
    , CAST('2099-12-31 23:59:59' AS TIMESTAMP) AS end_timestamp_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qc_p
    , UNNEST(qc_p.vendor_products) AS vp

), efood_weightable_items AS (

  /*
    The logic in this CTE handles the workaround Efood platform has taken in order to enable
    weightable products for DMART users.

    They use catalog_tag to define the weight step/multiple comprising a unit of weightable items sold
    to the users. This has taken effect from Feb'2024.
  */
  SELECT
    cp.global_entity_id
    , cp.global_catalog_id
    , cp.sku
    , cp.platform_vendor_id
    , MIN(SAFE_CAST(SPLIT(t.tag_name, '-')[SAFE_OFFSET(1)] AS NUMERIC)) AS weight_step
  FROM qc_catalog_products AS cp
    , UNNEST(cp.tags) AS t
  WHERE TRUE
    AND LOWER(t.tag_name) LIKE 'weighted_step-%'
    AND cp.global_entity_id = 'EF_GR'
  GROUP BY 1, 2, 3, 4

), qc_order_line_products AS (

  SELECT
    _qc_ol.origin
    , _qc_ol.order_created_date_utc
    , _qc_ol.order_created_at_utc
    , _qc_ol.global_entity_id
    , LOWER(SPLIT(_qc_ol.global_entity_id, '_')[OFFSET(1)]) AS country_code
    , _qc_ol.fulfilled_by_entity
    , _qc_ol.order_id
    , COALESCE(_qc_ol.global_catalog_id, ps.global_catalog_id, qc_p.global_catalog_id) AS global_catalog_id
    , COALESCE(_qc_ol.platform_product_id, ps.platform_product_id, qc_p.platform_product_id) AS platform_product_id
    , _qc_ol.pelican_order_item_id
    , COALESCE(_qc_ol.sku, ps.sku, qc_p.sku) AS sku
    , _qc_ol.djini_order_item_id
    , _qc_ol.replacement_pelican_order_item_id
    , _qc_ol.platform_vendor_id
    , _qc_ol.currency_code
    , _qc_ol.pricing_type
    , _qc_ol.vat_percentage
    , _qc_ol.is_custom
    , _qc_ol.order_status
    , _qc_ol.is_checkout_confirmed
    , _qc_ol.order_updated_at_utc
    , _qc_ol.pelican_order_item_status
    , _qc_ol.pickup_issue
    , _qc_ol.is_modified_quantity
    , _qc_ol.is_modified_price
    , _qc_ol.min_quantity
    , _qc_ol.max_quantity
    , CASE
        WHEN _qc_ol.global_entity_id = 'EF_GR' AND efood_weightables.weight_step IS NOT NULL
          THEN ROUND(SAFE_DIVIDE(_qc_ol.quantity_ordered, efood_weightables.weight_step), 2)
        ELSE _qc_ol.quantity_ordered
      END AS quantity_ordered
    , CASE
        WHEN _qc_ol.global_entity_id = 'EF_GR' AND efood_weightables.weight_step IS NOT NULL
          THEN ROUND(SAFE_DIVIDE(_qc_ol.quantity_picked_up, efood_weightables.weight_step), 2)
        ELSE _qc_ol.quantity_picked_up
      END AS quantity_picked_up
    , CASE
        WHEN _qc_ol.global_entity_id = 'EF_GR' AND efood_weightables.weight_step IS NOT NULL
          THEN ROUND(SAFE_DIVIDE(_qc_ol.quantity_delivered, efood_weightables.weight_step), 2)
        ELSE _qc_ol.quantity_delivered
      END AS quantity_delivered
    , CASE
        WHEN _qc_ol.global_entity_id = 'EF_GR' AND efood_weightables.weight_step IS NOT NULL
          THEN ROUND(SAFE_DIVIDE(_qc_ol.quantity_returned, efood_weightables.weight_step), 2)
        ELSE _qc_ol.quantity_returned
      END AS quantity_returned
    , CASE
        WHEN _qc_ol.global_entity_id = 'EF_GR' AND efood_weightables.weight_step IS NOT NULL
          THEN ROUND(SAFE_DIVIDE(_qc_ol.quantity_sold, efood_weightables.weight_step), 2)
        ELSE _qc_ol.quantity_sold
      END AS quantity_sold
    , _qc_ol.weighted_pieces_ordered
    , _qc_ol.weighted_pieces_picked_up
    , _qc_ol.unit_price_lc
    , _qc_ol.unit_price_paid_lc
    , _qc_ol.total_amt_paid_lc
    , _qc_ol.returns
    , _qc_ol.parent_order_id
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._qc_order_lines` AS _qc_ol
  LEFT JOIN qc_catalog_products AS qc_p
    ON _qc_ol.global_entity_id = qc_p.global_entity_id
    AND _qc_ol.platform_vendor_id = qc_p.platform_vendor_id
    AND _qc_ol.global_catalog_id = qc_p.global_catalog_id
    AND _qc_ol.order_created_at_utc BETWEEN qc_p.start_timestamp_utc AND qc_p.end_timestamp_utc
  LEFT JOIN product_stream AS ps
    ON _qc_ol.global_entity_id = ps.global_entity_id
    AND _qc_ol.platform_product_id = ps.platform_product_id
    AND _qc_ol.platform_vendor_id = ps.platform_vendor_id
    AND _qc_ol.order_created_at_utc BETWEEN ps.start_timestamp_utc AND ps.end_timestamp_utc
  LEFT JOIN efood_weightable_items AS efood_weightables
    ON _qc_ol.global_entity_id = efood_weightables.global_entity_id
    AND _qc_ol.platform_vendor_id = efood_weightables.platform_vendor_id
    AND _qc_ol.global_catalog_id = efood_weightables.global_catalog_id
    AND COALESCE(_qc_ol.sku, ps.sku, qc_p.sku) = efood_weightables.sku
  WHERE TRUE
    {%- if not params.backfill %}
    AND _qc_ol.order_created_date_utc BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} + 1 DAY)
      AND
      DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND _qc_ol.order_created_date_utc BETWEEN
      DATE_SUB('{{ params.backfill_start_date }}', INTERVAL 1 DAY)
      AND
      DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY)
    {%- endif %}

), warehouses AS (

  -- DISTINCT is needed due to an operational case in Talabat: one platform_vendor_id
  -- can have multiple additional_platform_vendor_id values (UUIDs), causing duplicate rows.
  SELECT DISTINCT
    wh.global_entity_id
    , wh.warehouse_id
    , wh.is_dmart
    , COALESCE(wh_v.platform_vendor_id, wh_v.additional_platform_vendor_id) AS join_key
    , COALESCE(LAG(wh_v.migrated_at_utc) OVER warehouse_vendor, '1970-01-01 00:00:00.000 UTC') AS valid_from_utc
    , COALESCE(wh_v.migrated_at_utc, '2030-01-01 00:00:00.000 UTC') AS valid_to_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.warehouses_v2` AS wh
  LEFT JOIN UNNEST(wh.vendors) AS wh_v
  WINDOW warehouse_vendor AS (
    PARTITION BY wh.global_entity_id, COALESCE(wh_v.platform_vendor_id, wh_v.additional_platform_vendor_id)
    ORDER BY wh_v.migrated_at_utc DESC
  )

), wac_info AS (

  SELECT
    wac.global_entity_id
    , wac.warehouse_id
    , wac.sku
    , wac.valid_from
    , wac.valid_to
    , wac.wac
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.wac_info` AS wac

), fx_rates AS (

  SELECT DISTINCT
    fx.exchange_rate_date
    , fx.currency_code
    , fx.exchange_rate_value AS fx_rate
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._tmp_fx_rates` AS fx
  WHERE TRUE
    {%- if not params.backfill %}
    AND fx.exchange_rate_date BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} + 1 DAY)
      AND
      DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND fx.exchange_rate_date BETWEEN
      DATE_SUB('{{ params.backfill_start_date }}', INTERVAL 1 DAY)
      AND
      DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY)
    {%- endif %}

), vat_history AS (

  SELECT
    vat.global_entity_id
    , vat.global_catalog_id
    , vat.timestamp_valid_from AS timestamp_valid_from_utc
    , vat.vat_rate AS product_vat_rate
    , COALESCE(
        LAG(vat.timestamp_valid_from) OVER last_updated_timestamp_utc
        , '2030-01-01 00:00:00.000 UTC'
      ) AS timestamp_valid_to_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._dim_product_vat_rate_history` AS vat
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(vat.timestamp_valid_from) < DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(vat.timestamp_valid_from) < DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY)
    {%- endif %}
  WINDOW last_updated_timestamp_utc AS (
    PARTITION BY vat.global_entity_id, vat.global_catalog_id
    ORDER BY vat.timestamp_valid_from DESC
  )

), price_history AS (

  SELECT
    p_ph.global_entity_id
    , p_ph.global_catalog_id
    , p_ph.original_price_lc
    , p_ph.valid_from_utc
    , p_ph.valid_to_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_products_original_price_history` AS p_ph
  WHERE TRUE
    AND DATE(p_ph.valid_from_lt) >= '2019-01-01'

), campaign_orders AS (

  SELECT
    c.global_entity_id
    , c.country_code
    , c.order_id
    , c.global_catalog_id
    , c.platform_product_id
    , c.djini_order_item_id
    , c.djini_order_items_discount_lc
    , c.djini_order_items_discount_eur
    , c.djini_order_items_supplier_funded_lc
    , c.djini_order_items_supplier_funded_eur
    , c.order_items_supplier_funded_lc
    , c.order_items_supplier_funded_eur
    , c.campaign_info
    , c.combo_products
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._qc_order_line_campaigns` AS c
  WHERE TRUE
    {%- if not params.backfill %}
    AND c.order_created_date_utc BETWEEN
      DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} + 1 DAY)
      AND
      DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND c.order_created_date_utc BETWEEN
      DATE_SUB('{{ params.backfill_start_date }}', INTERVAL 1 DAY)
      AND
      DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY)
    {%- endif %}

), vendors AS (

  SELECT
    v.global_entity_id
    , v.platform_vendor_id
    , v.vertical_type
    , v.vertical_parent
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.vendors` AS v

), sales_buffer AS (

  SELECT
    sbh.global_entity_id
    , sbh.global_catalog_id
    , sbh.valid_from_utc AS timestamp_valid_from_utc
    , sbh.valid_to_utc AS timestamp_valid_to_utc
    , sbh.sales_buffer
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_sales_buffer_history` AS sbh
  WHERE TRUE
    {%- if not params.backfill %}
    AND DATE(sbh.valid_from_lt) < DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    {%- elif params.is_backfill_chunks_enabled %}
    AND DATE(sbh.valid_from_lt) < DATE_ADD('{{ params.backfill_end_date }}', INTERVAL 1 DAY)
    {%- endif %}

), weightables_history AS (

  SELECT
    wah.global_entity_id
    , wah.global_catalog_id
    , wah.updated_value
    , wah.updated_field
    , wah.valid_from_utc
    , wah.valid_to_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_products_weightable_attributes_history` AS wah
  WHERE TRUE
    AND DATE(wah.valid_from_lt) < DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    AND wah.updated_field IN (
      'minimum_starting_weight'
      , 'sold_by_weight'
      , 'sold_by_piece'
      , 'average_weight_per_piece'
      , 'average_weight_per_piece_unit'
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY wah.global_entity_id, wah.global_catalog_id, wah.updated_field, DATE(wah.valid_from_lt)
    ORDER BY wah.valid_from_lt DESC
  ) = 1

), msw_history AS (

  SELECT
    wah.global_entity_id
    , wah.global_catalog_id
    , CAST(wah.updated_value AS NUMERIC) AS minimum_starting_weight
    , wah.valid_from_utc
    , wah.valid_to_utc
  FROM weightables_history AS wah
  WHERE TRUE
    AND wah.updated_field = 'minimum_starting_weight'

), sbw_history AS (

  SELECT
    wah.global_entity_id
    , wah.global_catalog_id
    , CAST(wah.updated_value AS BOOLEAN) AS is_sold_by_weight
    , wah.valid_from_utc
    , wah.valid_to_utc
  FROM weightables_history AS wah
  WHERE TRUE
    AND wah.updated_field = 'sold_by_weight'

), sbp_history AS (

  SELECT
    wah.global_entity_id
    , wah.global_catalog_id
    , CAST(wah.updated_value AS BOOLEAN) AS is_sold_by_piece
    , wah.valid_from_utc
    , wah.valid_to_utc
  FROM weightables_history AS wah
  WHERE TRUE
    AND wah.updated_field = 'sold_by_piece'

), awpp_history AS (

  SELECT
    wah.global_entity_id
    , wah.global_catalog_id
    , SAFE_CAST(wah.updated_value AS NUMERIC) AS average_weight_per_piece
    , wah.valid_from_utc
    , wah.valid_to_utc
  FROM weightables_history AS wah
  WHERE TRUE
    AND wah.updated_field = 'average_weight_per_piece'

)
SELECT
  qc_ol.origin
  , qc_ol.order_created_date_utc
  , qc_ol.order_created_at_utc
  , qc_ol.global_entity_id
  , qc_ol.fulfilled_by_entity
  , qc_ol.country_code
  , qc_ol.order_id
  , w.warehouse_id
  , COALESCE(w.is_dmart, FALSE) AS is_dmart
  , qc_ol.global_catalog_id
  , qc_ol.platform_product_id
  , qc_ol.pelican_order_item_id
  , qc_ol.sku
  , qc_ol.djini_order_item_id
  , qc_ol.replacement_pelican_order_item_id
  , qc_ol.platform_vendor_id
  , v.vertical_type
  , v.vertical_parent
  , qc_ol.currency_code
  , qc_ol.pricing_type
  , qc_ol.is_custom
  , qc_ol.order_updated_at_utc
  , qc_ol.order_status
  , qc_ol.pelican_order_item_status
  , qc_ol.is_checkout_confirmed
  , qc_ol.pickup_issue
  , qc_ol.is_modified_quantity
  , qc_ol.is_modified_price
  , qc_ol.min_quantity
  , qc_ol.max_quantity
  , COALESCE(qc_ol.quantity_ordered, 0) AS quantity_ordered
  , COALESCE(qc_ol.quantity_picked_up, 0) AS quantity_picked_up
  , COALESCE(qc_ol.quantity_delivered, 0) AS quantity_delivered
  , COALESCE(qc_ol.quantity_returned, 0) AS quantity_returned
  , COALESCE(qc_ol.quantity_sold, 0) AS quantity_sold
  , fx.fx_rate
  , sb.sales_buffer
  , qc_ol.unit_price_lc
  , (qc_ol.unit_price_lc / fx.fx_rate) AS unit_price_eur
  , qc_ol.unit_price_paid_lc
  , (qc_ol.unit_price_paid_lc / fx.fx_rate) AS unit_price_paid_eur
  , qc_ol.total_amt_paid_lc
  , (qc_ol.total_amt_paid_lc / fx.fx_rate) AS total_amt_paid_eur
  , COALESCE(CAST(vat.product_vat_rate AS NUMERIC), qc_ol.vat_percentage) AS product_vat_rate
  -- wac info
  , COALESCE(wac.wac, qc_ol.unit_price_lc) AS unit_cost_lc
  , COALESCE(wac.wac, qc_ol.unit_price_lc) / fx.fx_rate AS unit_cost_eur
  , CASE
      WHEN sbw.is_sold_by_weight OR sbp.is_sold_by_piece
        THEN CAST(
          (
            -- Multiplying by 1000 from g to Kg
            COALESCE(wac.wac, 0) * COALESCE(qc_ol.quantity_delivered - COALESCE(qc_ol.quantity_returned, 0), 0) * 1000
          ) AS NUMERIC
        )
      ELSE CAST(
        (
          COALESCE(wac.wac, 0) * COALESCE(qc_ol.quantity_delivered - COALESCE(qc_ol.quantity_returned, 0), 0)
        ) AS NUMERIC
      )
    END AS amt_cogs_lc
  , CASE
      WHEN sbw.is_sold_by_weight OR sbp.is_sold_by_piece
        THEN CAST(
          (
            COALESCE(wac.wac, 0)
              / fx.fx_rate
              * COALESCE(qc_ol.quantity_delivered - COALESCE(qc_ol.quantity_returned, 0), 0)
              * 1000
          ) AS NUMERIC
        )
      ELSE CAST(
        (
          COALESCE(wac.wac, 0)
            / fx.fx_rate
            * COALESCE(qc_ol.quantity_delivered - COALESCE(qc_ol.quantity_returned, 0), 0)
        ) AS NUMERIC
      )
    END AS amt_cogs_eur
  -- campaign info
  , COALESCE(c.djini_order_items_discount_lc, cp.djini_order_items_discount_lc, 0) AS djini_order_items_discount_lc
  , COALESCE(c.djini_order_items_discount_eur, cp.djini_order_items_discount_eur, 0) AS djini_order_items_discount_eur
  , COALESCE(
      c.djini_order_items_supplier_funded_lc
      , cp.djini_order_items_supplier_funded_lc
      , 0
    ) AS djini_order_items_supplier_funded_lc
  , COALESCE(
      c.djini_order_items_supplier_funded_eur
      , cp.djini_order_items_supplier_funded_eur
      , 0
    ) AS djini_order_items_supplier_funded_eur
  , COALESCE(
      c.order_items_supplier_funded_lc
      , cp.order_items_supplier_funded_lc
      , 0
    ) AS order_items_supplier_funded_lc
  , COALESCE(
      c.order_items_supplier_funded_eur
      , cp.order_items_supplier_funded_eur
      , 0
    ) AS order_items_supplier_funded_eur
  -- price history
  , COALESCE(price_history.original_price_lc, qc_ol.unit_price_lc, 0) AS unit_price_listed_lc
  , COALESCE(price_history.original_price_lc, qc_ol.unit_price_lc, 0) / fx.fx_rate AS unit_price_listed_eur
  , CASE
      WHEN COALESCE(price_history.original_price_lc, qc_ol.unit_price_lc, 0) - qc_ol.unit_price_paid_lc > 0
        THEN COALESCE(
          COALESCE(price_history.original_price_lc, qc_ol.unit_price_lc, 0) - qc_ol.unit_price_paid_lc
          , 0
        )
      ELSE 0
    END AS unit_discount_lc
  , CASE
      WHEN COALESCE(price_history.original_price_lc, qc_ol.unit_price_lc, 0) - qc_ol.unit_price_paid_lc > 0
        THEN COALESCE(
          COALESCE(price_history.original_price_lc, qc_ol.unit_price_lc, 0) - qc_ol.unit_price_paid_lc
          , 0
        ) / fx.fx_rate
      ELSE 0
    END AS unit_discount_eur
  -- vat info
  , (
      qc_ol.total_amt_paid_lc
        / (1 + COALESCE(CAST(vat.product_vat_rate AS NUMERIC), qc_ol.vat_percentage, 0.0) / 100)
    ) AS total_amt_paid_net_lc
  , (
      (qc_ol.total_amt_paid_lc / fx.fx_rate)
        / (1 + COALESCE(CAST(vat.product_vat_rate AS NUMERIC), qc_ol.vat_percentage, 0.0) / 100)
    ) AS total_amt_paid_net_eur
  -- ppp = total_price_paid_net - cogs + supplier_funding
  , CAST(
      (
        (
          qc_ol.total_amt_paid_lc
            / (1 + COALESCE(CAST(vat.product_vat_rate AS NUMERIC), qc_ol.vat_percentage, 0.0) / 100)
        ) -- total_price_paid_net_lc
          - (COALESCE(wac.wac, 0) * qc_ol.quantity_delivered) -- amt_cogs_lc
          + (COALESCE(c.order_items_supplier_funded_lc, 0)) -- supplier_funding_lc
      ) AS NUMERIC
    ) AS total_ppp_lc
  , CAST(
      (
        (
          (qc_ol.total_amt_paid_lc / fx.fx_rate)
            / (1 + COALESCE(CAST(vat.product_vat_rate AS NUMERIC), qc_ol.vat_percentage, 0.0) / 100)
        ) -- total_price_paid_net_eur
          - (COALESCE(wac.wac, 0) / fx.fx_rate * qc_ol.quantity_delivered) -- amt_cogs_eur
          + (COALESCE(c.order_items_supplier_funded_eur, 0)) -- supplier_funding_eur
      ) AS NUMERIC
    ) AS total_ppp_eur
  -- Arrays
  , qc_ol.returns
  , qc_ol.parent_order_id
  , COALESCE(c.campaign_info, cp.campaign_info) AS campaign_info
  , COALESCE(c.combo_products, cp.combo_products) AS combo_products
  , CASE
      WHEN COALESCE(sbw.is_sold_by_weight, FALSE) OR COALESCE(sbp.is_sold_by_piece, FALSE)
        THEN qc_ol.quantity_ordered
    END AS ordered_weight
  , CASE
      WHEN COALESCE(sbw.is_sold_by_weight, FALSE) OR COALESCE(sbp.is_sold_by_piece, FALSE)
        THEN qc_ol.quantity_delivered
    END AS delivered_weight
  , CASE
      WHEN COALESCE(sbw.is_sold_by_weight, FALSE) OR COALESCE(sbp.is_sold_by_piece, FALSE)
        THEN qc_ol.quantity_sold
    END AS sold_weight
  , CASE
      WHEN COALESCE(sbw.is_sold_by_weight, FALSE) OR COALESCE(sbp.is_sold_by_piece, FALSE)
        THEN qc_ol.quantity_returned
    END AS returned_weight
  , CASE
      WHEN COALESCE(sbw.is_sold_by_weight, FALSE) OR COALESCE(sbp.is_sold_by_piece, FALSE)
        THEN 'KG'
    END AS weight_unit
  , qc_ol.weighted_pieces_ordered
  , qc_ol.weighted_pieces_picked_up
  , STRUCT(
      COALESCE(sbw.is_sold_by_weight, FALSE) AS is_sold_by_weight
      , COALESCE(sbp.is_sold_by_piece, FALSE) AS is_sold_by_piece
      , COALESCE(sbw.is_sold_by_weight, FALSE) OR COALESCE(sbp.is_sold_by_piece, FALSE) AS is_weightable
      , 'KG' AS weightable_unit
      , msw.minimum_starting_weight
      , 'KG' AS minimum_starting_weight_unit
      , awpp.average_weight_per_piece
    ) AS weightable_attributes
FROM qc_order_line_products AS qc_ol
LEFT JOIN warehouses AS w
  ON qc_ol.platform_vendor_id = w.join_key
  AND qc_ol.fulfilled_by_entity = w.global_entity_id
  AND qc_ol.order_created_at_utc BETWEEN w.valid_from_utc AND w.valid_to_utc
LEFT JOIN vendors AS v
  ON qc_ol.global_entity_id = v.global_entity_id
  AND qc_ol.platform_vendor_id = v.platform_vendor_id
LEFT JOIN wac_info AS wac
  ON qc_ol.fulfilled_by_entity = wac.global_entity_id
  AND w.warehouse_id = wac.warehouse_id
  AND qc_ol.sku = wac.sku
  AND qc_ol.order_created_at_utc BETWEEN wac.valid_from AND wac.valid_to
LEFT JOIN vat_history AS vat
  ON qc_ol.fulfilled_by_entity = vat.global_entity_id
  AND qc_ol.global_catalog_id = vat.global_catalog_id
  AND qc_ol.order_created_at_utc >= vat.timestamp_valid_from_utc
  AND qc_ol.order_created_at_utc < vat.timestamp_valid_to_utc
LEFT JOIN price_history
  ON qc_ol.fulfilled_by_entity = price_history.global_entity_id
  AND qc_ol.global_catalog_id = price_history.global_catalog_id
  AND qc_ol.order_created_at_utc >= price_history.valid_from_utc
  AND qc_ol.order_created_at_utc < price_history.valid_to_utc
LEFT JOIN campaign_orders AS c
  ON qc_ol.global_entity_id = c.global_entity_id
  AND LOWER(qc_ol.country_code) = LOWER(c.country_code)
  AND qc_ol.order_id = c.order_id
  AND qc_ol.global_catalog_id = c.global_catalog_id
  AND COALESCE(qc_ol.djini_order_item_id, 'X') = COALESCE(c.djini_order_item_id, 'X')
LEFT JOIN campaign_orders AS cp
  ON qc_ol.global_entity_id = cp.global_entity_id
  AND LOWER(qc_ol.country_code) = LOWER(cp.country_code)
  AND qc_ol.order_id = cp.order_id
  AND qc_ol.platform_product_id = cp.platform_product_id
  AND COALESCE(qc_ol.djini_order_item_id, 'X') = COALESCE(cp.djini_order_item_id, 'X')
  AND c.order_id IS NULL
LEFT JOIN fx_rates AS fx
  ON DATE(qc_ol.order_created_at_utc) = fx.exchange_rate_date
  AND qc_ol.currency_code = fx.currency_code
LEFT JOIN sales_buffer AS sb
  ON qc_ol.global_entity_id = sb.global_entity_id
  AND qc_ol.global_catalog_id = sb.global_catalog_id
  AND qc_ol.order_created_at_utc >= sb.timestamp_valid_from_utc
  AND qc_ol.order_created_at_utc < sb.timestamp_valid_to_utc
LEFT JOIN msw_history AS msw
  ON qc_ol.global_catalog_id = msw.global_catalog_id
  AND qc_ol.global_entity_id = msw.global_entity_id
  AND qc_ol.order_created_at_utc >= msw.valid_from_utc
  AND qc_ol.order_created_at_utc < msw.valid_to_utc
LEFT JOIN sbw_history AS sbw
  ON qc_ol.global_catalog_id = sbw.global_catalog_id
  AND qc_ol.global_entity_id = sbw.global_entity_id
  AND qc_ol.order_created_at_utc >= sbw.valid_from_utc
  AND qc_ol.order_created_at_utc < sbw.valid_to_utc
LEFT JOIN sbp_history AS sbp
  ON qc_ol.global_catalog_id = sbp.global_catalog_id
  AND qc_ol.global_entity_id = sbp.global_entity_id
  AND qc_ol.order_created_at_utc >= sbp.valid_from_utc
  AND qc_ol.order_created_at_utc < sbp.valid_to_utc
LEFT JOIN awpp_history AS awpp
  ON qc_ol.global_catalog_id = awpp.global_catalog_id
  AND qc_ol.global_entity_id = awpp.global_entity_id
  AND qc_ol.order_created_at_utc >= awpp.valid_from_utc
  AND qc_ol.order_created_at_utc < awpp.valid_to_utc
WHERE TRUE
  {%- if not params.backfill %}
  AND qc_ol.order_created_date_utc BETWEEN
    DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} + 1 DAY)
    AND
    DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
  {%- elif params.is_backfill_chunks_enabled %}
  AND qc_ol.order_created_date_utc BETWEEN
    '{{ params.backfill_start_date }}'
    AND
    '{{ params.backfill_end_date }}'
  {%- endif %}
