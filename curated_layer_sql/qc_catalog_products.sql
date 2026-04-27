CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products`
CLUSTER BY global_entity_id
AS
WITH vendor_warehouses AS (

  SELECT DISTINCT
    w.global_entity_id
    , w.warehouse_id
    , w.name AS warehouse_name
    , w.is_dmart
    , w.is_lbi_v2_enabled
    , COALESCE(v.platform_vendor_id, v.additional_platform_vendor_id) AS platform_vendor_id
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.warehouses_v2` AS w
    , UNNEST(w.vendors) AS v
  WHERE TRUE
    -- filtering out warehouse-vendor pairs which were migrated
    AND v.migrated_at_utc IS NULL

), vat_rate_history AS (

  SELECT
    vrh.global_entity_id
    , vrh.vendor_id
    , vrh.global_catalog_id
    , vrh.timestamp_valid_from
    , vrh.timestamp_valid_until
    , vrh.vat_rate
    , vrh.current_flag
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._dim_product_vat_rate_history` AS vrh
  WHERE TRUE
    AND DATE(vrh.timestamp_valid_from) <= DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
    AND vrh.current_flag

), dim_sales_buffer AS (

  SELECT
    sbh.global_entity_id
    , sbh.global_catalog_id
    , sbh.sales_buffer
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_sales_buffer_history` AS sbh
  WHERE TRUE
    AND DATE(sbh.valid_from_lt) <= DATE_ADD('{{ next_ds }}', INTERVAL 1 DAY)
  QUALIFY ROW_NUMBER() OVER (
    -- sales buffer can have more than 1 sku, even though its the same vendor_product i.e. has the same
    -- global_catalog_id. As of 2023-07-25, such cases are observed for PeYa countries.
    -- e.g. 0244190313131 and 244190313131 considered different SKU.
    PARTITION BY sbh.global_entity_id, sbh.global_catalog_id
    ORDER BY sbh.valid_to_utc DESC, sbh.valid_from_utc DESC, sbh.src DESC
  ) = 1

), vendor_products AS (

  SELECT
    ct_vp.catalog_country_code
    , ct_vp.global_entity_id
    , ct_vp.catalog_vendor_product_id
    , ct_vp.catalog_vendor_id
    , ct_vp.catalog_chain_product_id
    , ct_vp.platform_product_id
    , ct_vp.product_remote_id
    , ct_vp.product_variation_id
    , ct_vp.global_catalog_id
    , ct_vp.vendor_product_created_at_utc
    , ct_vp.vendor_product_updated_at_utc
    , ct_vp.vendor_product_is_active
    , ct_vp.vendor_product_is_available
    , ct_vp.vendor_product_is_deleted
    , ct_vp.sales_buffer
    , ct_vp.status
    , ct_vp.sync_result
    , ct_vp.maximum_sales_quantity
    , ct_vp.catalog_original_price_lc
    , ct_vp.catalog_price_lc
    , ct_vp.category_tree_source
    , ct_vp.has_evd_category
    , ct_vp.categories
    , ct_vp.tags
    , ct_vp.catalog_global_vendor_id
    , ct_vp.catalog_vendor_name
    , ct_vp.platform_vendor_id
    , ct_vp.catalog_remote_vendor_id
    , ct_vp.catalog_additional_remote_vendor_id
    , ct_vp.vendor_is_active
    , ct_vp.vendor_is_deleted
    , ct_vp.category_tree_id
    , ct_vp.category_tree_name_english
    , ct_vp.category_tree_name_local
    , ct_vp.vendor_created_at_utc
    , ct_vp.vendor_updated_at_utc
    , ct_vp.type
    , ct_vp.created_by_client
    , ct_vp.created_by_component
    , COALESCE(v_w.warehouse_id, '') AS warehouse_id
    , v_w.warehouse_name
    , COALESCE(v_w.is_dmart, FALSE) AS is_dmart
    , v_w.is_lbi_v2_enabled
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._catalog_vendor_products` AS ct_vp
  LEFT JOIN vendor_warehouses AS v_w
    ON ct_vp.global_entity_id = v_w.global_entity_id
    AND ct_vp.platform_vendor_id = v_w.platform_vendor_id

), vendor_products_wflags AS (

  SELECT
    vp.catalog_country_code
    , vp.global_entity_id
    , vp.catalog_chain_product_id
    , vp.catalog_vendor_product_id
    , vp.catalog_vendor_id
    , vp.platform_product_id
    , vp.product_remote_id
    , vp.product_variation_id
    , vp.global_catalog_id
    , vp.vendor_product_created_at_utc
    , vp.vendor_product_updated_at_utc
    , vp.vendor_product_is_active
    , vp.vendor_product_is_available
    , vp.vendor_product_is_deleted
    , vp.status
    , vp.sync_result
    , vp.maximum_sales_quantity
    , vp.catalog_original_price_lc
    , vp.catalog_price_lc
    , vp.has_evd_category
    , vp.categories
    , vp.tags
    , vp.catalog_global_vendor_id
    , vp.catalog_vendor_name
    , vp.category_tree_source
    , vp.platform_vendor_id
    , vp.catalog_remote_vendor_id
    , vp.catalog_additional_remote_vendor_id
    , vp.vendor_is_active
    , vp.vendor_is_deleted
    , vp.category_tree_id
    , vp.category_tree_name_english
    , vp.category_tree_name_local
    , vp.vendor_created_at_utc
    , vp.vendor_updated_at_utc
    , vp.warehouse_id
    , vp.warehouse_name
    , vp.is_dmart
    , CASE
        WHEN vp.type = 'SHOP' THEN COALESCE(vp.sales_buffer, dsb.sales_buffer, 0)
        ELSE COALESCE(dsb.sales_buffer, 0)
      END AS sales_buffer
    , vp.created_by_client
    , vp.created_by_component
    , CAST(vrh.vat_rate AS NUMERIC) AS vat_rate
    , p_l.row
    , p_l.rack
    , p_l.shelf
    , p_l.storage_type
    , p_l.location_movement_request_strategy
  FROM vendor_products AS vp
  LEFT JOIN dim_sales_buffer AS dsb
    ON vp.global_entity_id = dsb.global_entity_id
    AND vp.global_catalog_id = dsb.global_catalog_id
  LEFT JOIN vat_rate_history AS vrh
    ON vp.global_catalog_id = vrh.global_catalog_id
  LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._products_location` AS p_l
    ON vp.warehouse_id = p_l.warehouse_id
    AND vp.global_catalog_id = p_l.catalog_product_id
    AND NOT vp.is_lbi_v2_enabled

), vendor_product_aggregation AS (

  SELECT
    vp_f.global_entity_id
    , vp_f.catalog_chain_product_id
    , LOGICAL_OR(vp_f.has_evd_category) AS has_evd_category
    , ARRAY_AGG(
        STRUCT(
          vp_f.catalog_vendor_product_id
          , vp_f.catalog_vendor_id
          , vp_f.platform_product_id
          , vp_f.product_remote_id
          , vp_f.product_variation_id
          , vp_f.global_catalog_id
          , vp_f.vendor_product_created_at_utc
          , vp_f.vendor_product_updated_at_utc
          , vp_f.vendor_product_is_active
          , vp_f.vendor_product_is_available
          , vp_f.vendor_product_is_deleted
          , vp_f.status
          , vp_f.sync_result
          , vp_f.maximum_sales_quantity
          , vp_f.catalog_original_price_lc
          , vp_f.catalog_price_lc
          , vp_f.categories
          , vp_f.tags
          , vp_f.catalog_global_vendor_id
          , vp_f.catalog_vendor_name
          , vp_f.category_tree_source
          , vp_f.platform_vendor_id
          , vp_f.catalog_remote_vendor_id
          , vp_f.catalog_additional_remote_vendor_id
          , vp_f.vendor_is_active
          , vp_f.vendor_is_deleted
          , vp_f.category_tree_id
          , vp_f.category_tree_name_english
          , vp_f.category_tree_name_local
          , vp_f.vendor_created_at_utc
          , vp_f.vendor_updated_at_utc
          , vp_f.vat_rate
          , vp_f.warehouse_id
          , vp_f.warehouse_name
          , vp_f.is_dmart
          , vp_f.sales_buffer
          , vp_f.row
          , vp_f.rack
          , vp_f.shelf
          , vp_f.storage_type
          , vp_f.location_movement_request_strategy
          , STRUCT(
              vp_f.created_by_client AS client
              , vp_f.created_by_component AS component
            ) AS vendor_product_created_by
        )
      ) AS vendor_products
  FROM vendor_products_wflags AS vp_f
  GROUP BY 1, 2

)
SELECT
  ct_cp.global_entity_id
  , ct_cp.catalog_region
  , ct_cp.country_code
  , ct_cp.catalog_master_product_id
  , ct_cp.pim_product_id
  , ct_cp.product_name
  , ct_cp.product_name_english
  , ct_cp.pim_product_name_english
  , ct_cp.product_name_local
  , ct_cp.pim_product_name_local
  , ct_cp.local_language_locale
  , COALESCE(
      ct_cp.product_description_english
      , ct_cp.product_description_local
    ) AS product_description
  , ct_cp.product_description_english
  , ct_cp.product_description_local
  , ct_cp.product_image_url
  , ct_cp.pim_image_url
  , ct_cp.is_chain_product_override
  , ct_cp.is_sold_by_weight
  , ct_cp.is_bundle
  , ct_cp.weight_value
  , ct_cp.weight_unit
  , ct_cp.length_in_cm
  , ct_cp.width_in_cm
  , ct_cp.height_in_cm
  , ct_cp.shelf_life_in_days
  , ct_cp.freshness_guarantee_in_days
  , ct_cp.min_receivings_shelf_life_days
  , ct_cp.product_type
  , ct_cp.product_segment_id
  , ct_cp.master_categories
  , ct_cp.barcodes
  , COALESCE(
      ct_cp.brand_name_english
      , ct_cp.brand_name_local
    ) AS brand_name
  , ct_cp.master_product_created_at_utc
  , ct_cp.catalog_chain_product_id
  , ct_cp.catalog_chain_id
  , CASE WHEN src.chain_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_chain_from_dmart
  , ct_cp.catalog_chain_name
  , ct_cp.sku
  , ct_cp.chain_product_created_at_utc
  , ct_cp.is_weightable
  , ct_cp.weightable_attributes
  , (
      ct_cp.is_evd_brand
      OR
      (ct_cp.brand_name_english IN ('EveryDay', 'unbranded', 'ComboProduct Brand') AND vp_a.has_evd_category)
    ) AS is_evd_product
  , vp_a.vendor_products
  , CURRENT_TIMESTAMP() AS last_execution_at
FROM `{{ params.project_id }}.{{ params.dataset.cl }}._catalog_chain_products` AS ct_cp
LEFT JOIN vendor_product_aggregation AS vp_a
  ON ct_cp.global_entity_id = vp_a.global_entity_id
  AND ct_cp.catalog_chain_product_id = vp_a.catalog_chain_product_id
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sources` AS src
  ON ct_cp.global_entity_id = src.global_entity_id
