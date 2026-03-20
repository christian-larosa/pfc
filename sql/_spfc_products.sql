CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_products`
CLUSTER BY country_code, sku
AS
WITH qc_catalog_products AS (

  SELECT
    vp.warehouse_id
    , qcp.sku
    , qcp.global_entity_id
    , qcp.country_code
    , qcp.product_name
    , qcp.brand_name
    , mc.master_category_names.level_one AS category_level_one
    , (SELECT ARRAY_TO_STRING(ARRAY_AGG(DISTINCT b.barcode), ',') AS t
        FROM UNNEST(qcp.barcodes) AS b
      ) AS barcodes
    , vp.platform_vendor_id
    , qcp.master_product_created_at_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qcp
  LEFT JOIN UNNEST(qcp.vendor_products) AS vp
  LEFT JOIN UNNEST(qcp.master_categories) AS mc
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS ge
    ON qcp.global_entity_id = ge.global_entity_id
  WHERE TRUE
    AND vp.warehouse_id IS NOT NULL
    AND vp.warehouse_id != ''
  QUALIFY ROW_NUMBER() OVER latest_chain = 1
  WINDOW latest_chain AS (
    PARTITION BY
      qcp.global_entity_id
      , qcp.sku
      , vp.warehouse_id
      , vp.platform_vendor_id
    ORDER BY qcp.chain_product_created_at_utc DESC NULLS LAST
  )

), dc_warehouse_mappings AS (

  SELECT DISTINCT
    ps.global_entity_id
    , pr.country_code
    , pr.sku
    , pr.dc_warehouse_id
    , pr.warehouse_id
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.product_replenishment` AS pr
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
    ON pr.country_code = ps.country_code
    AND pr.sku = ps.sku
  WHERE pr.dc_warehouse_id IS NOT NULL

), products_suppliers AS (

  SELECT DISTINCT
    ps.global_entity_id
    , ps.country_code
    , ps.sku
    , s.supplier_id
    , w.warehouse_id
    , w.is_preferred_supplier
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS ge
    ON ps.global_entity_id = ge.global_entity_id
    , UNNEST(ps.suppliers) AS s
    , UNNEST(s.warehouses) AS w
  LEFT JOIN dc_warehouse_mappings AS dcm
    ON ps.country_code = dcm.country_code
    AND ps.sku = dcm.sku
    AND w.warehouse_id = dcm.dc_warehouse_id
  WHERE TRUE
    AND s.is_supplier_deleted = FALSE
    AND w.warehouse_id IS NOT NULL
    AND s.supplier_id IS NOT NULL
    AND dcm.warehouse_id IS NULL
  UNION ALL
  SELECT DISTINCT
    dcm.global_entity_id
    , dcm.country_code
    , dcm.sku
    , s.supplier_id
    , dcm.warehouse_id
    , w.is_preferred_supplier
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS ge
    ON ps.global_entity_id = ge.global_entity_id
    , UNNEST(ps.suppliers) AS s
    , UNNEST(s.warehouses) AS w
  INNER JOIN dc_warehouse_mappings AS dcm
    ON ps.country_code = dcm.country_code
    AND ps.sku = dcm.sku
    AND w.warehouse_id = dcm.dc_warehouse_id
  WHERE TRUE
    AND s.is_supplier_deleted = FALSE
    AND w.warehouse_id IS NOT NULL
    AND s.supplier_id IS NOT NULL

-- ), 
-- TB_KW_307_replacement_suppliers AS (

--   SELECT
--     ps.global_entity_id
--     , ps.country_code
--     , ps.sku
--     , ANY_VALUE(ps.supplier_id) AS replacement_supplier_id -- any one matching preferred supplier
--   FROM products_suppliers AS ps
--   WHERE
--       ps.supplier_id != 307
--       AND ps.global_entity_id = 'TB_KW'
--   GROUP BY
--     global_entity_id
--     , country_code
--     , sku

), supplier_products AS (

    SELECT
      ps.global_entity_id
      , ps.country_code
      , ps.sku
      , ps.supplier_id
      , ps.warehouse_id
    FROM products_suppliers AS ps
    LEFT JOIN qc_catalog_products AS qcp
        ON ps.global_entity_id = qcp.global_entity_id
        AND ps.country_code = qcp.country_code
        AND ps.sku = qcp.sku
        AND ps.warehouse_id = qcp.warehouse_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            ps.global_entity_id
            , ps.country_code
            , ps.sku
            , ps.warehouse_id
        ORDER BY
            ps.is_preferred_supplier DESC
            , qcp.master_product_created_at_utc DESC
    ) = 1

), srm_suppliers AS (

  SELECT
  a.global_entity_id
  , a.country_code
  , a.name AS supplier_name
  , a.srm_gsid__c AS global_supplier_id
  , a.srm_supplierportalid__c AS supplier_id
  FROM 
    `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS ge
    ON a.global_entity_id = ge.global_entity_id

)
SELECT DISTINCT
  qcp.global_entity_id
  , qcp.country_code
  , sp.supplier_id
  , ss.global_supplier_id
  , ss.supplier_name
  , qcp.sku
  , qcp.product_name
  , qcp.brand_name
  , qcp.category_level_one
  , qcp.barcodes
  , qcp.warehouse_id
  , qcp.platform_vendor_id
  , qcp.master_product_created_at_utc
FROM qc_catalog_products AS qcp
LEFT JOIN supplier_products AS sp
ON qcp.global_entity_id = sp.global_entity_id
  AND qcp.country_code = sp.country_code
  AND qcp.sku = sp.sku
  AND qcp.warehouse_id = sp.warehouse_id
LEFT JOIN srm_suppliers AS ss
ON sp.global_entity_id = ss.global_entity_id
  AND sp.country_code = ss.country_code
  AND CAST(sp.supplier_id AS STRING) = ss.supplier_id
