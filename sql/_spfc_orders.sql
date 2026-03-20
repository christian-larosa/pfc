WITH date_context AS (

  -- DO NOT change the CTE for filtered_dates, scenario_flags and date_context, without assessing the tasks.
  -- The scheduling of this pipeline is one of the key criterion in reading and filtering data for the module.
  SELECT
    DATE('{{ next_ds }}') AS input_date
    , DATE_TRUNC(DATE('{{ next_ds }}'), WEEK(MONDAY)) AS current_week_monday
    , DATE_TRUNC(DATE('{{ next_ds }}'), MONTH) AS first_day_of_input_month

), scenario_flags AS (

  SELECT
    dc.input_date
    , dc.current_week_monday
    , dc.first_day_of_input_month
    -- Flag for '1st of month AND NOT a Monday'
    , (EXTRACT(DAY FROM dc.input_date) = 1
        AND FORMAT_DATE('%A', dc.input_date) != 'Monday') AS is_first_of_month_not_monday
    -- Flag for 'First Monday AFTER the 1st of the month'
    , (FORMAT_DATE('%A', dc.input_date) = 'Monday'
        AND dc.input_date > dc.first_day_of_input_month
        AND dc.input_date <= DATE_ADD(dc.first_day_of_input_month, INTERVAL 6 DAY)) AS is_first_monday_after_first
    -- Note: If input_date is the 1st of the month AND a Monday, it will fall into the ELSE (default) case.
    -- Example: 2025-09-01 is a Monday, and the 1st of the month.
    -- This flag (is_first_monday_after_first) correctly resolves to FALSE for 2025-09-01,
    -- as it's not strictly "after" the 1st for this specific scenario.
    FROM date_context AS dc

), filtered_dates AS (

  SELECT
    -- Determine the start date for filtering
    CASE
      -- Scenario: 1st of month & not Monday
      WHEN sf.is_first_of_month_not_monday THEN sf.current_week_monday
      -- Scenario: First Monday after 1st of month
      WHEN sf.is_first_monday_after_first THEN sf.first_day_of_input_month
      -- Default: Previous Monday
      ELSE DATE_SUB(sf.current_week_monday, INTERVAL 7 DAY)
    END AS filter_start_date
    -- Determine the end date for filtering
    , CASE
      -- Scenario: 1st of month & not Monday
      WHEN sf.is_first_of_month_not_monday THEN LAST_DAY(sf.input_date, MONTH)
      -- Scenario: First Monday after 1st of month
      WHEN sf.is_first_monday_after_first THEN DATE_SUB(sf.input_date, INTERVAL 1 DAY)
      -- Default: Previous Sunday
      ELSE DATE_SUB(sf.current_week_monday, INTERVAL 1 DAY)
    END AS filter_end_date
  FROM scenario_flags AS sf

), _spfc_products AS (

  SELECT
    spfc_p.global_entity_id
    , spfc_p.country_code
    , spfc_p.supplier_id
    , spfc_p.global_supplier_id
    , spfc_p.supplier_name
    , spfc_p.sku
    , spfc_p.product_name
    , spfc_p.brand_name
    , spfc_p.category_level_one
    , spfc_p.barcodes
    , spfc_p.warehouse_id
    , spfc_p.platform_vendor_id
    , spfc_p.master_product_created_at_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_products` AS spfc_p

), campaigns AS (
  -- Create a flat list of all relevant campaign IDs by combining root campaigns
  -- with their children from the `child_campaigns` array.
  SELECT
    pc.global_entity_id
    , pc.root_id
    , pc.campaign_id
    , pc.campaign_type
    , pc.campaign_start_at_utc
    , pc.campaign_end_at_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_campaigns` AS pc
  UNION DISTINCT
  SELECT
    pc.global_entity_id
    , pc.root_id
    , campaign.campaign_id
    , campaign.campaign_type
    , campaign.start_at_utc AS campaign_start_at_utc
    , campaign.end_at_utc AS campaign_end_at_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_campaigns` AS pc
  LEFT JOIN UNNEST (pc.child_campaigns) AS campaign

), qc_orders AS (

  SELECT
    DATE(c.campaign_end_at_utc) AS campaign_concluded_date_utc
    , c.campaign_end_at_utc AS campaign_concluded_at_utc
    , qo.order_created_date_lt
    , qo.global_entity_id
    , qo.country_code
    , qo.currency_code
    , qo.warehouse_id
    , qo.platform_vendor_id
    , ci.campaign_id
    , c.campaign_type
    , c.root_id
    , i.sku
    , i.quantity_sold
    -- listed price for customer for one sku
    , i.value_lc.unit_price_listed_lc
    --  discount per (one) sku calculated by (total discount / qty sold)
    , i.value_lc.unit_discount_lc
    -- price paid for per (one) sku
    , i.value_lc.unit_price_paid_lc
    --  promo discount for the sku on all quantities sold
    , i.value_lc.djini_order_items_discount_lc AS total_discount_lc
    -- total amount paid for all quantities sold of sku
    , i.value_lc.total_amt_paid_lc
    -- supplier funded amount for the sku on all quantities sold
    , i.value_lc.djini_order_items_supplier_funded_lc AS order_items_supplier_funded_lc
    -- following information is just for cross verification with above value
    -- listed price for customer for one sku, same as unit_price_listed_lc
    , ci.product_unit_price_lc
    -- total promo discount for the sku on all quantities sold, same as djini_order_items_discount_lc
    , ci.campaign_discount_amt_lc
    -- supplier funded amount for the sku on all quantities sold, same as djini_order_items_supplier_funded_lc
    , ci.campaign_supplier_funded_amt_lc
    -- refer the dev notes in table description for the campaign_type value mapping
    , COALESCE(
        CASE
          WHEN c.campaign_type = 'GroupValue' THEN i.value_lc.djini_order_items_discount_lc
          ELSE i.value_lc.djini_order_items_supplier_funded_lc
        END, 0) AS supplier_funded_amount_lc
    -- this is calculated as the unit promo funding by supplier in promo tool multiplied by qty sold
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_orders` AS qo
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS ge
    ON qo.global_entity_id = ge.global_entity_id
  LEFT JOIN UNNEST(qo.items) AS i
  LEFT JOIN UNNEST(i.campaign_info) AS ci
  INNER JOIN campaigns AS c
    ON qo.global_entity_id = c.global_entity_id
    AND ci.campaign_id = c.campaign_id
  INNER JOIN filtered_dates AS fd
    ON DATE(c.campaign_end_at_utc) BETWEEN fd.filter_start_date AND fd.filter_end_date
  WHERE TRUE
    AND qo.is_dmart = TRUE
    AND qo.is_successful = TRUE
    AND qo.is_failed = FALSE
    AND qo.is_cancelled = FALSE
    AND qo.order_created_date_lt >= DATE_SUB(DATE('{{ next_ds }}'), INTERVAL 20 MONTH)
    AND qo.order_created_date_lt >= DATE_SUB(DATE(fd.filter_start_date), INTERVAL 18 MONTH)
    AND i.quantity_sold > 0
    AND qo.warehouse_id NOT IN UNNEST({{ params.param_qcaas_warehouse }})
    AND qo.platform_vendor_id NOT IN UNNEST({{ params.disabled_platform_vendor_ids }})
)
SELECT
qo.campaign_concluded_date_utc
, qo.campaign_concluded_at_utc
, p.supplier_id
, p.global_supplier_id
, p.supplier_name
, qo.order_created_date_lt
, qo.global_entity_id
, qo.country_code
, qo.currency_code
, qo.warehouse_id
, qo.campaign_id
, qo.root_id
, qo.sku
, qo.quantity_sold
, qo.unit_price_listed_lc
, qo.unit_discount_lc
, qo.unit_price_paid_lc
, qo.total_discount_lc
, qo.total_amt_paid_lc
, qo.order_items_supplier_funded_lc
, qo.product_unit_price_lc
, qo.campaign_discount_amt_lc
, qo.campaign_supplier_funded_amt_lc
, qo.supplier_funded_amount_lc -- this the one to use for funding
, p.product_name
, p.brand_name
, p.category_level_one
, p.barcodes
FROM qc_orders AS qo
LEFT JOIN _spfc_products AS p
  ON qo.global_entity_id = p.global_entity_id
  AND qo.country_code = p.country_code
  AND qo.warehouse_id = p.warehouse_id
  AND qo.sku = p.sku
  AND qo.platform_vendor_id = p.platform_vendor_id
WHERE TRUE
  AND qo.supplier_funded_amount_lc > 0
