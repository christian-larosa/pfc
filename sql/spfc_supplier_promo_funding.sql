WITH date_context AS (

  -- DO NOT change the CTE for filtered_dates, scenario_flags and date_context, without assessing the tasks.
  -- The scheduling of this pipeline is one of the key criterion in reading and filtering data for the module.
  SELECT
-- Using `next_ds` as the input date because the pipeline processes data for the day following the execution date.
    DATE('{{ next_ds }}') AS input_date
    , DATE_TRUNC(DATE('{{ next_ds }}'), WEEK (MONDAY)) AS current_week_monday
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

), sku_grouped AS (
  SELECT
    sf.global_entity_id
    , sf.country_code
    , sf.currency_code
    , sf.supplier_id
    , sf.global_supplier_id
    , sf.supplier_name
    , sf.root_id
    , ARRAY_AGG(
        STRUCT(
            sf.sku
            , sf.product_name
            , sf.brand_name
            , sf.category_level_one
            , sf.barcodes
            , sf.avg_unit_price_listed_lc
            , sf.avg_unit_discount_lc
            , sf.avg_unit_price_paid_lc
            , sf.quantity_sold
            , sf.total_discount_lc
            , sf.total_amt_paid_lc
            , sf.supplier_funded_amount_lc
            , SAFE_DIVIDE(sf.supplier_funded_amount_lc, sf.quantity_sold) AS unit_supplier_funded_amount_lc
      )
    ) AS skus
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.spfc_sku_supplier_promo_funding` AS sf
  INNER JOIN filtered_dates AS fd
  ON DATE(sf.campaign_concluded_date_utc) BETWEEN fd.filter_start_date AND fd.filter_end_date
  GROUP BY 1, 2, 3, 4, 5, 6, 7

)
SELECT
  s.global_entity_id
  , s.country_code
  , s.currency_code
  , s.supplier_id
  , s.global_supplier_id
  , s.supplier_name
  , fd.filter_start_date AS campaign_concluded_week_utc
  , ARRAY_AGG(
      STRUCT(
        c.root_id
        , c.campaign_name
        , c.campaign_type
        , c.campaign_subtype
        , c.reason
        , c.campaign_created_at_utc
        , c.campaign_start_at_utc
        , c.campaign_end_at_utc
        , c.child_campaigns
        , s.skus
      )
    ) AS campaigns
FROM `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_campaigns` AS c
INNER JOIN filtered_dates AS fd
  ON DATE(c.campaign_end_at_utc) BETWEEN fd.filter_start_date AND fd.filter_end_date
INNER JOIN sku_grouped AS s
  ON c.global_entity_id = s.global_entity_id
  AND c.country_code = s.country_code
  AND c.root_id = s.root_id
GROUP BY 1, 2, 3, 4, 5, 6, 7
