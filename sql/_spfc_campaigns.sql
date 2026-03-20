WITH date_context AS (

  -- DO NOT change the CTE for filtered_dates, scenario_flags and date_context, without assessing the tasks.
  -- The scheduling of this pipeline is one of the key criterion in reading and filtering data for the module.
  SELECT
-- Using `next_ds` as the input date because the pipeline processes data for the day following the execution date.
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

), filtered_campaigns AS (
-- Get campaigns that match filtering criteria
  SELECT
    qc.global_entity_id
    , qc.country_code
    , qc.campaign_id
    , qc.root_id
    , qc.campaign_name
    , qc.campaign_type
    , qc.campaign_subtype
    , qc.reason
    , qc.created_at_utc
    , qc.start_at_utc
    , qc.end_at_utc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_campaigns` AS qc
  INNER JOIN `{{ params.project_id }}.{{ params.dataset.cl }}._spfc_enabled_global_entities` AS ge
    ON qc.global_entity_id = ge.global_entity_id
  INNER JOIN filtered_dates AS fd
    ON DATE(qc.end_at_utc) BETWEEN fd.filter_start_date AND fd.filter_end_date
  WHERE TRUE
    AND qc.state = 'READY'
    AND qc.is_valid = TRUE
    AND (
      qc.global_entity_id != 'TB_KW'
      -- ignore qcaas and gulfmart campaigns only for TB_KW
      OR NOT REGEXP_CONTAINS(LOWER(qc.campaign_name), r'qcaas|gulfmart')
    )

), campaign_dates AS (

-- Get min/max dates for each root campaign
  SELECT
    child.root_id
    , child.global_entity_id
    , child.country_code
    , MIN(child.created_at_utc) AS min_created_at_utc
    , MIN(child.start_at_utc) AS min_start_at_utc
    , MAX(child.end_at_utc) AS max_end_at_utc
  FROM filtered_campaigns AS child
  WHERE child.root_id IS NOT NULL
  GROUP BY 1, 2, 3
)
-- Get root campaigns with their child campaigns aggregated
SELECT
  COALESCE(root.global_entity_id, child.global_entity_id) AS global_entity_id
  , COALESCE(root.country_code, child.country_code) AS country_code
  , COALESCE(root.campaign_id, child.root_id) AS campaign_id
  , COALESCE(root.root_id, child.root_id) AS root_id
  , COALESCE(root.campaign_name, child.campaign_name) AS campaign_name
  , COALESCE(root.campaign_type, child.campaign_type) AS campaign_type
  , COALESCE(root.campaign_subtype, child.campaign_subtype) AS campaign_subtype
  , COALESCE(root.reason, child.reason) AS reason
  , COALESCE(cd.min_created_at_utc, child.created_at_utc) AS campaign_created_at_utc
  , COALESCE(cd.min_start_at_utc, child.start_at_utc) AS campaign_start_at_utc
  , COALESCE(cd.max_end_at_utc, child.end_at_utc) AS campaign_end_at_utc
  , ARRAY_AGG(
      IF(child.campaign_id != child.root_id
      , STRUCT(
          child.campaign_id
          , child.campaign_name
          , child.campaign_type
          , child.campaign_subtype
          , child.reason
          , child.created_at_utc
          , child.start_at_utc
          , child.end_at_utc
        )
      , NULL) IGNORE NULLS) AS child_campaigns
FROM filtered_campaigns AS child
LEFT JOIN filtered_campaigns AS root
  ON child.global_entity_id = root.global_entity_id
  AND child.country_code = root.country_code
  AND child.root_id = root.campaign_id
  AND root.campaign_id = root.root_id
LEFT JOIN campaign_dates AS cd
  ON child.root_id = cd.root_id
  AND child.global_entity_id = cd.global_entity_id
  AND child.country_code = cd.country_code
WHERE TRUE
  AND child.root_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
