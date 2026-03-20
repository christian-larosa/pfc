WITH date_context AS (

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

), partition_dates AS (

  SELECT pd AS partition_date
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT fd.filter_start_date FROM filtered_dates AS fd)
    , (SELECT fd.filter_end_date FROM filtered_dates AS fd)
  )) AS pd

)
SELECT partition_dates.partition_date FROM partition_dates
