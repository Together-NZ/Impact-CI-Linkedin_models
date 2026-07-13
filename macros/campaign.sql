{% macro campaign(source_name, table_name) %}
campaign as (
  SELECT
    JSON_VALUE(data, '$.id') AS campaign_id,
    JSON_VALUE(data, '$.account_id') as advertiser_account_id,
    JSON_VALUE(data, '$.name') AS campaign_name,
    JSON_VALUE(data, '$.objective_type') as campaign_type,
    JSON_VALUE(data, '$.totalBudget.amount') as campaign_budget,
    JSON_VALUE(data, '$.status') as campaign_status,
    LOWER(JSON_VALUE(data, '$.optimization_target_type')) AS optimization_goal,
    JSON_VALUE(data,'$.campaign_group_id') AS campaign_group_id,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")) AS campaign_start_date,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) AS camapign_end_date,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data, '$.id'),JSON_VALUE(data, '$.account_id'),FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")), FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) ORDER BY _sdc_extracted_at DESC) as row_num
    from {{ source(source_name, table_name) }}
),
 deduplicated_campaign_data AS (
SELECT * from campaign where row_num=1),
{% endmacro %}