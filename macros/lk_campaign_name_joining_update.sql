{% macro lk_campaign_name_joining_update(source_name, table_name) %}
final AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY daily_stats.creative_id,date,creative_id_1) AS row_num FROM daily_stats LEFT JOIN creative_campaign_link ON
daily_stats.creative_id = creative_campaign_link.creative_id_1 ),
stats_by_creative_campaign_id AS (
SELECT * except(row_num) FROM final where row_num = 1),
campaign_stats AS (      SELECT
    JSON_VALUE(data, '$.id') AS campaign_id,
    JSON_VALUE(data, '$.account_id') as advertiser_account_id,
    JSON_VALUE(data, '$.name') AS campaign_name,
    JSON_VALUE(data, '$.objective_type') as campaign_type,
    JSON_VALUE(data, '$.totalBudget.amount') as campaign_budget,
    JSON_VALUE(data, '$.status') as campaign_status,
    JSON_VALUE(data,'$.campaign_group_id') AS campaign_group_id,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")) AS campaign_start_date,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) AS camapign_end_date,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data, '$.id'),JSON_VALUE(data, '$.account_id'),FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")), FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) ) as row_num
    from {{ source(source_name, table_name) }}),
deduplicated_campaign_data AS (SELECT * EXCEPT(row_num) FROM campaign_stats where row_num = 1),
joint_campaign_group as (
  SELECT deduplicated_campaign_data.*,distinct_campaign_group_campaign_link.campaign_group_name
  FROM deduplicated_campaign_data LEFT JOIN distinct_campaign_group_campaign_link ON deduplicated_campaign_data.campaign_group_id = distinct_campaign_group_campaign_link.campaign_group_id

),
update_campaign_name as (
  select joint_campaign_group.* EXCEPT(campaign_name,campaign_group_name),
  CASE WHEN campaign_group_name is null then campaign_name else campaign_group_name 
  END AS campaign_name 
  FROM joint_campaign_group
)
{% endmacro %}