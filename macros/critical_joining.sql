{% macro critical_joining() %}
update_audience_name AS (
  SELECT *,
  COALESCE(REGEXP_EXTRACT(campaign_name, r'(?:PLATFORM|1PD)_([^_]+)'), 'Other') AS audience_name
  FROM deduplicated_campaign_data
),
joint_campaign_group as (
  SELECT update_audience_name.*,distinct_campaign_group_campaign_link.campaign_group_name
  FROM update_audience_name LEFT JOIN distinct_campaign_group_campaign_link ON update_audience_name.campaign_group_id = distinct_campaign_group_campaign_link.campaign_group_id

),
update_campaign_name as (
  select joint_campaign_group.* EXCEPT(campaign_name,campaign_group_name),
  CASE WHEN campaign_group_name is null then campaign_name else campaign_group_name 
  END AS campaign_name 
  FROM joint_campaign_group
),
overall_info_joining AS (
SELECT  update_campaign_name.* EXCEPT(campaign_id,campaign_start_date,row_num), creative_campaign_link.*  FROM creative_campaign_link LEFT JOIN  update_campaign_name ON creative_campaign_link.campaign_id=update_campaign_name.campaign_id
), 
{% endmacro %}