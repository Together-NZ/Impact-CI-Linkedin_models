{% macro lk_campaign_group_campaign_link(source_name, table_name) %}
campaign_group_campaign_link AS (
  SELECT DISTINCT JSON_VALUE(data,'$.name') AS campaign_group_name,JSON_VALUE(data,'$.id') AS campaign_group_id,date(timestamp(JSON_VALUE(data,'$.last_modified_time')) ,'Pacific/Auckland')as modified_date, ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.last_modified_time') DESC) AS row_num
   FROM {{ source(source_name, table_name) }} 
  ),
distinct_campaign_group_campaign_link AS (
  SELECT * FROM campaign_group_campaign_link WHERE modified_date > '2025-09-01' and row_num=1 and lower(campaign_group_name) like '%uow%'
)
{% endmacro %}