{% macro lk_creative_campaign_link(source_name, table_name) %}
creative_campaign_link_raw AS (
  SELECT
  SPLIT(JSON_VALUE(data,'$.campaign'),':')[3] AS campaign_id,
  SPLIT(JSON_VALUE(data,'$.id'),':')[3] AS creative_id_1,
  JSON_VALUE(data,'$.name') AS creative_name,
  ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.last_modified_time'),_sdc_extracted_at DESC) AS row_num
  
        FROM 
        {{ source(source_name, table_name) }}
),
creative_campaign_link AS (
  SELECT * EXCEPT(row_num) FROM creative_campaign_link_raw WHERE row_num = 1
)
{% endmacro %}