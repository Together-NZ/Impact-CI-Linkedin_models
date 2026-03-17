{% macro lk_final_output() %}
SELECT stat_creative.*, campaign_data.* EXCEPT(campaign_id) ,
SPLIT(campaign_name,'_')[OFFSET(1)] AS campaign_descr,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 8 THEN SPLIT(campaign_name, '_')[OFFSET(7)] 
  ELSE NULL
END AS audience_name,
CASE 
        WHEN SPLIT (campaign_name,'_')[OFFSET(2)] LIKE '%SOCIAL%'
        AND (
            lower(creative_name) LIKE '%vid%'
            OR lower(campaign_name) LIKE '%vid%'
        ) THEN 'Social Video'
        WHEN SPLIT (campaign_name,'_')[OFFSET(2)] LIKE '%SOCIAL%'
        AND (
            lower(creative_name) NOT LIKE '%vid%'
            AND lower(campaign_name) NOT LIKE '%vid%'
        )
        THEN 'Social Display'
        ELSE 'Other'
  END AS media_format,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[OFFSET(5)] 
  ELSE NULL
END AS ad_format_detail,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(creative_name, '_')) -2 )] 
  ELSE NULL
END AS ad_format,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(creative_name, '_')) -1 )] 
  ELSE NULL
END AS creative_descr,
'Linkedin' as publisher

FROM 
stats_by_creative_campaign_id AS stat_creative LEFT JOIN  update_campaign_name as campaign_data
ON stat_creative.campaign_id = campaign_data.campaign_id 
{% endmacro %}