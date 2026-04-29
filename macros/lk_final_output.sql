{% macro lk_final_output() %}
SELECT stat_creative.*, campaign_data.* EXCEPT(campaign_id) ,
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
    CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 8 THEN SPLIT(campaign_name, '_')[SAFE_OFFSET(7)] 
         ELSE 'Other' END AS audience_name,
    CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 8 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(7)] 
         ELSE 'Other' END AS creative_descr,
    CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 8 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(5)] 
         ELSE 'Other' END AS ad_format_detail,
    CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 8 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(6)] 
         ELSE 'Other' END AS ad_format,
    CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) <=1 THEN 'Other'
        ELSE SPLIT(campaign_name,'_')[SAFE_OFFSET(1)] END AS campaign_descr,
'Linkedin' as publisher

FROM 
stats_by_creative_campaign_id AS stat_creative LEFT JOIN  update_campaign_name as campaign_data
ON stat_creative.campaign_id = campaign_data.campaign_id 
{% endmacro %}