{% macro result_calculation() %}
  optimization_goal_fetching AS (
    SELECT *, 
    CASE 
    WHEN optimization_goal IN ('max_impression', 'max_impressions','target_cost_per_impression','cap_cost_and_maximise_impressions') THEN impressions
    WHEN optimization_goal IN ('max_click','max_clicks','cap_cost_and_maximise_clicks','target_cost_per_click') THEN clicks
    WHEN optimization_goal IN ('max_video_view', 'max_video_views') THEN video_views
    ELSE 0 END AS result,
    CASE 
        WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >= 3 AND SPLIT (campaign_name,'_')[OFFSET(2)] LIKE '%SOCIAL%'
        AND (
            lower(creative_name) LIKE '%vid%'
            OR lower(campaign_name) LIKE '%vid%'
        ) THEN 'Social Video'
        WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >= 3 AND SPLIT (campaign_name,'_')[OFFSET(2)] LIKE '%SOCIAL%'
        AND (
            lower(creative_name) NOT LIKE '%vid%'
            AND lower(campaign_name) NOT LIKE '%vid%'
        )
        THEN 'Social Display'
        ELSE 'Other'
  END AS media_format,
    CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(7)] 
         ELSE 'Other' END AS creative_descr,
    CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(5)] 
         ELSE 'Other' END AS ad_format_detail,
    CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(6)] 
         ELSE 'Other' END AS ad_format,
    CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) <=1 THEN 'Other'
        ELSE SPLIT(campaign_name,'_')[SAFE_OFFSET(1)] END AS campaign_descr,
'Linkedin' as publisher,
'Linkedin' as platform
    FROM joint_daily_stats_platform_info

)
  SELECT * FROM optimization_goal_fetching 
{% endmacro %}