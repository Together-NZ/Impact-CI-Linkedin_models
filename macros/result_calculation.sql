{% macro result_calculation() %}
  optimization_goal_fetching AS (
    SELECT *, 
    CASE 
    WHEN optimization_goal IN ('max_impression', 'max_impressions','target_cost_per_impression','cap_cost_and_maximise_impressions') THEN impressions
    WHEN optimization_goal IN ('max_click','max_clicks','cap_cost_and_maximise_clicks','target_cost_per_click') THEN clicks
    WHEN optimization_goal IN ('max_video_view', 'max_video_views') THEN video_views
    ELSE 0 END AS result
    FROM joint_daily_stats_platform_info

)
  SELECT * FROM optimization_goal_fetching 
{% endmacro %}