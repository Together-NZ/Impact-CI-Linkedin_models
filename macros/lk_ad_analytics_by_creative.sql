{% macro lk_ad_analytics_by_creative(source_name, table_name) %}
daily_stats_raw AS (SELECT
  SAFE_CAST(JSON_VALUE(data, '$.video_completions') AS INT64) AS video_completion,
  SAFE_CAST(JSON_VALUE(data, '$.comments') AS INT64) AS comments,
  JSON_VALUE(data, '$.cost_in_usd') AS cost_in_usd,
  SAFE_CAST(JSON_VALUE(data, '$.cost_in_local_currency') AS FLOAT64) AS media_cost,
  SAFE_CAST(JSON_VALUE(data, '$.impressions') AS INT64) AS impressions,
  SAFE_CAST(JSON_VALUE(data, '$.clicks') AS INT64) AS clicks,
  SAFE_CAST(JSON_VALUE(data, '$.total_engagements') AS INT64) AS totalEngagements,
  SAFE_CAST(JSON_VALUE(data, '$.full_screen_plays') AS INT64) AS fullScreenPlays,
  SAFE_CAST(JSON_VALUE(data, '$.video_starts') AS INT64) AS video_starts,
  SAFE_CAST(JSON_VALUE(data, '$.video_views') AS INT64) AS video_views,
  SAFE_CAST(JSON_VALUE(data, '$.video_first_quartile_completions') AS INT64) AS video_25_completion,
  SAFE_CAST(JSON_VALUE(data, '$.video_midpoint_completions') AS INT64) AS video_50_completion,
  SAFE_CAST(JSON_VALUE(data, '$.video_third_quartile_completions') AS INT64) AS video_75_completion,
  SAFE_CAST(JSON_VALUE(data, '$.likes') AS INT64) AS likes,
  SAFE_CAST(JSON_VALUE(data, '$.follows') AS INT64) AS follows,
  SAFE_CAST(JSON_VALUE(data, '$.comment_likes') AS INT64) AS commentLikes,
  SAFE_CAST(JSON_VALUE(data, '$.landing_page_clicks') AS INT64) AS landingPageClicks,
  DATE(
  TIMESTAMP(JSON_VALUE(data, '$.start_at')),
  "Pacific/Auckland"
  ) AS date,
  _sdc_extracted_at AS updated_at,

  JSON_VALUE(data, '$.creative_id') AS creative_id,
  ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.creative_id'),  JSON_VALUE(data, '$.start_at') ORDER BY _sdc_extracted_at DESC) AS row_num
FROM
  {{ source(source_name, table_name) }}),
  daily_stats AS (
    SELECT * EXCEPT(row_num)  FROM daily_stats_raw WHERE row_num = 1
  )

{% endmacro %}