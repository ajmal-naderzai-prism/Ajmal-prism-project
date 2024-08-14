WITH unpivoted AS (
  SELECT
    Date,
    platform,
    metric_type,
    metric_value
  FROM
    {{ source('prism_acquire', 'adplatform_data') }},
    UNNEST([
      STRUCT('criteo' AS platform, 'cost' AS metric_type, criteo_cost AS metric_value),
      STRUCT('criteo' AS platform, 'clicks' AS metric_type, criteo_clicks AS metric_value),
      STRUCT('criteo' AS platform, 'impressions' AS metric_type, criteo_impressions AS metric_value),
      STRUCT('google' AS platform, 'cost' AS metric_type, google_cost AS metric_value),
      STRUCT('google' AS platform, 'clicks' AS metric_type, google_clicks AS metric_value),
      STRUCT('google' AS platform, 'impressions' AS metric_type, google_impressions AS metric_value),
      STRUCT('meta' AS platform, 'cost' AS metric_type, meta_cost AS metric_value),
      STRUCT('meta' AS platform, 'clicks' AS metric_type, meta_clicks AS metric_value),
      STRUCT('meta' AS platform, 'impressions' AS metric_type, meta_impressions AS metric_value),
      STRUCT('rtbhouse' AS platform, 'cost' AS metric_type, rtbhouse_cost AS metric_value),
      STRUCT('rtbhouse' AS platform, 'clicks' AS metric_type, rtbhouse_clicks AS metric_value),
      STRUCT('rtbhouse' AS platform, 'impressions' AS metric_type, rtbhouse_impressions AS metric_value),
      STRUCT('tiktok' AS platform, 'cost' AS metric_type, tiktok_cost AS metric_value),
      STRUCT('tiktok' AS platform, 'clicks' AS metric_type, tiktok_clicks AS metric_value),
      STRUCT('tiktok' AS platform, 'impressions' AS metric_type, tiktok_impressions AS metric_value)
    ]) AS unpivoted_metrics
)

SELECT
  Date,
  Platform,
  SUM(CASE WHEN Metric_type = 'cost' THEN Metric_value ELSE NULL END) AS cost,
  CAST(SUM(CASE WHEN Metric_type = 'clicks' THEN Metric_value ELSE NULL END) AS INT64) AS clicks,
  CAST(SUM(CASE WHEN Metric_type = 'impressions' THEN Metric_value ELSE NULL END) AS INT64) AS impressions
FROM
  unpivoted
GROUP BY
  Date,
  Platform
ORDER BY
  Date, Platform
