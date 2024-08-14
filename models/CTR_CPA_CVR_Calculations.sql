WITH sessions_data AS (
  -- Get the unique sessions for CPC (Cost Per Click)
  SELECT
    date,
    traffic_source AS platform,
    COUNT(DISTINCT session_id) AS session_clicks
  FROM {{ source('prism_acquire', 'sessions') }}
  WHERE traffic_medium = 'cpc'
  GROUP BY date, platform
  ORDER BY date, platform
),
transactions_data AS (
  -- Get the unique transactions and total_revenue
  SELECT
    s.date,
    s.traffic_source AS platform,
    COUNT(DISTINCT t.transaction_id) AS transactions,
    ROUND(SUM(t.transaction_total),2) AS total_revenue
  FROM {{ source('prism_acquire', 'transactions') }} t
  INNER JOIN {{ source('prism_acquire', 'sessions') }} s
  ON t.session_id = s.session_id
  GROUP BY s.date, s.traffic_source
  ORDER BY s.date, s.traffic_source
)
--- Main query, calculations of Click-Through Rate(CTR), Cost Per Acquisition(CPA), Conversion Rate(CVR) and Cost Per Impression(CPM).
SELECT
  apd.date,
  apd.platform,
  sd.session_clicks AS daily_sessions,
  td.transactions AS daily_transactions,
  apd.clicks AS daily_clicks,
  apd.impressions AS daily_impressions,
  apd.cost AS daily_cost,
  ROUND((apd.clicks / apd.impressions) * 100, 2) AS CTR, -- Click-through rate
  ROUND((apd.cost / td.transactions), 2) AS CPA, -- Cost per acquision
  ROUND((td.transactions / sd.session_clicks) * 100, 2) AS CVR, -- Conversion rate
  ROUND((apd.cost / (apd.impressions / 1000)), 2) AS CPM  -- Cost per 1000 impressions
FROM {{ ref('unpivoted_metrics') }} apd
INNER JOIN sessions_data sd ON apd.date = sd.date AND apd.platform = sd.platform
LEFT JOIN transactions_data td ON apd.date = td.date AND apd.platform = td.platform
ORDER BY apd.date, apd.platform

{{ config(materialized='table') }}