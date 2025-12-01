/* 1. STAGING: CLEANING & REGEX PARSING */
CREATE OR REPLACE TABLE staging.stg_events_normalized AS
SELECT 
    COALESCE(client_id, clientId) AS user_id,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', timestamp) AS event_ts,
    event_name,
    page_url,
    LOWER(REGEXP_EXTRACT(COALESCE(referrer, ''), r'https?://([^/]+)')) AS referrer_host,
    SAFE_CAST(JSON_VALUE(event_data, '$.value') AS FLOAT64) AS revenue,
    JSON_VALUE(event_data, '$.transaction_id') AS transaction_id,
    LOWER(REGEXP_EXTRACT(page_url, r'[?&]utm_source=([^&]+)')) AS utm_source,
    LOWER(REGEXP_EXTRACT(page_url, r'[?&]utm_medium=([^&]+)')) AS utm_medium,
    LOWER(REGEXP_EXTRACT(page_url, r'[?&]utm_campaign=([^&]+)')) AS utm_campaign,
    -- helpful derived date partition
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', timestamp)) AS event_date
FROM raw.events_raw
WHERE COALESCE(client_id, clientId) IS NOT NULL;


-- 2. ENRICH + derive channel (small CTE so we can reuse)
WITH enriched_events AS (
  SELECT
    *,
    CASE
      WHEN utm_medium IN ('cpc','ppc','paidsearch','paid_search','paid') THEN 'Paid Search'
      WHEN utm_medium IN ('social','social-paid','paidsocial') THEN 'Paid Social'
      WHEN utm_medium IN ('email','klaviyo') THEN 'Email'
      WHEN utm_medium IN ('display','banner') THEN 'Display'
      WHEN utm_medium IN ('affiliate') THEN 'Affiliate'
      WHEN referrer_host LIKE '%google%' OR referrer_host LIKE '%bing%' THEN 'Organic Search'
      WHEN referrer_host LIKE '%facebook%' OR referrer_host LIKE '%t.co%' OR referrer_host LIKE '%instagram%' THEN 'Organic Social'
      WHEN referrer_host IS NULL OR referrer_host = '' THEN 'Direct'
      ELSE 'Referral'
    END AS derived_channel
  FROM staging.stg_events_normalized
)

-- 3. SESSIONIZATION (fix: treat first event as new session)
, session_flags AS (
  SELECT
    *,
    LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_ts
  FROM enriched_events
),

session_flags2 AS (
  SELECT
    *,
    CASE
      WHEN prev_ts IS NULL THEN 1
      WHEN TIMESTAMP_DIFF(event_ts, prev_ts, MINUTE) >= 30 THEN 1
      ELSE 0
    END AS is_new_session_flag
  FROM session_flags
),

session_grouping AS (
  SELECT
    *,
    SUM(is_new_session_flag) OVER (PARTITION BY user_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_seq,
    CONCAT(user_id, '-S', CAST(SUM(is_new_session_flag) OVER (PARTITION BY user_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING)) AS session_id
  FROM session_flags2
)

-- 4. AGGREGATE TO SESSION LEVEL (FIRST & LAST channel)
CREATE OR REPLACE TABLE analytics.fact_sessions AS
SELECT
  session_id,
  user_id,
  MIN(event_ts) AS session_start_ts,
  MAX(event_ts) AS session_end_ts,
  COUNT(1) AS events,
  -- first-touch channel & last-touch channel for the session
  ARRAY_AGG(derived_channel ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS session_first_channel,
  ARRAY_AGG(derived_channel ORDER BY event_ts DESC LIMIT 1)[OFFSET(0)] AS session_last_channel,
  ARRAY_AGG(utm_campaign ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS session_campaign
FROM session_grouping
GROUP BY 1,2;


-- 5. OPTIONAL: attach session_id back to every event (needed for funnels & validation)
CREATE OR REPLACE TABLE analytics.session_events AS
SELECT
  session_id,
  user_id,
  event_ts,
  event_name,
  page_url,
  derived_channel,
  utm_source,
  utm_medium,
  utm_campaign,
  revenue,
  transaction_id
FROM session_grouping;


-- 6. ATTRIBUTION: dedupe conversions by transaction_id and attribute at session level (7-day window)
CREATE OR REPLACE TABLE analytics.fact_attribution AS
WITH conversions AS (
  -- de-dupe: pick the latest event_ts per transaction_id
  SELECT
    transaction_id,
    user_id,
    MAX(event_ts) AS conversion_ts,
    MAX(revenue) AS revenue
  FROM staging.stg_events_normalized
  WHERE event_name IN ('purchase','checkout_completed') AND transaction_id IS NOT NULL
  GROUP BY transaction_id, user_id
)

SELECT
  c.transaction_id,
  c.user_id,
  c.revenue,
  c.conversion_ts,

  -- First Click: earliest session_start in 7-day window (uses session_first_channel)
  COALESCE((
    SELECT s.session_first_channel
    FROM analytics.fact_sessions s
    WHERE s.user_id = c.user_id
      AND s.session_start_ts BETWEEN TIMESTAMP_SUB(c.conversion_ts, INTERVAL 7 DAY) AND c.conversion_ts
    ORDER BY s.session_start_ts ASC
    LIMIT 1
  ), 'Direct') AS first_click_channel,

  -- Last Click: latest session_start in 7-day window (uses session_last_channel)
  COALESCE((
    SELECT s.session_last_channel
    FROM analytics.fact_sessions s
    WHERE s.user_id = c.user_id
      AND s.session_start_ts BETWEEN TIMESTAMP_SUB(c.conversion_ts, INTERVAL 7 DAY) AND c.conversion_ts
    ORDER BY s.session_start_ts DESC
    LIMIT 1
  ), 'Direct') AS last_click_channel

FROM conversions c;
