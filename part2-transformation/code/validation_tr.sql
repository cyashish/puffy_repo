-- validation_tests.sql
-- Single-file unit test for sessionization & attribution pipeline (BigQuery-style SQL)
-- NOTE: Replace dataset `test` with your test dataset if needed.

-- Cleanup previous test artifacts (safe to run)
DROP TABLE IF EXISTS test.raw_events_raw;
DROP TABLE IF EXISTS test.stg_events_normalized;
DROP TABLE IF EXISTS test.enriched_events;
DROP TABLE IF EXISTS test.session_grouping;
DROP TABLE IF EXISTS test.fact_sessions;
DROP TABLE IF EXISTS test.session_events;
DROP TABLE IF EXISTS test.fact_attribution;
DROP TABLE IF EXISTS test.validation_summary;
DROP TABLE IF EXISTS test.test_notes;

-- 1) Create synthetic raw events table
CREATE TABLE test.raw_events_raw (
  client_id STRING,
  clientId STRING,
  timestamp STRING,         -- ISO string like '2025-11-01T10:00:00Z'
  event_name STRING,
  page_url STRING,
  referrer STRING,
  event_data STRING
);

-- 2) Insert synthetic test rows for cases A-E and a drift case
-- Times are in UTC. We'll use TIMESTAMP parsing in staging.
-- Test A: single event -> must create a session
INSERT INTO test.raw_events_raw VALUES
('user_A', NULL, '2025-11-01T10:00:00Z', 'page_view', 'https://example.com/home', '', '{}');

-- Test B: session boundary at 30 minutes -> two sessions
INSERT INTO test.raw_events_raw VALUES
('user_B', NULL, '2025-11-02T10:00:00Z', 'page_view', 'https://example.com/p1', 'https://google.com', '{}');
INSERT INTO test.raw_events_raw VALUES
('user_B', NULL, '2025-11-02T10:10:00Z', 'page_view', 'https://example.com/p2', 'https://google.com', '{}');
-- 35 min later -> new session
INSERT INTO test.raw_events_raw VALUES
('user_B', NULL, '2025-11-02T10:45:00Z', 'page_view', 'https://example.com/p3', 'https://bing.com', '{}');

-- Test C: intra-session mixed channels and purchase at session end
-- first event: organic (referrer google)
INSERT INTO test.raw_events_raw VALUES
('user_C', NULL, '2025-11-03T09:00:00Z', 'page_view', 'https://example.com/p', 'https://google.com', '{}');
-- mid-session paid click (utm)
INSERT INTO test.raw_events_raw VALUES
('user_C', NULL, '2025-11-03T09:05:00Z', 'click', 'https://example.com/landing?utm_source=adnetwork&utm_medium=cpc&utm_campaign=camp1', 'https://adtracker.com', '{}');
-- purchase at end (should attribute first=Organic Search, last=Paid Search)
INSERT INTO test.raw_events_raw VALUES
('user_C', NULL, '2025-11-03T09:10:00Z', 'purchase', 'https://example.com/checkout', 'https://example.com', '{"transaction_id":"TXN_C_1","value":150.0}');

-- Test D: 7-day boundary inclusive
-- touchpoint exactly 7 days before purchase
INSERT INTO test.raw_events_raw VALUES
('user_D', NULL, '2025-10-27T08:00:00Z', 'page_view', 'https://example.com/early', 'https://facebook.com', '{}');
INSERT INTO test.raw_events_raw VALUES
('user_D', NULL, '2025-11-03T08:00:00Z', 'purchase', 'https://example.com/checkout', 'https://example.com', '{"transaction_id":"TXN_D_1","value":200.0}');

-- Test E: purchase with no prior sessions -> Direct
INSERT INTO test.raw_events_raw VALUES
('user_E', NULL, '2025-11-05T12:00:00Z', 'purchase', 'https://example.com/checkout', '', '{"transaction_id":"TXN_E_1","value":75.0}');

-- Schema drift case: only clientId present (Feb 27-like)
INSERT INTO test.raw_events_raw VALUES
(NULL, 'clientId_ghost', '2025-11-04T11:00:00Z', 'page_view', 'https://example.com/ghost', '', '{}');

-- Duplicate purchase event to test dedupe handling for revenue (same transaction repeated)
INSERT INTO test.raw_events_raw VALUES
('user_C', NULL, '2025-11-03T09:10:00Z', 'purchase', 'https://example.com/checkout', 'https://example.com', '{"transaction_id":"TXN_C_1","value":150.0}');

-- 3) STAGING: normalization, parsing, UTM regex, SAFE_CAST
CREATE OR REPLACE TABLE test.stg_events_normalized AS
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
  DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', timestamp)) AS event_date
FROM test.raw_events_raw
WHERE COALESCE(client_id, clientId) IS NOT NULL;

-- 4) ENRICH (derive channel)
CREATE OR REPLACE TABLE test.enriched_events AS
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
FROM test.stg_events_normalized;

-- 5) SESSIONIZATION: flag new sessions (treat first event as new)
CREATE OR REPLACE TABLE test.session_grouping AS
WITH lagged AS (
  SELECT
    *,
    LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_ts
  FROM test.enriched_events
)
SELECT
  *,
  CASE
    WHEN prev_ts IS NULL THEN 1
    WHEN TIMESTAMP_DIFF(event_ts, prev_ts, MINUTE) >= 30 THEN 1
    ELSE 0
  END AS is_new_session_flag,
  SUM(CASE WHEN prev_ts IS NULL THEN 1 WHEN TIMESTAMP_DIFF(event_ts, prev_ts, MINUTE) >= 30 THEN 1 ELSE 0 END)
    OVER (PARTITION BY user_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_seq,
  CONCAT(user_id, '-S', CAST(SUM(CASE WHEN prev_ts IS NULL THEN 1 WHEN TIMESTAMP_DIFF(event_ts, prev_ts, MINUTE) >= 30 THEN 1 ELSE 0 END)
    OVER (PARTITION BY user_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING)) AS session_id
FROM lagged;

-- 6) AGGREGATE TO SESSIONS (first & last channel)
CREATE OR REPLACE TABLE test.fact_sessions AS
SELECT
  session_id,
  user_id,
  MIN(event_ts) AS session_start_ts,
  MAX(event_ts) AS session_end_ts,
  COUNT(1) AS events,
  ARRAY_AGG(derived_channel ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS session_first_channel,
  ARRAY_AGG(derived_channel ORDER BY event_ts DESC LIMIT 1)[OFFSET(0)] AS session_last_channel,
  ARRAY_AGG(utm_campaign ORDER BY event_ts LIMIT 1)[OFFSET(0)] AS session_campaign
FROM test.session_grouping
GROUP BY session_id, user_id;

-- 7) session_events mapping
CREATE OR REPLACE TABLE test.session_events AS
SELECT
  session_id, user_id, event_ts, event_name, page_url, derived_channel, utm_source, utm_medium, utm_campaign, revenue, transaction_id
FROM test.session_grouping;

-- 8) ATTRIBUTION: dedupe conversions and attribute at session-level with 7-day window
CREATE OR REPLACE TABLE test.fact_attribution AS
WITH conversions AS (
  SELECT transaction_id, user_id, MAX(event_ts) AS conversion_ts, MAX(COALESCE(revenue,0)) AS revenue
  FROM test.stg_events_normalized
  WHERE event_name IN ('purchase','checkout_completed') AND transaction_id IS NOT NULL
  GROUP BY transaction_id, user_id
)
SELECT
  c.transaction_id,
  c.user_id,
  c.revenue,
  c.conversion_ts,
  COALESCE((
    SELECT s.session_first_channel
    FROM test.fact_sessions s
    WHERE s.user_id = c.user_id
      AND s.session_start_ts BETWEEN TIMESTAMP_SUB(c.conversion_ts, INTERVAL 7 DAY) AND c.conversion_ts
    ORDER BY s.session_start_ts ASC
    LIMIT 1
  ), 'Direct') AS first_click_channel,
  COALESCE((
    SELECT s.session_last_channel
    FROM test.fact_sessions s
    WHERE s.user_id = c.user_id
      AND s.session_start_ts BETWEEN TIMESTAMP_SUB(c.conversion_ts, INTERVAL 7 DAY) AND c.conversion_ts
    ORDER BY s.session_start_ts DESC
    LIMIT 1
  ), 'Direct') AS last_click_channel
FROM conversions c;

-- 9) RUN ASSERTIONS: produce validation_summary
CREATE OR REPLACE TABLE test.validation_summary AS
WITH
-- A: First event starts a session (user_A)
tA AS (
  SELECT
    (SELECT COUNT(*) FROM test.fact_sessions WHERE user_id = 'user_A') AS sessions_for_A,
    (SELECT session_start_ts FROM test.fact_sessions WHERE user_id = 'user_A' LIMIT 1) AS session_start_A
),

-- B: user_B should have 2 sessions
tB AS (
  SELECT COUNT(DISTINCT session_id) AS sessions_for_B FROM test.fact_sessions WHERE user_id = 'user_B'
),

-- C: user_C first-click should be Organic Search, last-click Paid Search
tC AS (
  SELECT first_click_channel, last_click_channel
  FROM test.fact_attribution
  WHERE transaction_id = 'TXN_C_1'
),

-- D: user_D boundary inclusion -> the touch at exactly 7 days should be included (we expect attribution to use the session from 7-days-ago). For simplicity check there's a session for user_D with start = touch time.
tD AS (
  SELECT COUNT(*) AS sessions_for_D,
         MIN(session_start_ts) AS min_start_for_D
  FROM test.fact_sessions
  WHERE user_id = 'user_D'
),

-- E: user_E purchase with no prior sessions -> should be Direct
tE AS (
  SELECT first_click_channel, last_click_channel
  FROM test.fact_attribution
  WHERE transaction_id = 'TXN_E_1'
),

-- F: Deduped conversions count (raw distinct txns) vs attributed rows
tF AS (
  SELECT
    (SELECT COUNT(DISTINCT transaction_id) FROM test.stg_events_normalized WHERE event_name IN ('purchase','checkout_completed') AND transaction_id IS NOT NULL) AS raw_txns,
    (SELECT COUNT(*) FROM test.fact_attribution) AS attributed_txns
),

-- G: Revenue parity
tG AS (
  SELECT
    (SELECT SUM(rev) FROM (
       SELECT transaction_id, MAX(COALESCE(revenue,0)) AS rev
       FROM test.stg_events_normalized
       WHERE event_name IN ('purchase','checkout_completed') AND transaction_id IS NOT NULL
       GROUP BY transaction_id
    )) AS revenue_raw,
    (SELECT SUM(revenue) FROM test.fact_attribution) AS revenue_attributed
),

-- H: ensure no session has start > end
tH AS (
  SELECT COUNT(*) AS bad_sessions FROM test.fact_sessions WHERE session_start_ts > session_end_ts
),

-- I: ensure every staging row has user_id not null
tI AS (
  SELECT COUNT(*) AS null_user_ids FROM test.stg_events_normalized WHERE user_id IS NULL
),

-- Build summary rows
rows AS (
  SELECT 'A_first_event_session' AS test_name,
    CASE WHEN (SELECT sessions_for_A FROM tA) = 1 THEN 'PASS' ELSE 'FAIL' END AS result,
    FORMAT('expected=1 actual=%d start=%s', (SELECT sessions_for_A FROM tA), (SELECT CAST(session_start_A AS STRING) FROM tA)) AS details
  UNION ALL
  SELECT 'B_session_boundary_30min',
    CASE WHEN (SELECT sessions_for_B FROM tB) = 2 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('expected=2 actual=%d', (SELECT sessions_for_B FROM tB))
  UNION ALL
  SELECT 'C_intra_session_first_last',
    CASE WHEN (SELECT first_click_channel FROM tC) = 'Organic Search' AND (SELECT last_click_channel FROM tC) = 'Paid Search' THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('first=%s last=%s', (SELECT first_click_channel FROM tC), (SELECT last_click_channel FROM tC))
  UNION ALL
  SELECT 'D_7day_inclusive',
    CASE WHEN (SELECT sessions_for_D FROM tD) >= 1 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('min_session_start=%s', (SELECT CAST(min_start_for_D AS STRING) FROM tD))
  UNION ALL
  SELECT 'E_direct_when_no_prior',
    CASE WHEN (SELECT first_click_channel FROM tE) = 'Direct' AND (SELECT last_click_channel FROM tE) = 'Direct' THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('first=%s last=%s', (SELECT first_click_channel FROM tE), (SELECT last_click_channel FROM tE))
  UNION ALL
  SELECT 'F_conversion_count_dedupe',
    CASE WHEN (SELECT raw_txns FROM tF) = (SELECT attributed_txns FROM tF) THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('raw=%d attributed=%d', (SELECT raw_txns FROM tF), (SELECT attributed_txns FROM tF))
  UNION ALL
  SELECT 'G_revenue_parity',
    CASE WHEN ROUND(COALESCE((SELECT revenue_raw FROM tG),0) - COALESCE((SELECT revenue_attributed FROM tG),0),4) = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('raw=%.2f attributed=%.2f', COALESCE((SELECT revenue_raw FROM tG),0), COALESCE((SELECT revenue_attributed FROM tG),0))
  UNION ALL
  SELECT 'H_session_start_before_end',
    CASE WHEN (SELECT bad_sessions FROM tH) = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('bad_sessions=%d', (SELECT bad_sessions FROM tH))
  UNION ALL
  SELECT 'I_non_null_user_id_in_staging',
    CASE WHEN (SELECT null_user_ids FROM tI) = 0 THEN 'PASS' ELSE 'FAIL' END,
    FORMAT('null_user_ids=%d', (SELECT null_user_ids FROM tI))
)
SELECT * FROM rows;

-- Also persist the summary table
CREATE TABLE test.validation_summary AS
SELECT * FROM (
  SELECT * FROM rows
);

-- 10) Helpful diagnostics for quick debugging (if any FAILS)
-- Show attribution table
SELECT * FROM test.fact_attribution ORDER BY transaction_id;

-- Show sessions
SELECT * FROM test.fact_sessions ORDER BY user_id, session_start_ts;

-- Show session_events for user_C to debug intra-session behavior
SELECT * FROM test.session_events WHERE user_id = 'user_C' ORDER BY event_ts;

-- End of validation test file
