-- 1) parsed events vs raw (counts)
SELECT COUNT(*) AS raw_cnt FROM raw.events_raw;
SELECT COUNT(*) AS stg_cnt FROM staging.stg_events_normalized;

-- 2) sessions sanity
SELECT COUNT(*) AS sessions, AVG(events) AS avg_events_per_session FROM analytics.fact_sessions;

-- 3) conversions counted vs attributed
SELECT
  (SELECT COUNT(DISTINCT transaction_id) FROM staging.stg_events_normalized WHERE event_name IN ('purchase','checkout_completed') AND transaction_id IS NOT NULL) AS conversions_raw,
  (SELECT COUNT(*) FROM analytics.fact_attribution) AS conversions_attributed;

-- 4) revenue parity
SELECT
  (SELECT SUM(revenue) FROM (
     SELECT transaction_id, MAX(revenue) AS revenue FROM staging.stg_events_normalized
     WHERE event_name IN ('purchase','checkout_completed') AND transaction_id IS NOT NULL
     GROUP BY transaction_id
   )
  ) AS revenue_raw,
  (SELECT SUM(revenue) FROM analytics.fact_attribution) AS revenue_attributed;
