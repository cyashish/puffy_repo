## Appendix: Reproducing Issue A
```sql
-- Count events by identifier field and date
SELECT 
  DATE(timestamp) AS event_date,
  COUNT(*) FILTER (WHERE client_id IS NOT NULL) AS client_id_count,
  COUNT(*) FILTER (WHERE clientId IS NOT NULL) AS clientId_count,
  COUNT(*) AS total_events
FROM raw.events_*
WHERE timestamp BETWEEN '2025-02-23' AND '2025-03-08'
GROUP BY 1
ORDER BY 1;
```

Run this to see the Feb 27 cutover.