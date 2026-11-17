WITH top_limits AS (
  SELECT limit_id FROM latest_usage GROUP BY limit_id ORDER BY max(pct_used) DESC NULLS LAST LIMIT 6
)
SELECT DATE_TRUNC('DAY', u.scan_ts) AS scan_day,
       u.limit_id, r.level, r.area,
       r.resource || ' per ' || r.scope AS limit_name,
       round(max(u.pct_used) / 100.0, 4) AS pct_used
FROM limit_usage u
JOIN top_limits t ON t.limit_id = u.limit_id
JOIN limits_reference r ON r.limit_id = u.limit_id
WHERE u.scan_ts >= date_sub(current_date(), 90)
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1
