SELECT u.limit_id, r.level, r.area, r.resource || ' per ' || r.scope AS limit_name,
       coalesce(u.parent_name,
                CASE WHEN u.workspace_id IS NOT NULL THEN 'workspace ' || u.workspace_id
                     ELSE '(whole ' || r.scope || ')' END) AS parent_name,
       u.workspace_id,
       u.usage_count, u.limit_value, round(u.pct_used / 100.0, 4) AS pct_used,
       CASE WHEN u.full_visibility THEN 'Complete' ELSE 'Lower bound' END AS count_quality,
       u.source, u.scan_ts
FROM latest_usage u
JOIN limits_reference r ON r.limit_id = u.limit_id
ORDER BY u.pct_used DESC NULLS LAST
