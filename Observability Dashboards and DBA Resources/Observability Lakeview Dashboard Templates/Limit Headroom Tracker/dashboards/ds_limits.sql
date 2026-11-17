WITH ctx AS (SELECT cloud FROM scan_context),
usage AS (
  SELECT limit_id,
         max_by(coalesce(parent_name, 'workspace ' || workspace_id), coalesce(pct_used, -1)) AS top_parent,
         max_by(usage_count, coalesce(pct_used, -1)) AS usage_count,
         max(pct_used) AS pct_used,
         bool_and(full_visibility) AS full_visibility,
         max(scan_ts) AS scan_ts
  FROM latest_usage
  GROUP BY limit_id
)
SELECT r.limit_id, r.level, r.area, r.resource, r.scope,
       r.resource || ' per ' || r.scope AS limit_name,
       CASE ctx.cloud WHEN 'aws' THEN r.limit_aws WHEN 'azure' THEN r.limit_azure ELSE r.limit_gcp END AS limit_value,
       u.usage_count, u.top_parent,
       round(u.pct_used / 100.0, 4) AS pct_used,
       CASE WHEN u.limit_id IS NULL THEN 'Not measured'
            WHEN u.pct_used IS NULL THEN 'Unknown (no limit for this cloud)'
            WHEN u.pct_used >= 90 THEN 'Critical (90%+)'
            WHEN u.pct_used >= 70 THEN 'Warning (70-90%)'
            ELSE 'OK (under 70%)' END AS status,
       CASE WHEN u.limit_id IS NOT NULL THEN 1 ELSE 0 END AS is_measured,
       CASE WHEN u.pct_used >= 70 THEN 1 ELSE 0 END AS is_warning,
       CASE WHEN u.pct_used >= 90 THEN 1 ELSE 0 END AS is_critical,
       CASE WHEN r.adjustable THEN 'Yes: request via account team' ELSE 'No (fixed)' END AS raisable,
       CASE WHEN u.full_visibility THEN 'Complete' WHEN u.limit_id IS NULL THEN '' ELSE 'Lower bound' END AS count_quality,
       r.measure_method, r.release_status, r.notes, u.scan_ts,
       CASE ctx.cloud WHEN 'aws' THEN r.source_url_aws WHEN 'azure' THEN r.source_url_azure ELSE r.source_url_gcp END AS source_url
FROM limits_reference r CROSS JOIN ctx
LEFT JOIN usage u ON u.limit_id = r.limit_id
ORDER BY u.pct_used DESC NULLS LAST, r.area, r.limit_id
