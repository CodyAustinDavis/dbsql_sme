SELECT c.detected_on, c.limit_id,
       coalesce(r.resource || ' per ' || r.scope, c.limit_id) AS limit_name,
       c.change_type, c.cloud, c.old_value, c.new_value, c.source_url
FROM limit_changes c
LEFT JOIN limits_reference r ON r.limit_id = c.limit_id
WHERE c.change_type <> 'added' OR c.detected_on > (SELECT min(detected_on) FROM limit_changes)
ORDER BY c.logged_at DESC
