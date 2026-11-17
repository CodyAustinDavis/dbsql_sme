-- limit-tracking: write usage rows collected by scripts/scan_rest.py (REST list APIs).
-- Parameters: :catalog, :schema, :rows (JSON array of {limit_id, parent_name, usage_count,
-- scope_kind, complete}), :workspace_admin ('true' | 'false'), :workspace_id. Runs as one SQL scripting block; fails before writing if the
-- cloud is unrecognized or a limit_id has no limits_reference row, then writes in one atomic INSERT.
BEGIN
  DECLARE v_prefix STRING DEFAULT
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:schema, '`', '``') || '`';
  DECLARE v_rows ARRAY<STRUCT<limit_id: STRING, parent_name: STRING, usage_count: BIGINT, scope_kind: STRING, complete: BOOLEAN>>
    DEFAULT from_json(:rows, 'ARRAY<STRUCT<limit_id: STRING, parent_name: STRING, usage_count: BIGINT, scope_kind: STRING, complete: BOOLEAN>>');
  DECLARE v_ws_admin BOOLEAN DEFAULT coalesce(try_cast(:workspace_admin AS BOOLEAN), false);
  DECLARE v_ws_id STRING DEFAULT :workspace_id;
  DECLARE v_ts TIMESTAMP DEFAULT current_timestamp();
  DECLARE v_cloud STRING;
  DECLARE v_ms_admin BOOLEAN;
  DECLARE v_missing STRING;

  IF v_rows IS NULL OR size(v_rows) = 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'limit-tracking: no REST usage rows to write';
  END IF;
  IF exists(v_rows, r -> r.limit_id IS NULL OR r.usage_count IS NULL) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'limit-tracking: REST rows with NULL limit_id or usage_count; nothing written';
  END IF;

  SET (v_cloud, v_ms_admin) = (
    SELECT cloud, metastore_admin FROM IDENTIFIER(v_prefix || '.scan_context')
  );
  IF v_cloud IS NULL OR v_cloud NOT IN ('aws', 'azure', 'gcp') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
      'limit-tracking: cannot read a recognized cloud from system.information_schema.metastores';
  END IF;

  SET v_missing = (
    SELECT array_join(array_sort(collect_set(u.limit_id)), ', ')
    FROM (SELECT inline(v_rows)) u
    LEFT ANTI JOIN IDENTIFIER(v_prefix || '.limits_reference') r ON r.limit_id = u.limit_id
  );
  IF v_missing <> '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
      'limit-tracking: no limits_reference row for: ' || v_missing || ' (run 02 first); nothing written';
  END IF;

  INSERT INTO IDENTIFIER(v_prefix || '.limit_usage')
    (scan_ts, limit_id, cloud, parent_name, workspace_id, usage_count, limit_value, pct_used, scanned_by, full_visibility, source)
  WITH lim AS (
    SELECT limit_id,
           CASE v_cloud WHEN 'aws' THEN limit_aws WHEN 'azure' THEN limit_azure ELSE limit_gcp END AS limit_value
    FROM IDENTIFIER(v_prefix || '.limits_reference')
  )
  SELECT v_ts, u.limit_id, v_cloud, u.parent_name,
         CASE WHEN u.scope_kind = 'workspace' THEN v_ws_id END,
         u.usage_count, l.limit_value,
         round(100.0 * try_divide(u.usage_count, l.limit_value), 2), current_user(),
         coalesce(u.complete, true) AND CASE u.scope_kind WHEN 'workspace' THEN v_ws_admin ELSE v_ms_admin END,
         'rest_api'
  FROM (SELECT inline(v_rows)) u
  JOIN lim l ON l.limit_id = u.limit_id;
END;
