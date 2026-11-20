-- limit-tracking: scan Unity Catalog usage from system.information_schema (read-only).
-- information_schema only returns objects the caller can see. Each row records whether the caller
-- is the metastore owner (full_visibility); when FALSE, counts are lower bounds.
-- Counted catalogs: every catalog except Databricks-managed ones (system, hive_metastore,
-- samples, __databricks_internal*). Foreign and Delta Sharing catalogs are counted.
-- Connections: system-managed (MANAGED_*) types are excluded; whether they count toward the
-- quota is unverified.
-- Runs as one SQL scripting block and only reads metadata until a single, atomic INSERT. Before
-- scanning it fails if the metastore cloud is unrecognized or a scanned limit_id has no
-- limits_reference row; the INSERT itself raise_error()s on a missing row as a backstop.
-- Parameters: :catalog, :schema.
BEGIN
  DECLARE v_prefix STRING DEFAULT
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:schema, '`', '``') || '`';
  -- Every limit_id this scan emits (tests check it matches the usage CTE and the 02 seed).
  DECLARE v_scanned_ids ARRAY<STRING> DEFAULT array(
      'uc_catalogs_per_metastore',
      'uc_tables_per_metastore',
      'uc_volumes_per_metastore',
      'uc_connections_per_metastore',
      'uc_external_locations_per_metastore',
      'uc_credentials_per_metastore',
      'sharing_providers_per_metastore',
      'sharing_recipients_per_metastore',
      'sharing_shares_per_metastore',
      'uc_schemas_per_catalog',
      'uc_tables_per_schema',
      'uc_volumes_per_schema',
      'uc_functions_per_schema',
      'uc_columns_per_table',
      'uc_tag_assignments_per_securable',
      'sharing_tables_per_share',
      'uc_column_tag_assignments_per_table'
  );
  DECLARE v_ts TIMESTAMP DEFAULT current_timestamp();
  DECLARE v_cloud STRING;
  DECLARE v_admin BOOLEAN;
  DECLARE v_missing STRING;

  SET (v_cloud, v_admin) = (
    SELECT cloud, metastore_admin FROM IDENTIFIER(v_prefix || '.scan_context')
  );
  IF v_cloud IS NULL OR v_cloud NOT IN ('aws', 'azure', 'gcp') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
      'limit-tracking: cannot read a recognized cloud from system.information_schema.metastores';
  END IF;

  SET v_missing = (
    SELECT array_join(array_sort(collect_set(id)), ', ')
    FROM (SELECT explode(v_scanned_ids) AS id) ids
    LEFT ANTI JOIN IDENTIFIER(v_prefix || '.limits_reference') r ON r.limit_id = ids.id
  );
  IF v_missing <> '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
      'limit-tracking: no limits_reference row for: ' || v_missing || ' (run 02 first); nothing written';
  END IF;

  -- Single INSERT: Delta commits all rows or none. A NULL or 0 limit for this cloud keeps the row
  -- with NULL pct_used.
  INSERT INTO IDENTIFIER(v_prefix || '.limit_usage')
    (scan_ts, limit_id, cloud, parent_name, workspace_id, usage_count, limit_value, pct_used, scanned_by, full_visibility, source)
  WITH
    uc_catalogs AS (
      SELECT catalog_name FROM system.information_schema.catalogs
      WHERE catalog_name NOT IN ('system', 'hive_metastore', 'samples')
        AND catalog_name NOT LIKE '\\_\\_databricks\\_internal%'
    ),
    tables_per_schema AS (
      SELECT table_catalog || '.' || table_schema AS parent_name, count(*) AS n
      FROM system.information_schema.tables
      WHERE table_catalog IN (SELECT catalog_name FROM uc_catalogs) AND table_schema <> 'information_schema'
      GROUP BY table_catalog, table_schema
    ),
    volumes_per_schema AS (
      SELECT volume_catalog || '.' || volume_schema AS parent_name, count(*) AS n
      FROM system.information_schema.volumes
      WHERE volume_catalog IN (SELECT catalog_name FROM uc_catalogs)
      GROUP BY volume_catalog, volume_schema
    ),
    usage AS (
      -- metastore-wide counts
      SELECT 'uc_catalogs_per_metastore' AS limit_id, CAST(NULL AS STRING) AS parent_name, count(*) AS usage_count FROM uc_catalogs
      UNION ALL SELECT 'uc_tables_per_metastore', NULL, coalesce(sum(n), 0) FROM tables_per_schema
      UNION ALL SELECT 'uc_volumes_per_metastore', NULL, coalesce(sum(n), 0) FROM volumes_per_schema
      UNION ALL SELECT 'uc_connections_per_metastore', NULL, count(*) FROM system.information_schema.connections
                WHERE coalesce(connection_type, '') NOT LIKE 'MANAGED%'
      UNION ALL SELECT 'uc_external_locations_per_metastore', NULL, count(*) FROM system.information_schema.external_locations
      UNION ALL SELECT 'uc_credentials_per_metastore', NULL, count(*) FROM system.information_schema.credentials
      UNION ALL SELECT 'sharing_providers_per_metastore', NULL, count(*) FROM system.information_schema.providers
      UNION ALL SELECT 'sharing_recipients_per_metastore', NULL, count(*) FROM system.information_schema.recipients
      UNION ALL SELECT 'sharing_shares_per_metastore', NULL, count(*) FROM system.information_schema.shares
      -- per-parent counts
      UNION ALL SELECT 'uc_schemas_per_catalog', catalog_name, count(*) FROM system.information_schema.schemata
                WHERE catalog_name IN (SELECT catalog_name FROM uc_catalogs) AND schema_name <> 'information_schema'
                GROUP BY catalog_name
      UNION ALL SELECT 'uc_tables_per_schema', parent_name, n FROM tables_per_schema
      UNION ALL SELECT 'uc_volumes_per_schema', parent_name, n FROM volumes_per_schema
      UNION ALL SELECT 'uc_functions_per_schema', routine_catalog || '.' || routine_schema, count(*) FROM system.information_schema.routines
                WHERE routine_catalog IN (SELECT catalog_name FROM uc_catalogs) GROUP BY routine_catalog, routine_schema
      UNION ALL SELECT 'uc_columns_per_table', table_catalog || '.' || table_schema || '.' || table_name, count(*) FROM system.information_schema.columns
                WHERE table_catalog IN (SELECT catalog_name FROM uc_catalogs) AND table_schema <> 'information_schema'
                GROUP BY table_catalog, table_schema, table_name
      UNION ALL SELECT 'uc_tag_assignments_per_securable', securable, count(*) FROM (
                  SELECT catalog_name AS c, 'catalog ' || catalog_name AS securable FROM system.information_schema.catalog_tags
                  UNION ALL SELECT catalog_name, 'schema ' || catalog_name || '.' || schema_name FROM system.information_schema.schema_tags
                    WHERE schema_name <> 'information_schema'
                  UNION ALL SELECT catalog_name, 'table ' || catalog_name || '.' || schema_name || '.' || table_name FROM system.information_schema.table_tags
                    WHERE schema_name <> 'information_schema'
                  UNION ALL SELECT catalog_name, 'volume ' || catalog_name || '.' || schema_name || '.' || volume_name FROM system.information_schema.volume_tags
                ) t WHERE c IN (SELECT catalog_name FROM uc_catalogs) GROUP BY securable
      UNION ALL SELECT 'sharing_tables_per_share', share_name, count(*) FROM system.information_schema.table_share_usage
                GROUP BY share_name
      UNION ALL SELECT 'uc_column_tag_assignments_per_table', catalog_name || '.' || schema_name || '.' || table_name, count(*) FROM system.information_schema.column_tags
                WHERE catalog_name IN (SELECT catalog_name FROM uc_catalogs) AND schema_name <> 'information_schema'
                GROUP BY catalog_name, schema_name, table_name
    ),
  ranked AS (
    SELECT *, row_number() OVER (PARTITION BY limit_id ORDER BY usage_count DESC) AS rn FROM usage
    -- A per-parent limit with no parents left gets a 0 row, so latest_usage stops showing old rows.
    UNION ALL
    SELECT id, CAST(NULL AS STRING), 0, 1
    FROM (SELECT explode(v_scanned_ids) AS id) ids
    WHERE id NOT IN (SELECT limit_id FROM usage)
  ),
  lim AS (
    SELECT limit_id,
           CASE v_cloud WHEN 'aws' THEN limit_aws WHEN 'azure' THEN limit_azure ELSE limit_gcp END AS limit_value
    FROM IDENTIFIER(v_prefix || '.limits_reference')
  )
  SELECT v_ts,
         CASE WHEN l.limit_id IS NULL
              THEN raise_error('limit-tracking: no limits_reference row for ' || u.limit_id || '; nothing written')
              ELSE u.limit_id END,
         v_cloud, u.parent_name, CAST(NULL AS STRING), u.usage_count, l.limit_value,
         round(100.0 * try_divide(u.usage_count, l.limit_value), 2), current_user(), v_admin, 'information_schema'
  FROM ranked u
  LEFT JOIN lim l ON l.limit_id = u.limit_id
  WHERE u.rn <= 100;
END;
