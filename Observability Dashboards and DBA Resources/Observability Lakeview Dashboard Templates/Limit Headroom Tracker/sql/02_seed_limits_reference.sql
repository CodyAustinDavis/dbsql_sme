-- limit-tracking: seed / refresh the limits reference from public docs.
-- Re-runnable, one SQL scripting block. Added / removed limits and value or adjustability
-- changes are logged to limit_changes, then merged. Logging skips a diff that is already the
-- latest logged change of its kind, so a retry after a failed MERGE doesn't double-log.
-- Parameters: :catalog, :schema.
-- Sources (verified 2026-11-10):
--   AWS   https://docs.databricks.com/aws/en/resources/limits
--   Azure https://learn.microsoft.com/en-us/azure/databricks/resources/limits
--   GCP   https://docs.databricks.com/gcp/en/resources/limits
--   Model Serving: .../machine-learning/model-serving/model-serving-limits (per cloud)
BEGIN
  DECLARE v_prefix STRING DEFAULT
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:schema, '`', '``') || '`';

  -- Session-scoped staging. Seed runs must not overlap (the change-log diff is not concurrency-
  -- safe); the bundle job runs with max_concurrent_runs = 1.
  CREATE OR REPLACE TEMPORARY VIEW lt_seed AS
  SELECT limit_id, level, area, resource, scope, limit_aws, limit_azure, limit_gcp, adjustable,
    release_status, measure_method, notes,
    -- page_path is the docs page (same path on every cloud), applied to each cloud's base URL.
    'https://docs.databricks.com/aws/en/' || page_path               AS source_url_aws,
    'https://learn.microsoft.com/en-us/azure/databricks/' || page_path AS source_url_azure,
    'https://docs.databricks.com/gcp/en/' || page_path               AS source_url_gcp,
    DATE'2026-11-10' AS last_verified
  FROM VALUES
    -- limit_id, level, area, resource, scope, aws, azure, gcp, adjustable, release_status, measure_method, notes, page_path
    ('uc_catalogs_per_metastore', 'metastore',          'Unity Catalog', 'Catalogs',                       'metastore',  1000,    1000,    1000,    true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('uc_schemas_per_catalog', 'metastore',             'Unity Catalog', 'Schemas',                        'catalog',    10000,   10000,   10000,   true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('uc_tables_per_schema', 'metastore',               'Unity Catalog', 'Tables',                         'schema',     10000,   10000,   10000,   true, 'GA',  'information_schema', 'Counts all table types, including views', 'resources/limits'),
    ('uc_tables_per_metastore', 'metastore',            'Unity Catalog', 'Tables',                         'metastore',  1000000, 1000000, 1000000, false, 'GA', 'information_schema', NULL, 'resources/limits'),
    ('uc_columns_per_table', 'metastore',               'Unity Catalog', 'Columns',                        'table',      32768,   32768,   32768,   false, 'GA', 'information_schema', NULL, 'resources/limits'),
    ('uc_volumes_per_schema', 'metastore',              'Unity Catalog', 'Volumes',                        'schema',     10000,   10000,   10000,   true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('uc_volumes_per_metastore', 'metastore',           'Unity Catalog', 'Volumes',                        'metastore',  100000,  100000,  100000,  true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('uc_functions_per_schema', 'metastore',            'Unity Catalog', 'Functions',                      'schema',     10000,   10000,   10000,   true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('uc_registered_models_per_schema', 'metastore',    'Unity Catalog', 'Registered models',              'schema',     1000,    1000,    1000,    true, 'GA',  'rest_api',           'Measured by scripts/scan_rest.py', 'resources/limits'),
    ('uc_registered_models_per_metastore', 'metastore', 'Unity Catalog', 'Registered models',              'metastore',  5000,    5000,    5000,    true, 'GA',  'rest_api',           'Measured by scripts/scan_rest.py', 'resources/limits'),
    ('uc_model_versions_per_model', 'metastore',        'Unity Catalog', 'Model versions',                 'registered model', 10000, 10000, 10000, true, 'GA', 'rest_api',           'Measured by scripts/scan_rest.py', 'resources/limits'),
    ('uc_model_versions_per_metastore', 'metastore',    'Unity Catalog', 'Model versions',                 'metastore',  100000,  100000,  100000,  false, 'GA', 'rest_api',           'Measured by scripts/scan_rest.py', 'resources/limits'),
    ('uc_connections_per_metastore', 'metastore',       'Unity Catalog', 'Connections',                    'metastore',  1000,    1000,    1000,    true, 'GA',  'information_schema', 'Scan excludes MANAGED_* connection types (unverified whether they count)', 'resources/limits'),
    ('uc_external_locations_per_metastore', 'metastore','Unity Catalog', 'External locations',             'metastore',  10000,   10000,   10000,   true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('uc_credentials_per_metastore', 'metastore',       'Unity Catalog', 'Storage and service credentials','metastore',  1000,    1000,    1000,    true, 'GA',  'information_schema', 'Combined total of storage and service credentials', 'resources/limits'),
    ('uc_privileges_per_parent_object', 'metastore',    'Unity Catalog', 'Privileges',                     'parent object (metastore, catalog, schema)', 4000, 4000, 4000, true, 'GA', 'not_measurable', NULL, 'resources/limits'),
    ('uc_privileges_per_object', 'metastore',           'Unity Catalog', 'Privileges',                     'non-parent object (table, view, share)',     1000, 1000, 1000, true, 'GA', 'not_measurable', NULL, 'resources/limits'),
    ('uc_policies_per_metastore', 'metastore',          'Unity Catalog', 'Policies',                       'metastore',  10000,   10000,   10000,   true, NULL,  'not_measurable',     'Release status not yet confirmed from docs', 'resources/limits'),
    ('uc_tag_assignments_per_securable', 'metastore',   'Tags',          'Tag assignments',                'securable object', 50,  50,      50,      false, 'GA', 'information_schema', 'Measured for catalogs, schemas, tables and volumes', 'resources/limits'),
    ('uc_column_tag_assignments_per_table', 'metastore','Tags',          'Column tag assignments',         'table',      1000,    1000,    1000,    true, 'GA',  'information_schema', 'All columns of a table counted together', 'resources/limits'),
    ('sharing_providers_per_metastore', 'metastore',    'Delta Sharing', 'Providers',                      'metastore',  1000,    1000,    1000,    true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('sharing_recipients_per_metastore', 'metastore',   'Delta Sharing', 'Recipients',                     'metastore',  5000,    5000,    5000,    true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('sharing_shares_per_metastore', 'metastore',       'Delta Sharing', 'Shares',                         'metastore',  1000,    1000,    1000,    true, 'GA',  'information_schema', NULL, 'resources/limits'),
    ('sharing_tables_per_share', 'metastore',           'Delta Sharing', 'Tables',                         'share',      1000,    1000,    1000,    true, 'GA',  'information_schema', 'Counted from table_share_usage', 'resources/limits'),
    ('cleanrooms_per_metastore', 'metastore',           'Clean rooms',   'Clean rooms',                    'metastore',  100,     10,      10,      true, 'GA',  'rest_api',           'Azure and GCP limit is 10; measured by scripts/scan_rest.py (information_schema.clean_rooms is not queryable)', 'resources/limits'),
    ('ws_apps_per_workspace', 'workspace', 'Databricks Apps', 'Apps', 'workspace', 100, 100, 50, false, 'GA', 'rest_api', 'Docs list this as fixed; ask your account team if you need more', 'resources/limits'),
    ('ws_saved_jobs_per_workspace', 'workspace', 'Jobs', 'Saved jobs', 'workspace', 12000, 12000, 12000, false, 'GA', 'rest_api', NULL, 'resources/limits'),
    ('ws_saved_pipelines_per_workspace', 'workspace', 'Lakeflow pipelines', 'Saved pipelines', 'workspace', 12000, 12000, 12000, false, 'GA', 'rest_api', 'Includes active and inactive pipelines', 'resources/limits'),
    ('ws_sql_warehouses_per_workspace', 'workspace', 'SQL warehouses', 'SQL warehouses', 'workspace', 1000, 1000, 1000, false, 'GA', 'rest_api', NULL, 'resources/limits'),
    ('ws_secret_scopes_per_workspace', 'workspace', 'Secrets', 'Secret scopes', 'workspace', 1000, 1000, 1000, true, 'GA', 'rest_api', NULL, 'resources/limits'),
    ('ws_ip_access_entries_per_workspace', 'workspace', 'Networking', 'IP access list entries (CIDR)', 'workspace', 1000, 1000, 1000, false, 'GA', 'rest_api', 'Summed across all workspace IP access lists', 'resources/limits'),
    ('ws_serving_endpoints_per_workspace', 'workspace', 'Model Serving', 'Serving endpoints', 'workspace', 1000, 1000, 1000, true, 'GA', 'rest_api', 'Custom model and agent endpoints limit', 'machine-learning/model-serving/model-serving-limits'),
    ('ws_ai_search_endpoints_per_workspace', 'workspace', 'AI Search', 'AI Search endpoints', 'workspace', 500, 500, 500, true, 'GA', 'rest_api', NULL, 'resources/limits'),
    ('ai_search_indexes_per_endpoint', 'workspace', 'AI Search', 'Indexes', 'AI Search endpoint', 50, 50, 50, true, 'GA', 'rest_api', NULL, 'resources/limits'),
    ('acct_groups_per_account', 'account', 'Identity', 'Groups', 'account', 5000, 5000, 5000, true, 'GA', 'account_api', 'Accounts created on or after 2025-11-17 can have up to 10,000; needs an account admin to measure', 'resources/limits'),
    ('acct_users_sps_per_account', 'account', 'Identity', 'Users and service principals', 'account', 10000, 10000, 10000, true, 'GA', 'account_api', 'Accounts created on or after 2025-11-17 can have up to 50,000; needs an account admin to measure', 'resources/limits')
  AS t(limit_id, level, area, resource, scope, limit_aws, limit_azure, limit_gcp, adjustable, release_status, measure_method, notes, page_path);

  INSERT INTO IDENTIFIER(v_prefix || '.limit_changes')
    (limit_id, cloud, change_type, old_value, new_value, detected_on, logged_at, source_url)
  WITH
  r AS (SELECT * FROM IDENTIFIER(v_prefix || '.limits_reference')),
  diffs AS (
    SELECT s.limit_id, 'all' AS cloud, 'added' AS change_type, CAST(NULL AS STRING) AS old_value,
           CAST(NULL AS STRING) AS new_value, CAST(NULL AS STRING) AS source_url
    FROM lt_seed s LEFT ANTI JOIN r ON s.limit_id = r.limit_id
    UNION ALL
    SELECT r.limit_id, 'all', 'removed', NULL, NULL, NULL
    FROM r LEFT ANTI JOIN lt_seed s ON s.limit_id = r.limit_id
    UNION ALL
    SELECT s.limit_id, c.cloud, c.change_type, c.old_value, c.new_value, c.source_url
    FROM lt_seed s JOIN r ON s.limit_id = r.limit_id
    LATERAL VIEW stack(4,
      'aws',   'limit_value', CAST(r.limit_aws AS STRING),   CAST(s.limit_aws AS STRING),   s.source_url_aws,
      'azure', 'limit_value', CAST(r.limit_azure AS STRING), CAST(s.limit_azure AS STRING), s.source_url_azure,
      'gcp',   'limit_value', CAST(r.limit_gcp AS STRING),   CAST(s.limit_gcp AS STRING),   s.source_url_gcp,
      'all',   'adjustable',  CAST(r.adjustable AS STRING),  CAST(s.adjustable AS STRING),  CAST(NULL AS STRING)
    ) c AS cloud, change_type, old_value, new_value, source_url
    WHERE NOT (c.old_value <=> c.new_value)
  ),
  -- added/removed share one 'presence' kind; defined once and applied to both sides.
  all_rows AS (
    SELECT 'new' AS src, limit_id, cloud, change_type, old_value, new_value, source_url, CAST(NULL AS TIMESTAMP) AS logged_at FROM diffs
    UNION ALL
    SELECT 'log', limit_id, cloud, change_type, old_value, new_value, source_url, logged_at FROM IDENTIFIER(v_prefix || '.limit_changes')
  ),
  typed AS (
    SELECT *, CASE WHEN change_type IN ('added', 'removed') THEN 'presence' ELSE change_type END AS kind FROM all_rows
  ),
  kinds AS (SELECT * FROM typed WHERE src = 'new'),
  h AS (SELECT * FROM typed WHERE src = 'log'),
  -- Value/adjustability history only counts since the limit was last added, so a change that
  -- repeats one from before a remove + re-add is still logged.
  last_added AS (
    SELECT limit_id, max(logged_at) AS added_at FROM h WHERE change_type = 'added' GROUP BY limit_id
  ),
  last_logged AS (
    SELECT h.limit_id, h.cloud, h.kind, max_by(struct(h.change_type, h.old_value, h.new_value), h.logged_at) AS last
    FROM h LEFT JOIN last_added a ON a.limit_id = h.limit_id
    WHERE h.kind = 'presence' OR a.added_at IS NULL OR h.logged_at > a.added_at
    GROUP BY h.limit_id, h.cloud, h.kind
  )
  SELECT k.limit_id, k.cloud, k.change_type, k.old_value, k.new_value, current_date(), current_timestamp(), k.source_url
  FROM kinds k
  LEFT JOIN last_logged l ON l.limit_id = k.limit_id AND l.cloud = k.cloud AND l.kind = k.kind
  WHERE l.last IS NULL
     OR NOT (l.last.change_type = k.change_type AND l.last.old_value <=> k.old_value AND l.last.new_value <=> k.new_value);

  MERGE INTO IDENTIFIER(v_prefix || '.limits_reference') r
  USING lt_seed s
  ON r.limit_id = s.limit_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  WHEN NOT MATCHED BY SOURCE THEN DELETE;

  DROP VIEW IF EXISTS lt_seed;
END;
