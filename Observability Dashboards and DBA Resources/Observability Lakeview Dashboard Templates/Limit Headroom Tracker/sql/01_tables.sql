-- limit-tracking: core tables. Parameters: :catalog, :schema. Runs as one SQL scripting block.
BEGIN
  DECLARE v_prefix STRING DEFAULT
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:schema, '`', '``') || '`';
  DECLARE v_sql STRING;

  CREATE SCHEMA IF NOT EXISTS IDENTIFIER(v_prefix)
    COMMENT 'limit-tracking: documented Databricks limits and usage headroom';

  CREATE TABLE IF NOT EXISTS IDENTIFIER(v_prefix || '.limits_reference') (
    limit_id         STRING  NOT NULL COMMENT 'Stable key, e.g. uc_catalogs_per_metastore',
    level            STRING  COMMENT 'Level the limit is enforced at: account | region | metastore | workspace',
    area             STRING  COMMENT 'Unity Catalog, Delta Sharing, Clean rooms, ...',
    resource         STRING  COMMENT 'What is being counted',
    scope            STRING  COMMENT 'Parent the limit applies to: metastore, catalog, schema, table, ...',
    limit_aws        BIGINT,
    limit_azure      BIGINT,
    limit_gcp        BIGINT,
    adjustable       BOOLEAN COMMENT 'TRUE when docs say Fixed = No (request via account team)',
    release_status   STRING  COMMENT 'Release status of the feature the limit belongs to (GA, Public Preview, Beta)',
    measure_method   STRING  COMMENT 'information_schema | rest_api | not_measurable',
    notes            STRING,
    source_url_aws   STRING  NOT NULL COMMENT 'Public docs page the AWS value comes from',
    source_url_azure STRING  NOT NULL COMMENT 'Public docs page the Azure value comes from',
    source_url_gcp   STRING  NOT NULL COMMENT 'Public docs page the GCP value comes from',
    last_verified    DATE    NOT NULL
  ) COMMENT 'Documented Databricks limits and quotas (public docs only).';

  CREATE TABLE IF NOT EXISTS IDENTIFIER(v_prefix || '.limit_changes') (
    limit_id     STRING    NOT NULL,
    cloud        STRING    NOT NULL COMMENT 'aws | azure | gcp | all',
    change_type  STRING    NOT NULL COMMENT 'added | removed | limit_value | adjustable',
    old_value    STRING,
    new_value    STRING,
    detected_on  DATE      NOT NULL COMMENT 'Date the reference seed picked up the change',
    logged_at    TIMESTAMP NOT NULL COMMENT 'Exact time the row was logged (orders same-day changes)',
    source_url   STRING    COMMENT 'Docs page for per-cloud limit_value changes; NULL for cloud = all (see limits_reference)'
  ) COMMENT 'History of documented limits being added, removed, or changing value / adjustability.';

  CREATE TABLE IF NOT EXISTS IDENTIFIER(v_prefix || '.limit_usage') (
    scan_ts      TIMESTAMP NOT NULL,
    limit_id     STRING    NOT NULL,
    cloud        STRING    NOT NULL,
    parent_name  STRING    COMMENT 'Parent object for per-parent limits (e.g. catalog.schema); NULL for metastore-wide',
    workspace_id STRING    COMMENT 'Workspace the row was scanned from (workspace-scoped REST rows); NULL otherwise',
    usage_count  BIGINT    NOT NULL,
    limit_value  BIGINT,
    pct_used     DOUBLE,
    scanned_by   STRING    COMMENT 'Identity that ran the scan; counts only include objects it can see',
    full_visibility BOOLEAN COMMENT 'TRUE if scanned_by can see every object in scope (metastore owner for metastore/catalog/schema rows, workspace admin for workspace rows); FALSE means counts are lower bounds',
    source       STRING    COMMENT 'Scanner that produced the row: information_schema | rest_api'
  ) COMMENT 'Point-in-time usage vs limit. Per-parent limits keep the top 100 parents per scan.';

  -- Upgrade tables created by earlier versions (runs on every job run; a bundle deploy alone runs
  -- no SQL). Add missing columns with the same comments a fresh install has; INSERTs name their
  -- columns, so appended columns are fine. 42710 = column already exists.
  FOR c AS
    SELECT * FROM VALUES
      ('limits_reference', 'level', 'STRING', 'Level the limit is enforced at: account | region | metastore | workspace'),
      ('limit_usage', 'workspace_id',    'STRING',  'Workspace the row was scanned from (workspace-scoped REST rows); NULL otherwise'),
      ('limit_usage', 'full_visibility', 'BOOLEAN', 'TRUE if scanned_by can see every object in scope (metastore owner for metastore/catalog/schema rows, workspace admin for workspace rows); FALSE means counts are lower bounds'),
      ('limit_usage', 'source',          'STRING',  'Scanner that produced the row: information_schema | rest_api')
    AS t(tbl, col, typ, cmt)
  DO
    BEGIN
      DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
      SET v_sql = 'ALTER TABLE ' || v_prefix || '.' || c.tbl || ' ADD COLUMN ' || c.col || ' ' || c.typ
        || ' COMMENT \'' || c.cmt || '\'';
      EXECUTE IMMEDIATE v_sql;
    END;
  END FOR;

  -- Backfill rows written before those columns existed: they all came from the information_schema
  -- scan. The oldest layout recorded visibility as metastore_admin; copy it when that column exists
  -- (42703 = column not found, i.e. nothing to copy).
  -- Only touch the table when un-backfilled rows exist (a no-op scan of NULL stats otherwise).
  IF EXISTS (SELECT 1 FROM IDENTIFIER(v_prefix || '.limit_usage') WHERE source IS NULL) THEN
    SET v_sql = 'UPDATE ' || v_prefix || '.limit_usage SET source = \'information_schema\' WHERE source IS NULL';
    EXECUTE IMMEDIATE v_sql;
  END IF;
  IF EXISTS (SELECT 1 FROM IDENTIFIER(v_prefix || '.limit_usage') WHERE full_visibility IS NULL) THEN
    BEGIN
      DECLARE CONTINUE HANDLER FOR SQLSTATE '42703' BEGIN END;
      SET v_sql = 'UPDATE ' || v_prefix || '.limit_usage SET full_visibility = metastore_admin'
        || ' WHERE full_visibility IS NULL';
      EXECUTE IMMEDIATE v_sql;
    END;
  END IF;

  -- Shared by 03 and 04 so both scanners resolve cloud and metastore visibility the same way.
  -- current_user() is evaluated for whoever queries the view.
  CREATE OR REPLACE VIEW IDENTIFIER(v_prefix || '.scan_context') AS
  SELECT lower(cloud) AS cloud,
         coalesce(current_user() = metastore_owner OR is_account_group_member(metastore_owner), false) AS metastore_admin
  FROM system.information_schema.metastores
  LIMIT 1;

  -- Latest scan from each scanner (information_schema and rest_api write separate scan_ts values).
  -- A view body must name its source literally, so build it from the escaped v_prefix.
  -- Latest scan per source, limit and workspace: a collector skipped in the latest run keeps its
  -- previous rows (check scan_ts), and each workspace keeps its own latest rows.
  -- Explicit columns, so upgraded tables (which may keep legacy columns) expose the same shape.
  SET v_sql = 'CREATE OR REPLACE VIEW ' || v_prefix || '.latest_usage AS SELECT scan_ts, limit_id, cloud, '
    || 'parent_name, workspace_id, usage_count, limit_value, pct_used, scanned_by, full_visibility, source FROM '
    || v_prefix || '.limit_usage QUALIFY scan_ts = max(scan_ts) OVER (PARTITION BY source, limit_id, workspace_id)';
  EXECUTE IMMEDIATE v_sql;
END;
