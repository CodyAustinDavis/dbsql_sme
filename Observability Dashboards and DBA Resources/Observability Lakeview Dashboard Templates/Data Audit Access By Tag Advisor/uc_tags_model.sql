CREATE SCHEMA IF NOT EXISTS main.access_history_advisor;

CREATE MATERIALIZED VIEW IF NOT EXISTS main.access_history_advisor.uc_tags
 (
    catalog_name STRING,
    schema_name STRING,
    table_name STRING,
    column_name STRING,
    tag_name STRING,
    tag_value STRING,
    object_type STRING
  ) 
COMMENT 'UC Tags in a Single Table'
SCHEDULE CRON '0 0 0 * * ? *'
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'tag_name')
AS
SELECT
  catalog_name,
  null as schema_name,
  null as table_name,
  null as column_name,
  tag_name,
  tag_value,
  'catalog' as object_type
FROM
  system.information_schema.catalog_tags
where
  catalog_name != '__databricks_internal'
UNION ALL
SELECT
  catalog_name,
  schema_name,
  null as table_name,
  null as column_name,
  tag_name,
  tag_value,
  'schema' as object_type
FROM
  system.information_schema.schema_tags
where
  catalog_name != '__databricks_internal'
UNION ALL
SELECT
  catalog_name,
  schema_name,
  table_name,
  null as column_name,
  tag_name,
  tag_value,
  'table' as object_type
FROM
  system.information_schema.table_tags
where
  catalog_name != '__databricks_internal'
UNION ALL
SELECT
  catalog_name,
  schema_name,
  table_name,
  column_name,
  tag_name,
  tag_value,
  'column' as object_type
FROM
  system.information_schema.column_tags
where
  catalog_name != '__databricks_internal';

