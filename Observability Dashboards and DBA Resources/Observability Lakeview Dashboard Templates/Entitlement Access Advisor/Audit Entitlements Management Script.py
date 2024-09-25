# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Backend Data Model for Access Entitlements Advisor & Access History Advisor Dashboards
# MAGIC
# MAGIC This is the backend for a dashboard that helps auditors and companies evaluate the current state of their entitlements. This notebook is designed to capture the CURRENT snapshot of the state of all data entity entitlements. The Dashboard then combines this with audit log history to track recent deviations from the current state. 
# MAGIC
# MAGIC The best practice is to run this notebook on a daily cadence to keep the entitlement snapshots up to date, and then use audit logs to track events that deviate from the current state. 

# COMMAND ----------

# DBTITLE 1,Define Output Location for Results
target_catalog = "main"
target_schema = "access_entitlements_advisor"
spark.sql(f"USE CATALOG {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE SCHEMA {target_schema}")

# COMMAND ----------

# DBTITLE 1,Generate Current Snapshot of Full Entitlement Hierarchy
from databricks.sdk import WorkspaceClient
import pandas as pd

# Create a Databricks client
client = WorkspaceClient()

# Initialize empty list to hold the user-group mapping
user_group_crosswalk = []

# Step 1: List all users
users = client.users.list()

for i in users:
  active_user_name = i.user_name
  active_user_id = i.id

  active_groups = i.groups

  for g in active_groups: 

      # Step 5: For each member, map the user ID to the group
        user_group_crosswalk.append({
            'user_id': active_user_id,
            'user_name': active_user_name,
            'group_name': g.display
        })

# COMMAND ----------

# DBTITLE 1,Convert To Delta Table
from pyspark.sql.functions import current_timestamp

df_crosswalk = pd.DataFrame(user_group_crosswalk)

## Make spark DF
df_spark_entitlements = spark.createDataFrame(df_crosswalk)

target_table = target_catalog + "." + target_schema + "." + "user_group_crosswalk"

df_spark_entitlements.withColumn("snapshot_timestsamp", current_timestamp()).write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# DBTITLE 1,Create Table Snapshot of Table Priveleges at the same time as the group membership
## We do this for performance reasons as well as to ensure the snapshot is done at the same time as the user/group mapping

spark.sql(f"""CREATE OR REPLACE TABLE {target_catalog}.{target_schema}.all_privileges_snapshot
          AS 
          (
            -- Tables    
SELECT grantor, grantee, 
    table_catalog AS catalog, 
    table_schema AS schema,
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS entity_name, 
    'table' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege,
CASE WHEN privilege_type IN ('ALL_PRIVILEGES', 'MODIFY', 'APPLY_TAG')
        THEN 'EDIT_ACCESS'
        ELSE 'READ_ONLY'
        END AS privilege_level,
        now() AS snapshot_timestsamp
FROM system.information_schema.table_privileges
 UNION ALL 

-- Schema
SELECT grantor, grantee, 
    catalog_name AS catalog, 
    schema_name AS schema,
    CONCAT(catalog_name, '.', schema_name) AS entity_name, 
    'schema' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege ,
CASE WHEN privilege_type IN ('ALL_PRIVILEGES', 'MODIFY', 'APPLY_TAG', 'CREATE_MODEL', 'CREATE_MATERIALIZED_VIEW', 'REFRESH', 'APPLY_TAG', 'CREATE_FUNCTION', 'CREATE_VOLUME', 'WRITE_VOLUME', 'CREATE_TABLE', 'CREATE_VIEW', 'MODIFY')
        THEN 'EDIT_ACCESS'
        WHEN privilege_type IN ('SELECT', 'USE_SCHEMA', 'READ_VOLUME', 'EXECUTE') THEN 'READ_ONLY'
        END AS privilege_level,
            now() AS snapshot_timestsamp
FROM system.information_schema.schema_privileges

UNION ALL

-- Catalog 
SELECT grantor, grantee, 
    catalog_name AS catalog,
    null AS schema,
    CONCAT(catalog_name) AS entity_name, 
    'catalog' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege,

CASE WHEN privilege_type IN ('CREATE_SCHEMA', 'ALL_PRIVILEGES', 'MODIFY', 'APPLY_TAG', 'CREATE_MODEL', 'CREATE_MATERIALIZED_VIEW', 'REFRESH', 'APPLY_TAG', 'CREATE_FUNCTION', 'CREATE_VOLUME', 'WRITE_VOLUME', 'CREATE_TABLE', 'CREATE_VIEW', 'MODIFY')
        THEN 'EDIT_ACCESS'
        WHEN privilege_type IN ('BROWSE', 'USE_CATALOG', 'SELECT', 'USE_SCHEMA', 'READ_VOLUME', 'EXECUTE') THEN 'READ_ONLY'
        END AS privilege_level,
                now() AS snapshot_timestsamp
FROM system.information_schema.catalog_privileges

UNION ALL

-- Volumes 
SELECT grantor, grantee, 
    volume_catalog AS catalog,
    volume_schema AS schema,
    CONCAT(volume_catalog, '.', volume_schema, '.', volume_name) AS entity_name, 
    'volume' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege ,
CASE WHEN privilege_type IN ('ALL_PRIVILEGES','WRITE_VOLUME','APPLY_TAG')
        THEN 'EDIT_ACCESS'
        WHEN privilege_type IN ('READ_VOLUME') THEN 'READ_ONLY'
        END AS privilege_level,
                now() AS snapshot_timestsamp
FROM system.information_schema.volume_privileges

UNION ALL

-- Storage Credentials 
SELECT grantor, grantee, 
    null AS catalog,
    null AS schema,
    CONCAT(storage_credential_name) AS entity_name, 
    'storage credential' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege,
CASE WHEN privilege_type IN ('ALL_PRIVILEGES','CREATE_EXTERNAL_LOCATION','WRITE_FILES', 'CREATE_EXTERNAL_TABLE')
        THEN 'EDIT_ACCESS'
        WHEN privilege_type IN ('READ_FILES') THEN 'READ_ONLY'
        END AS privilege_level,
                now() AS snapshot_timestsamp
FROM system.information_schema.storage_credential_privileges

UNION ALL

-- External Locations 
SELECT grantor, grantee, 
    null AS catalog, 
    null AS schema,
    CONCAT(external_location_name) AS entity_name, 
    'external location' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege ,
CASE WHEN privilege_type IN ('ALL_PRIVILEGES','CREATE_EXTERNAL_VOLUME', 'CREATE_FOREIGN_CATALOG', 'CREATE_MANAGED_STORAGE','WRITE_FILES', 'CREATE_EXTERNAL_TABLE')
        THEN 'EDIT_ACCESS'
        WHEN privilege_type IN ('BROWSE', 'READ_FILES') THEN 'READ_ONLY'
        END AS privilege_level,
                now() AS snapshot_timestsamp
FROM system.information_schema.external_location_privileges

UNION ALL

-- Metastores
SELECT grantor, grantee, 
    null AS catalog, 
    null AS schema,
    CONCAT(metastore_id) AS entity_name, 
    'metastore' AS entity_type, 
    privilege_type, is_grantable, inherited_from, 
CASE wHEN inherited_from = 'NONE' THEN 'Direct Privlege' ELSE 'Inherited' END AS IsDirectPrivilege ,
    CASE 
        WHEN privilege_type LIKE 'CREATE%' OR privilege_type LIKE 'MANAGE%' THEN 'EDIT_ACCESS'
        WHEN privilege_type IN ('MANAGE_ALLOWLIST', 'CREATE_EXTERNAL_LOCATION', 'SET_SHARE_PERMISSION') THEN 'EDIT_ACCESS'
        WHEN privilege_type LIKE ('USE%') THEN 'READ_ONLY'
    END AS privilege_level,
            now() AS snapshot_timestsamp
FROM system.information_schema.metastore_privileges

          )
          CLUSTER BY (privilege_level, entity_type, grantee, entity_name)
          """)

spark.sql(f"OPTIMIZE {target_catalog}.{target_schema}.all_privileges_snapshot")

# COMMAND ----------

# DBTITLE 1,Example Query
# MAGIC %sql
# MAGIC -- What tables and a user read and from what permission?
# MAGIC
# MAGIC SELECT *
# MAGIC FROM system.information_schema.all_privileges_snapshot t
# MAGIC INNER JOIN main.access_entitlements_advisor.user_group_crosswalk p ON t.grantee = p.user_name OR t.grantee = p.group_name
# MAGIC WHERE p.user_name = 'first.last@databricks.com'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM main.access_entitlements_advisor.all_privileges_snapshot

# COMMAND ----------

# DBTITLE 1,Data Asset Tag Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.access_entitlements_advisor.uc_tags
# MAGIC CLUSTER BY (entity_name, tag_name)
# MAGIC AS
# MAGIC SELECT
# MAGIC   catalog_name,
# MAGIC   null as schema_name,
# MAGIC   null as table_name,
# MAGIC   null as column_name,
# MAGIC   catalog_name AS entity_name,
# MAGIC   'catalog' AS entity_type,
# MAGIC   tag_name,
# MAGIC   tag_value,
# MAGIC   now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC   system.information_schema.catalog_tags
# MAGIC where
# MAGIC   catalog_name NOT IN ('__databricks_internal', 'system')
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   null as table_name,
# MAGIC   null as column_name,
# MAGIC   CONCAT(catalog_name,'.', schema_name) AS entity_name,
# MAGIC   'schema' AS entity_type,
# MAGIC   tag_name,
# MAGIC   tag_value,
# MAGIC           now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC   system.information_schema.schema_tags
# MAGIC where
# MAGIC   catalog_name NOT IN ('__databricks_internal', 'system')
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   null as column_name,
# MAGIC   CONCAT(catalog_name, '.', schema_name, '.', table_name) AS entity_name,
# MAGIC   'table' AS entity_type,
# MAGIC   tag_name,
# MAGIC   tag_value,
# MAGIC           now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC   system.information_schema.table_tags
# MAGIC where
# MAGIC   catalog_name NOT IN ('__databricks_internal', 'system')
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   CONCAT(catalog_name, '.', schema_name, '.', table_name, '.', column_name) AS entity_name,
# MAGIC   'column' AS entity_type,
# MAGIC   tag_name,
# MAGIC   tag_value,
# MAGIC           now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC   system.information_schema.column_tags
# MAGIC where
# MAGIC   catalog_name NOT IN ('__databricks_internal', 'system');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Access History by Entity Type

# COMMAND ----------

# DBTITLE 1,Access Audit History Data Table
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE main.access_entitlements_advisor.table_column_access_history
# MAGIC CLUSTER BY (access_event_type, entity_name)
# MAGIC AS 
# MAGIC --- COLUMN Entity READ Events
# MAGIC SELECT
# MAGIC cl.source_table_catalog AS catalog_name,
# MAGIC cl.source_table_schema AS schema_name,
# MAGIC cl.source_table_name AS table_name,
# MAGIC cl.source_column_name AS column_name,
# MAGIC CONCAT(cl.source_table_catalog, '.', cl.source_table_schema, '.', cl.source_table_name, '.', cl.source_column_name) AS entity_name,
# MAGIC  cl.created_by AS accessed_by,
# MAGIC  cl.entity_type AS access_entity_type,
# MAGIC  cl.source_type AS access_data_asset_type,
# MAGIC  'column' AS access_data_entity_type,
# MAGIC  'READ' AS access_event_type,
# MAGIC  COUNT(distinct cl.event_time) as access_count,
# MAGIC  MIN(cl.event_date) as first_access_date,
# MAGIC  MAX(cl.event_date) as last_access_date,
# MAGIC  collect_set(t.tag_name) AS tags_list,
# MAGIC          now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC  system.access.column_lineage cl
# MAGIC LEFT JOIN main.access_entitlements_advisor.uc_tags AS t
# MAGIC  ON cl.source_table_catalog = t.catalog_name
# MAGIC  AND cl.source_table_schema = t.schema_name
# MAGIC  AND cl.source_table_name = t.table_name
# MAGIC  AND cl.source_column_name = t.column_name
# MAGIC  AND t.entity_type = 'column'
# MAGIC WHERE
# MAGIC  cl.source_type IS NOT NULL
# MAGIC AND CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name, '.', source_column_name) IS NOT NULL
# MAGIC GROUP BY
# MAGIC  ALL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC --- COLUMN Entity WRITE Events
# MAGIC SELECT
# MAGIC target_table_catalog AS catalog_name,
# MAGIC target_table_schema AS schema_name,
# MAGIC target_table_name AS table_name,
# MAGIC target_column_name AS column_name,
# MAGIC CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name, '.', target_column_name) AS entity_name,
# MAGIC  cl.created_by AS accessed_by,
# MAGIC  cl.entity_type AS access_entity_type,
# MAGIC  cl.source_type AS access_data_asset_type,
# MAGIC  'column' AS access_data_entity_type,
# MAGIC  'WRITE' AS access_event_type,
# MAGIC  COUNT(distinct cl.event_time) as access_count,
# MAGIC  MIN(cl.event_date) as first_access_date,
# MAGIC  MAX(cl.event_date) as last_access_date,
# MAGIC collect_set(t.tag_name) AS tags_list,
# MAGIC         now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC  system.access.column_lineage  cl
# MAGIC LEFT JOIN main.access_entitlements_advisor.uc_tags AS t
# MAGIC  ON cl.target_table_catalog = t.catalog_name
# MAGIC  AND cl.target_table_schema = t.schema_name
# MAGIC  AND cl.target_table_name = t.table_name
# MAGIC  AND cl.target_column_name = t.column_name
# MAGIC  AND t.entity_type = 'column'
# MAGIC WHERE
# MAGIC  cl.target_type IS NOT NULL
# MAGIC  AND (
# MAGIC  CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name, '.', target_column_name) IS NOT NULL
# MAGIC  )
# MAGIC GROUP BY
# MAGIC  ALL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC --- TABLE Entity READ Events
# MAGIC SELECT
# MAGIC source_table_catalog AS catalog_name,
# MAGIC source_table_schema AS schema_name,
# MAGIC source_table_name AS table_name,
# MAGIC null AS column_name,
# MAGIC CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name) AS entity_name,
# MAGIC  cl.created_by AS accessed_by,
# MAGIC  cl.entity_type AS access_entity_type,
# MAGIC  cl.source_type AS access_data_asset_type,
# MAGIC  'table' AS access_data_entity_type,
# MAGIC  'READ' AS access_event_type,
# MAGIC  COUNT(distinct cl.event_time) as access_count,
# MAGIC  MIN(cl.event_date) as first_access_date,
# MAGIC  MAX(cl.event_date) as last_access_date,
# MAGIC  collect_set(t.tag_name) AS tags_list,
# MAGIC          now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC  system.access.table_lineage cl
# MAGIC LEFT JOIN main.access_entitlements_advisor.uc_tags AS t
# MAGIC  ON cl.source_table_catalog = t.catalog_name
# MAGIC  AND cl.source_table_schema = t.schema_name
# MAGIC  AND cl.source_table_name = t.table_name
# MAGIC  AND t.entity_type = 'table'
# MAGIC WHERE
# MAGIC  cl.source_type IS NOT NULL
# MAGIC AND CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name) IS NOT NULL
# MAGIC GROUP BY
# MAGIC  ALL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC --- TABLE Entity WRITE Events
# MAGIC SELECT
# MAGIC target_table_catalog AS catalog_name,
# MAGIC target_table_schema AS schema_name,
# MAGIC target_table_name AS table_name,
# MAGIC null AS column_name,
# MAGIC CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name) AS entity_name,
# MAGIC  cl.created_by AS accessed_by,
# MAGIC  cl.entity_type AS access_entity_type,
# MAGIC  cl.source_type AS access_data_asset_type,
# MAGIC  'table' AS access_data_entity_type,
# MAGIC  'WRITE' AS access_event_type,
# MAGIC  COUNT(distinct cl.event_time) as access_count,
# MAGIC  MIN(cl.event_date) as first_access_date,
# MAGIC  MAX(cl.event_date) as last_access_date,
# MAGIC collect_set(t.tag_name) AS tags_list,
# MAGIC         now() AS snapshot_timestsamp
# MAGIC FROM
# MAGIC  system.access.table_lineage cl
# MAGIC LEFT JOIN main.access_entitlements_advisor.uc_tags AS t
# MAGIC  ON cl.target_table_catalog = t.catalog_name
# MAGIC  AND cl.target_table_schema = t.schema_name
# MAGIC  AND cl.target_table_name = t.table_name
# MAGIC  AND t.entity_type = 'table'
# MAGIC WHERE
# MAGIC  target_type IS NOT NULL
# MAGIC  AND (
# MAGIC  CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name) IS NOT NULL
# MAGIC  )
# MAGIC GROUP BY
# MAGIC  ALL
# MAGIC ;
