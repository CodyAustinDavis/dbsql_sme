# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## System Tables for Data Warehousing - as of 2023 11 15
# MAGIC
# MAGIC #### System Tables Covered:
# MAGIC 1. billing
# MAGIC 2. information_schema
# MAGIC 3. compute
# MAGIC 4. access

# COMMAND ----------

# DBTITLE 1,Billing Aggregate BI View with Pricing - Most Recent Contract
# MAGIC %sql
# MAGIC
# MAGIC WITH hours_price_by_sku
# MAGIC AS (
# MAGIC SELECT u.*, 
# MAGIC usage_quantity * px.unit_price AS Usage_Dollars,
# MAGIC
# MAGIC -- HIGH RECOMMMENDED: Standardize Tagging for Clusters / Jobs / and Resources upfront so that billing and usage analytics is easy
# MAGIC -- Tag for ENV
# MAGIC LOWER(COALESCE(custom_tags.env, custom_tags.ENV)) AS Env_Tag,
# MAGIC
# MAGIC -- Ideally Create Tag for Job
# MAGIC -- Create Tag for Subsystem / Use Case
# MAGIC LOWER(COALESCE(custom_tags.service, custom_tags.SERVICE)) AS Subsystem_Name_Tag,
# MAGIC CASE WHEN Env_Tag IS NULL OR Subsystem_Name_Tag IS NULL THEN 0 ELSE 1 END AS IsProperlyTagged,
# MAGIC usage_metadata.job_id AS job_id,
# MAGIC usage_metadata.cluster_id AS cluster_id,
# MAGIC usage_metadata.warehouse_id AS warehouse_id
# MAGIC FROM system.billing.usage AS u
# MAGIC INNER JOIN (
# MAGIC   
# MAGIC   WITH px_all AS (
# MAGIC   SELECT 
# MAGIC   sku_name, pricing.default::float AS unit_price,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY sku_name ORDER BY price_start_time DESC) AS Price_Recency_Rank
# MAGIC   FROM system.billing.list_prices
# MAGIC   )
# MAGIC   SELECT 
# MAGIC   sku_name,
# MAGIC   -- Add in discounts or other customer information
# MAGIC   CASE 
# MAGIC   WHEN sku_name LIKE ('%ALL_PURPOSE%') THEN (unit_price::float * (1-0)::float)::float -- 10% discount
# MAGIC   WHEN sku_name LIKE ('%DLT%') THEN (unit_price::float * (1-0)::float)::float -- 10% discount
# MAGIC   WHEN sku_name LIKE ('%JOBS%') THEN (unit_price::float * (1-0)::float)::float -- 10% discount
# MAGIC   WHEN sku_name LIKE ('%SQL%') THEN (unit_price::float * (1-0)::float)::float -- 10% SQL discount
# MAGIC   ELSE unit_price::float
# MAGIC   END AS unit_price
# MAGIC   FROM px_all WHERE Price_Recency_Rank = 1 --Get most recent prices for each sku
# MAGIC   )
# MAGIC   AS px ON px.sku_name = u.sku_name 
# MAGIC
# MAGIC WHERE u.usage_start_time::date >= now()::date - 30 -- 30 day rolling, make a parameter
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM hours_price_by_sku

# COMMAND ----------

# DBTITLE 1,Usage Trends for Alerting
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS main.cost_observability;
# MAGIC CREATE OR REPLACE TABLE main.cost_observability.billing_trends_aggregate
# MAGIC AS (
# MAGIC WITH clean_usage AS (
# MAGIC
# MAGIC SELECT u.*, 
# MAGIC date_trunc('hour', usage_start_time) AS usage_hour,
# MAGIC usage_quantity * px.unit_price AS Usage_Dollars,
# MAGIC usage_metadata.job_id AS job_id,
# MAGIC usage_metadata.cluster_id AS cluster_id,
# MAGIC usage_metadata.warehouse_id AS warehouse_id
# MAGIC FROM system.billing.usage AS u
# MAGIC INNER JOIN (
# MAGIC   
# MAGIC   WITH px_all AS (
# MAGIC   SELECT 
# MAGIC   sku_name, pricing.default::float AS unit_price,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY sku_name ORDER BY price_start_time DESC) AS Price_Recency_Rank
# MAGIC   FROM system.billing.list_prices 
# MAGIC   )
# MAGIC   SELECT 
# MAGIC   sku_name,
# MAGIC   unit_price
# MAGIC   FROM px_all WHERE Price_Recency_Rank = 1 --Get most recent prices for each sku
# MAGIC   )
# MAGIC   AS px ON px.sku_name = u.sku_name 
# MAGIC
# MAGIC WHERE u.usage_date::date >= now()::date - 30 -- 30 day rolling, make a parameter
# MAGIC ),
# MAGIC
# MAGIC grouped_usage_by_sku AS (
# MAGIC
# MAGIC SELECT 
# MAGIC usage_start_time,
# MAGIC sku_name,
# MAGIC SUM(Usage_Dollars) AS Usage_Dollars
# MAGIC FROM hourly_smooth_usage
# MAGIC GROUP BY usage_start_time, sku_name
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC usage_start_time,
# MAGIC sku_name,
# MAGIC --  By SKU or TAG (good idea)
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         PARTITION BY sku_name 
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_4_hours_by_sku,
# MAGIC
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         PARTITION BY sku_name 
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_12_hours_by_sku,
# MAGIC
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         PARTITION BY sku_name 
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 72 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_72_hours_by_sku,
# MAGIC
# MAGIC -- Overall Trending for billing executive elerts
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_4_hours_overall,
# MAGIC     
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_12_hours_overall,
# MAGIC
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 72 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_72_hours_overall
# MAGIC     
# MAGIC FROM grouped_usage_by_sku
# MAGIC ORDER BY usage_start_time
# MAGIC );
# MAGIC
# MAGIC -- Get Overall Usage Trends
# MAGIC SELECT * FROM main.cost_observability.billing_trends_aggregate

# COMMAND ----------

# DBTITLE 1,Create Budgets on Hourly / Daily Basis From Contract Details By SKU / Job
# MAGIC %sql
# MAGIC
# MAGIC -- Create budgets manually by editing static table
# MAGIC -- This can be cumulative, annual, based on contract, etc.
# MAGIC CREATE OR REPLACE TABLE main.cost_observability.budgets_by_sku
# MAGIC (sku STRING, hourly_budget FLOAT);
# MAGIC
# MAGIC INSERT INTO main.cost_observability.budgets_by_sku
# MAGIC VALUES ('ALL_PURPOSE', 25), ('JOBS', 30), ('SQL', 15), ('DLT', 25)

# COMMAND ----------

# DBTITLE 1,Calculate Usage Trends By Sku, Tag, Job, and/or Warehouse id 
# MAGIC %sql
# MAGIC -- All DBSQL Usage Trends
# MAGIC -- Now we can calculate statistical metrics on this to alert on Variances
# MAGIC SELECT *
# MAGIC FROM main.cost_observability.billing_trends_aggregate AS usage
# MAGIC LEFT JOIN main.cost_observability.budgets_by_sku AS bud
# MAGIC   ON bud.sku = 'SQL'
# MAGIC WHERE sku_name LIKE ('%SQL%')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.cost_observability.billing_trends_aggregate
# MAGIC LEFT JOIN main.cost_observability.budgets_by_sku AS bud
# MAGIC   ON bud.sku = 'DLT'
# MAGIC WHERE sku_name LIKE ('%DLT%')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.cost_observability.billing_trends_aggregate
# MAGIC LEFT JOIN main.cost_observability.budgets_by_sku AS bud
# MAGIC   ON bud.sku = 'JOBS'
# MAGIC WHERE sku_name LIKE ('%JOBS%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM main.cost_observability.billing_trends_aggregate
# MAGIC LEFT JOIN main.cost_observability.budgets_by_sku AS bud
# MAGIC   ON bud.sku = 'ALL_PURPOSE'
# MAGIC WHERE sku_name LIKE ('%ALL_PURPOSE%')

# COMMAND ----------

# DBTITLE 1,Billing Momentum Analytics and Alerting
# MAGIC %sql
# MAGIC
# MAGIC WITH hourly_smoothed_usage AS (
# MAGIC SELECT u.*, 
# MAGIC date_trunc('hour', usage_start_time) AS usage_hour,
# MAGIC usage_quantity * px.unit_price AS Usage_Dollars,
# MAGIC usage_metadata.job_id AS job_id,
# MAGIC usage_metadata.cluster_id AS cluster_id,
# MAGIC usage_metadata.warehouse_id AS warehouse_id
# MAGIC FROM system.billing.usage AS u
# MAGIC INNER JOIN (
# MAGIC   
# MAGIC   WITH px_all AS (
# MAGIC   SELECT 
# MAGIC   sku_name, pricing.default::float AS unit_price,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY sku_name ORDER BY price_start_time DESC) AS Price_Recency_Rank
# MAGIC   FROM system.billing.list_prices 
# MAGIC   )
# MAGIC   SELECT 
# MAGIC   sku_name,
# MAGIC   unit_price
# MAGIC   FROM px_all WHERE Price_Recency_Rank = 1 --Get most recent prices for each sku
# MAGIC   )
# MAGIC   AS px ON px.sku_name = u.sku_name 
# MAGIC
# MAGIC WHERE u.usage_date::date >= now()::date - 30 -- 30 day rolling, make a parameter
# MAGIC ),
# MAGIC
# MAGIC grouped_usage_by_sku AS (
# MAGIC
# MAGIC SELECT 
# MAGIC usage_start_time,
# MAGIC SUM(Usage_Dollars) AS Usage_Dollars
# MAGIC FROM hourly_smooth_usage
# MAGIC GROUP BY usage_start_time
# MAGIC ),
# MAGIC
# MAGIC rolling_metrics AS (
# MAGIC SELECT 
# MAGIC usage_start_time,
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_4_hours_overall,
# MAGIC     
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_12_hours_overall,
# MAGIC
# MAGIC     
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_24_hours_overall,
# MAGIC
# MAGIC AVG(Usage_Dollars) OVER (
# MAGIC         ORDER BY usage_start_time DESC 
# MAGIC         ROWS BETWEEN 72 PRECEDING AND CURRENT ROW
# MAGIC     ) AS rolling_72_hours_overall
# MAGIC FROM grouped_usage_by_sku
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC *,
# MAGIC STDDEV(rolling_4_hours_overall) OVER (ORDER BY usage_start_time DESC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_4_hours_stddev,
# MAGIC STDDEV(rolling_12_hours_overall) OVER (ORDER BY usage_start_time DESC ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS moving_12_hours_stddev,
# MAGIC STDDEV(rolling_72_hours_overall) OVER (ORDER BY usage_start_time DESC ROWS BETWEEN 72 PRECEDING AND CURRENT ROW) AS moving_72_hours_stddev,
# MAGIC     rolling_72_hours_overall + (moving_72_hours_stddev * 2) AS rolling_72_hour_upper_band,
# MAGIC     rolling_72_hours_overall - (moving_72_hours_stddev * 2) AS rolling_72_hour_lower_band
# MAGIC FROM rolling_metrics
# MAGIC ORDER BY usage_start_time
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Top Usage in Periods of Time By Warehouse, Job, Cluster Id
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE main.cost_observability.billing_with_clusters
# MAGIC AS
# MAGIC WITH clean_usage AS (
# MAGIC
# MAGIC SELECT u.*, 
# MAGIC date_trunc('hour', usage_start_time) AS usage_hour,
# MAGIC usage_quantity * px.unit_price AS Usage_Dollars,
# MAGIC usage_metadata.job_id AS job_id,
# MAGIC usage_metadata.cluster_id AS cluster_id,
# MAGIC usage_metadata.warehouse_id AS warehouse_id
# MAGIC FROM system.billing.usage AS u
# MAGIC INNER JOIN (
# MAGIC   
# MAGIC   WITH px_all AS (
# MAGIC   SELECT 
# MAGIC   sku_name, pricing.default::float AS unit_price,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY sku_name ORDER BY price_start_time DESC) AS Price_Recency_Rank
# MAGIC   FROM system.billing.list_prices 
# MAGIC   )
# MAGIC   SELECT 
# MAGIC   sku_name,
# MAGIC   unit_price
# MAGIC   FROM px_all WHERE Price_Recency_Rank = 1 --Get most recent prices for each sku
# MAGIC   )
# MAGIC   AS px ON px.sku_name = u.sku_name 
# MAGIC
# MAGIC WHERE u.usage_date::date >= now()::date - 30 -- 30 day rolling, make a parameter
# MAGIC )
# MAGIC SELECT u.*, c.cluster_name, c.owned_by, c.create_time, c.delete_time, CASE WHEN c.delete_time IS NOT NULL THEN 1 ELSE 0 END as is_deleted,
# MAGIC cluster_source, dbr_version, change_time AS most_recent_cluster_update_time
# MAGIC FROM clean_usage AS u
# MAGIC LEFT JOIN system.compute.clusters c ON c.cluster_id = u.cluster_id
# MAGIC WHERE c.cluster_id IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Usage By Cluster Owner, Type, Source
# MAGIC %sql
# MAGIC -- Top 10 Clusters
# MAGIC WITH cluster_rank AS (
# MAGIC   
# MAGIC SELECT
# MAGIC cluster_name, owned_by,
# MAGIC SUM(Usage_Dollars) AS TotalUsageDollars,
# MAGIC MAX(dbr_version) AS DBR_Version, 
# MAGIC MAX(is_deleted) AS IsDeleted,
# MAGIC MAX(most_recent_cluster_update_time) AS Update_Time
# MAGIC FROM main.cost_observability.billing_with_clusters
# MAGIC GROUP BY cluster_name, owned_by
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM cluster_rank ORDER BY TotalUsageDollars DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Cluster Usage By Jobs
# MAGIC %sql
# MAGIC -- Top 10 Clusters
# MAGIC WITH cluster_job_rank AS (
# MAGIC   
# MAGIC SELECT
# MAGIC cluster_name, owned_by,
# MAGIC job_id,
# MAGIC sku_name,
# MAGIC
# MAGIC SUM(Usage_Dollars) AS TotalUsageDollars,
# MAGIC MAX(dbr_version) AS DBR_Version, 
# MAGIC MAX(is_deleted) AS IsDeleted,
# MAGIC MAX(most_recent_cluster_update_time) AS Update_Time
# MAGIC FROM main.cost_observability.billing_with_clusters
# MAGIC GROUP BY cluster_name, owned_by, job_id, sku_name
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM cluster_job_rank
# MAGIC WHERE job_id IS NOT NULL -- Clusters running Jobs
# MAGIC  ORDER BY TotalUsageDollars DESC
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,What else is in the compute table?
# MAGIC %sql
# MAGIC
# MAGIC -- Tags
# MAGIC -- AWS info
# MAGIC
# MAGIC SELECT * FROM system.compute.clusters

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Access Management Tables
# MAGIC 1. Audit Logs
# MAGIC 2. Table/Column Lineage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT audit_level
# MAGIC FROM system.access.audit

# COMMAND ----------

# DBTITLE 1,Workspace Access History
# MAGIC %sql
# MAGIC
# MAGIC -- User / Events over time
# MAGIC CREATE OR REPLACE TABLE main.cost_observability.audit_access_trends
# MAGIC AS (
# MAGIC SELECT *,
# MAGIC date_trunc('hour', event_time) AS event_hour,
# MAGIC user_identity.email AS user,
# MAGIC CASE WHEN response.status_code = 200 THEN 1 ELSE 0 END AS is_success
# MAGIC FROM system.access.audit
# MAGIC WHERE audit_level = 'WORKSPACE_LEVEL'
# MAGIC AND event_date >= now()::date - 7
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Event Aggregate Trends Over Time
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC event_hour,
# MAGIC service_name,
# MAGIC action_name,
# MAGIC COUNT(DISTINCT user) AS User_Count,
# MAGIC COUNT(DISTINCT event_id) AS Event_Count,
# MAGIC COUNT(DISTINCT CASE WHEN is_success = 1 THEN event_id END) AS Successfull_Events,
# MAGIC COUNT(DISTINCT CASE WHEN is_success = 0 THEN event_id END) AS Failed_Events,
# MAGIC Successfull_Events::float / Event_Count::float AS Success_Rate
# MAGIC FROM main.cost_observability.audit_access_trends
# MAGIC GROUP BY event_hour, service_name, action_name
# MAGIC ORDER BY event_hour, service_name, action_name
# MAGIC

# COMMAND ----------

# DBTITLE 1,Trending Specific Events
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM main.cost_observability.audit_access_trends

# COMMAND ----------

# DBTITLE 1,Analyze Success Rate of Events By Service and User
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC event_hour,
# MAGIC service_name,
# MAGIC action_name,
# MAGIC COUNT(DISTINCT user) AS User_Count,
# MAGIC COUNT(DISTINCT event_id) AS Event_Count,
# MAGIC COUNT(DISTINCT CASE WHEN is_success = 1 THEN event_id END) AS Successfull_Events,
# MAGIC COUNT(DISTINCT CASE WHEN is_success = 0 THEN event_id END) AS Failed_Events,
# MAGIC Successfull_Events::float / Event_Count::float AS Success_Rate
# MAGIC FROM main.cost_observability.audit_access_trends
# MAGIC WHERE service_name = 'databrickssql'
# MAGIC GROUP BY event_hour, service_name, action_name
# MAGIC ORDER BY event_hour, service_name, action_name
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Group / User Data Access Management and Search with Information Schema

# COMMAND ----------

dbutils.widgets.text("Group Names", "")
dbutils.widgets.text("User Names", "")


group_names = dbutils.widgets.get("Group Names")
user_names = dbutils.widgets.get("User Names")

print(f"Searching For all Data Access for Group: {group_names}")
print(f"Seaching For all Data Access for User: {user_names}")
      

all_names = []

if len(group_names) > 1:
  new_groups = [i.strip() for i in group_names.split(",")]
  all_names = new_groups + all_names

if len(user_names) > 1:
  new_users = [i.strip() for i in user_names.split(",")]
  all_names = new_users + all_names

all_names = list(set(all_names))
all_names_str = ",".join(all_names)
print(f"Searching for access on entities: {all_names}")

# COMMAND ----------

from pyspark.sql.functions import *

cat_privs = spark.sql(f"""
SELECT *,
CASE 
WHEN privilege_type LIKE ('CREATE%') THEN 'Edit'
WHEN privilege_type LIKE ('ALL_%') THEN 'Edit'
WHEN privilege_type LIKE ('MODIFY%') THEN 'Edit'
WHEN privilege_type LIKE ('APPLY_%') THEN 'Edit'
WHEN privilege_type LIKE ('EXECUTE%') THEN 'Edit'
WHEN privilege_type LIKE ('WRITE_%') THEN 'Edit'
WHEN privilege_type LIKE ('READ_%') THEN 'Read'
WHEN privilege_type LIKE ('REFRESH%') THEN 'Read'
WHEN privilege_type LIKE ('SELECT%') THEN 'Read'
WHEN privilege_type LIKE ('USE%') THEN 'Read'
ELSE 'Uknown' END AS AccessType
FROM system.information_schema.catalog_privileges
""").filter(col("grantee").isin(*all_names))

schema_privs = spark.sql(f"""
SELECT *,
CASE 
WHEN privilege_type LIKE ('CREATE%') THEN 'Edit'
WHEN privilege_type LIKE ('ALL_%') THEN 'Edit'
WHEN privilege_type LIKE ('MODIFY%') THEN 'Edit'
WHEN privilege_type LIKE ('APPLY_%') THEN 'Edit'
WHEN privilege_type LIKE ('EXECUTE%') THEN 'Edit'
WHEN privilege_type LIKE ('WRITE_%') THEN 'Edit'
WHEN privilege_type LIKE ('READ_%') THEN 'Read'
WHEN privilege_type LIKE ('REFRESH%') THEN 'Read'
WHEN privilege_type LIKE ('SELECT%') THEN 'Read'
WHEN privilege_type LIKE ('USE%') THEN 'Read'
ELSE 'Uknown' END AS AccessType
FROM system.information_schema.schema_privileges
""").filter(col("grantee").isin(*all_names))


tbl_privs = spark.sql(f"""
SELECT *,
CASE 
WHEN privilege_type LIKE ('CREATE%') THEN 'Edit'
WHEN privilege_type LIKE ('ALL_%') THEN 'Edit'
WHEN privilege_type LIKE ('MODIFY%') THEN 'Edit'
WHEN privilege_type LIKE ('APPLY_%') THEN 'Edit'
WHEN privilege_type LIKE ('EXECUTE%') THEN 'Edit'
WHEN privilege_type LIKE ('WRITE_%') THEN 'Edit'
WHEN privilege_type LIKE ('READ_%') THEN 'Read'
WHEN privilege_type LIKE ('REFRESH%') THEN 'Read'
WHEN privilege_type LIKE ('SELECT%') THEN 'Read'
WHEN privilege_type LIKE ('USE%') THEN 'Read'
ELSE 'Uknown' END AS AccessType
FROM system.information_schema.table_privileges
""").filter(col("grantee").isin(*all_names))

tbl_privs.createOrReplaceTempView("tbl_privs")
schema_privs.createOrReplaceTempView("schema_privs")
cat_privs.createOrReplaceTempView("cat_privs")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC grantee,
# MAGIC catalog_name,
# MAGIC COUNT(DISTINCT privilege_type) AS NumberOfTotalPermissions,
# MAGIC COUNT(DISTINCT CASE WHEN AccessType = 'Read' THEN privilege_type END) AS NumberOfReadPermissions,
# MAGIC COUNT(DISTINCT CASE WHEN AccessType = 'Edit' THEN privilege_type END) AS NumberOfAlterPermissions,
# MAGIC CASE WHEN NumberOfReadPermissions > 0 THEN 1 ELSE 0 END AS Has_Read_Access,
# MAGIC CASE WHEN NumberOfAlterPermissions > 0 THEN 1 ELSE 0 END AS Has_Edit_Access
# MAGIC FROM cat_privs
# MAGIC GROUP BY 
# MAGIC grantee,
# MAGIC catalog_name
# MAGIC ORDER BY NumberOfTotalPermissions DESC

# COMMAND ----------

# DBTITLE 1,Schema Level Access
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC grantee,
# MAGIC CONCAT(catalog_name, '.', schema_name) AS SchemaName,
# MAGIC COUNT(DISTINCT privilege_type) AS NumberOfTotalPermissions,
# MAGIC COUNT(DISTINCT CASE WHEN AccessType = 'Read' THEN privilege_type END) AS NumberOfReadPermissions,
# MAGIC COUNT(DISTINCT CASE WHEN AccessType = 'Edit' THEN privilege_type END) AS NumberOfAlterPermissions,
# MAGIC CASE WHEN NumberOfReadPermissions > 0 THEN 1 ELSE 0 END AS Has_Read_Access,
# MAGIC CASE WHEN NumberOfAlterPermissions > 0 THEN 1 ELSE 0 END AS Has_Edit_Access
# MAGIC FROM schema_privs
# MAGIC WHERE schema_name != 'information_schema'
# MAGIC AND catalog_name != 'system'
# MAGIC GROUP BY 
# MAGIC grantee,
# MAGIC SchemaName
# MAGIC ORDER BY NumberOfTotalPermissions DESC

# COMMAND ----------

# DBTITLE 1,Table Level Access
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC grantee,
# MAGIC CONCAT(table_catalog, '.', table_schema, '.', table_name) AS TableName,
# MAGIC COUNT(DISTINCT privilege_type) AS NumberOfTotalPermissions,
# MAGIC COUNT(DISTINCT CASE WHEN AccessType = 'Read' THEN privilege_type END) AS NumberOfReadPermissions,
# MAGIC COUNT(DISTINCT CASE WHEN AccessType = 'Edit' THEN privilege_type END) AS NumberOfAlterPermissions,
# MAGIC CASE WHEN NumberOfReadPermissions > 0 THEN 1 ELSE 0 END AS Has_Read_Access,
# MAGIC CASE WHEN NumberOfAlterPermissions > 0 THEN 1 ELSE 0 END AS Has_Edit_Access
# MAGIC FROM tbl_privs
# MAGIC WHERE table_schema != 'information_schema'
# MAGIC AND table_catalog != 'system'
# MAGIC GROUP BY 
# MAGIC grantee,
# MAGIC TableName
# MAGIC ORDER BY NumberOfTotalPermissions DESC

# COMMAND ----------

# DBTITLE 1,Table / Asset Level Staleness Analytics
# MAGIC %sql
# MAGIC
# MAGIC -- This is the number one feature users request when we walk to them about governing their env
# MAGIC -- How often are my tables altered? 
# MAGIC -- How often are my tables READ?? -- How do we answer this with system tables? The tables coming soon
# MAGIC
# MAGIC -- Oldest Updated Tables
# MAGIC SELECT table_catalog,
# MAGIC table_schema,
# MAGIC CONCAT(table_catalog, '.', table_schema,'.', table_name) AS table_name,
# MAGIC table_owner,
# MAGIC created,
# MAGIC last_altered,
# MAGIC date_diff(last_altered, created) AS DaysActivelyUpdated,
# MAGIC date_diff(now(), last_altered) AS DaysSinceLastUpdate
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog != 'system'
# MAGIC AND table_schema != 'information_schema'
# MAGIC ORDER BY DaysSinceLastUpdate DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,What Else Does Information Schema Have? 
# MAGIC %sql
# MAGIC -- Constraints -- This is HUGELY important to get a data warehouse working in the ecosystem. JDBC and many connectors need this info to work
# MAGIC -- Tags
# MAGIC -- Masks
# MAGIC -- Key columns
# MAGIC -- Referential Constraints
# MAGIC -- Volumes / Privs
# MAGIC -- and more!
# MAGIC
# MAGIC SELECT * FROM system.information_schema.table_constraints
