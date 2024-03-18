# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delta Optimizer - Query History Profiler
# MAGIC
# MAGIC <ul> 
# MAGIC   
# MAGIC   <li> Polls Query History API and get List of Queries for a set of SQL Warehouses (this is incremental, so you just define a lookback period for the first time you poll)
# MAGIC </ul>
# MAGIC
# MAGIC ### Permissions Required:
# MAGIC 1. User running the delta optimizer must have CREATE DATABASE permission to create a delta optimizer instance (OR have it created upfront by an admin). 
# MAGIC 2. User running the delta optimizer must have READ permissions to ALL databases being profiled and optimized. 
# MAGIC 3. User running the delta optimizer must have usage permissions to all SQL Warehouses being profiled and optimized. 
# MAGIC
# MAGIC ### Steps: 
# MAGIC
# MAGIC 1. Decide where you want your Delta Optimizer Instance Output to live by setting <b> Optimizer Output Database </b>. You can have one or many optimizer instances, but each instances needs its own isolated database, they cannot share database namespaces. 
# MAGIC
# MAGIC 2. Insert <b> Server HostName </b>. This is the root workspace url for your databricks workspace. This is NOT tied to any specific cluster. 
# MAGIC
# MAGIC 3. Choose <b> Catalog Filter Mode </b>. There are 3 options: include_list, exclude_list, and all. include_list is default, which allows you to select the databases you want to profile and optimize. exclude_list will monitor ALL databases except the ones in the list. 'all' mode will monitor ALL databases. Note that the user running the delta optimizer must have read permissions to ALL databases selected no matter the mode.
# MAGIC
# MAGIC 4. List the databases to profile in the <b> Catalog Names (csv)... </b> parameter. This is either an include or exclude list. If mode = 'all', then this parameter is not used. 
# MAGIC
# MAGIC 5. Choose <b> Database Filter Mode </b>. There are 3 options: include_list, exclude_list, and all. include_list is default, which allows you to select the databases you want to profile and optimize. exclude_list will monitor ALL databases except the ones in the list. 'all' mode will monitor ALL databases. Note that the user running the delta optimizer must have read permissions to ALL databases selected no matter the mode.
# MAGIC
# MAGIC 6. List the databases to profile in the <b> Database Names (csv)... </b> parameter. This is either an include or exclude list. If mode = 'all', then this parameter is not used. 
# MAGIC
# MAGIC 7. Choose the <b> Table Filter Mode </b>. There are 3 options:  include_list, exclude_list, and all. include_list is default, which allows you to select the subset of tables you want to profile and optimize. This most will ALWAYS operate within the subset of databases chosen from the <b> Database Filter Mode </b>. i.e. if the table you want is not included in the selected databases, no matter the mode, it will not be profiled and optimized. 
# MAGIC
# MAGIC 8. List the tables to profile in the <b> Table Filter List... </b> parameter. This is either an include or exclude list. If mode = 'all', then this parameter is not used. 
# MAGIC
# MAGIC 9. Fill out the list of <b> SQL Warehouse IDs (csv list) </b> to profile and extract query history from. This is how the optimizer will detect HOW your tables are being used in queries. It will use the Query History API to incrementally pull your queries for the selected SQL Warehouses and store them in the Delta optimizer database used. 
# MAGIC
# MAGIC 10. Choose a <b> Query History Lookback Period </b>. This is ONLY for cold starts. This represents a lagging sample of days from today to pull query history for. After the first run, it picks up where it last left off automatically unless the <b> Start Over? </b> parameter = 'Yes'.
# MAGIC
# MAGIC 11. Optionally choose the <b> Start Over? </b> parameter. 'Yes' means it will truncate all historical state and re-profile history from scratch. 'No' means it will always pick up where it left off. 
# MAGIC
# MAGIC
# MAGIC ### KEY USER NOTES: 
# MAGIC 1. Think of the catalog/database/filter lists/modes like a funnel. No matter whether inclusion or exclusion mode for each level, the lower levels will always ONLY contain the subset that results from the previous. For example, if I am running for all catalogs except 'main', then in my database list, if there are any databases that live it 'main', they will not be optimized. 
# MAGIC 2. Database names should be fully qualified (catalog.database.table)
# MAGIC 3. Table Filter List must be fully qualified (catalog.database.table)
# MAGIC 4. If table filter mode is all, then the filter list can be blank, otherwise ensure that it is correct
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### LIMITATIONS: 
# MAGIC 1. Currently it does NOT profile SQL queries run on Adhoc or Jobs clusters, only SQL Warehouses for now. This is on the roadmap to fix. 
# MAGIC
# MAGIC ### Depedencies
# MAGIC <li> Ensure that you either get a token as a secret or use a cluster with the env variable called DBX_TOKEN to authenticate to DBSQL

# COMMAND ----------

from deltaoptimizer import QueryProfiler
import os

# COMMAND ----------

# DBTITLE 1,Register and Retrieve DBX Auth Token
DBX_TOKEN = "<dbx_token>"

# COMMAND ----------

# DBTITLE 1,Set up params before running
## Assume running in a Databricks notebook
dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
dbutils.widgets.text("Server Hostname:", "")
dbutils.widgets.dropdown("Start Over?","No", ["Yes","No"])
dbutils.widgets.text("Optimizer Output Database:", "hive_metastore.delta_optimizer")
dbutils.widgets.text("Optimizer Output Location (optional):", "")
dbutils.widgets.dropdown("Table Filter Mode", "all", ["all", "include_list", "exclude_list"])
dbutils.widgets.dropdown("Database Filter Mode", "all", ["all", "include_list", "exclude_list"])
dbutils.widgets.dropdown("Catalog Filter Mode", "all", ["all", "include_list", "exclude_list"])
dbutils.widgets.text("Table Filter List (catalog.database.table) (Csv List)", "")
dbutils.widgets.text("Database Filter List (catalog.database) (Csv List)", "")
dbutils.widgets.text("Catalog Filter List (Csv List)", "")

# COMMAND ----------

# DBTITLE 1,Get Params to Variables - Can manually supply

## Required
lookbackPeriod = int(dbutils.widgets.get("Query History Lookback Period (days)"))
warehouseIdsList = [i.strip() for i in dbutils.widgets.get("SQL Warehouse Ids (csv list)").split(",")]
workspaceName = dbutils.widgets.get("Server Hostname:").strip()
warehouse_ids = dbutils.widgets.get("SQL Warehouse Ids (csv list)")
database_output = dbutils.widgets.get("Optimizer Output Database:").strip()
start_over = dbutils.widgets.get("Start Over?")

## Optional
table_filter_mode = dbutils.widgets.get("Table Filter Mode")
database_filter_mode = dbutils.widgets.get("Database Filter Mode")
catalog_filter_mode = dbutils.widgets.get("Catalog Filter Mode")
table_filter_list = [i.strip() for i in dbutils.widgets.get("Table Filter List (catalog.database.table) (Csv List)").split(",")]
database_filter_list = [i.strip() for i in dbutils.widgets.get("Database Filter List (catalog.database) (Csv List)").split(",")]
catalog_filter_list = [i.strip() for i in dbutils.widgets.get("Catalog Filter List (Csv List)").split(",")]

if len(dbutils.widgets.get("Optimizer Output Location (optional):").strip()) > 0:
  database_location = dbutils.widgets.get("Optimizer Output Location (optional):").strip()
else: 
  database_location = None


# COMMAND ----------

# DBTITLE 1,Build Query History Profile
####### Step 1: Build Profile #######
## Initialize Profiler

## catalogs_to_check_views should include ALL catalogs where views could live that you want to optimize underlying tables for
## Ideally they are just the same catalogs are your database names defined in the params so we try to parse for you to start there, but if you need to add, change the list here. 

## NOTE: Query profiler doesnt really use database filter mode because it doesnt access the databases, only the SQL Query history API. 

query_profiler = QueryProfiler(workspaceName, 
  warehouseIdsList, 
  database_name=database_output, ## REQUIRED: Need to know where to put the table
  query_history_partition_cols = ['update_date'], ## also could be 'status', or something else
  scrub_views=False)


""" Optional Parameters to Filter the Profile Output After Ingestion: Not really needed for just gathering profile info
## This is really only for downstream analyzing performance with delta optimzier
  database_location=database_location, ## Can be none
  catalogs_to_check_views=catalog_filter_list, 
  catalog_filter_mode=catalog_filter_mode, ## "all" by default 
  catalog_filter_list=catalog_filter_list, 
  database_filter_mode=database_filter_mode, ## "all" by default 
  database_filter_list = database_filter_list, 
  table_filter_mode=table_filter_mode, ## "all" by default 
  table_filter_list=table_filter_list,

"""

# COMMAND ----------

if start_over == "Yes":
  query_profiler.truncate_delta_optimizer_results()

# COMMAND ----------

# DBTITLE 1,Incrementally Load New Queries to Delta Table
query_profiler.load_query_history(dbx_token = DBX_TOKEN, mode='auto', lookback_period_days=lookbackPeriod)

# COMMAND ----------

# DBTITLE 1,Optional Create Performance Aggregates Out of that with this command
query_profiler.build_query_history_profile(dbx_token = DBX_TOKEN, mode='auto', lookback_period_days=lookbackPeriod)

# COMMAND ----------

# DBTITLE 1,Check Proper Partition Columns
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL main.delta_optimizer.raw_query_history_statistics
