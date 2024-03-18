# Databricks notebook source
# DBTITLE 1,API Config
# Please ensure the url starts with https and DOES NOT have a slash at the end
WORKSPACE_HOST = 'https://adb-2541733722036151.11.azuredatabricks.net'
WAREHOUSE_URL = "{0}/api/2.0/sql/warehouses".format(WORKSPACE_HOST) ## SQL Warehouses APIs 2.0
QUERIES_URL = "{0}/api/2.0/sql/history/queries".format(WORKSPACE_HOST) ## Query History API 2.0 
WORKFLOWS_URL = "{0}/api/2.1/jobs/list".format(WORKSPACE_HOST) ## Jobs & Workflows History API 2.1 
DASHBOARDS_URL = "{0}/api/2.0/preview/sql/queries".format(WORKSPACE_HOST,250) ## Queries and Dashboards API - ❗️in preview, deprecated soon❗️

MAX_RESULTS_PER_PAGE = 1000
MAX_PAGES_PER_RUN = 500
PAGE_SIZE = 250 # 250 is the max

# We will fetch all queries that were started between this number of hours ago, and now()
# Queries that are running for longer than this will not be updated.
# Can be set to a much higher number when backfilling data, for example when this Job didn't run for a while.
NUM_HOURS_TO_UPDATE = 168

# COMMAND ----------

# DBTITLE 1,API Authentication
# If you want to run this notebook yourself, you need to create a Databricks personal access token,
# store it using our secrets API, and pass it in through the Spark config, such as this:
# spark.pat_token {{secrets/query_history_etl/user}}, or Azure Keyvault.

#Databricks secrets API
#AUTH_HEADER = {"Authorization" : "Bearer " + spark.conf.get("spark.pat_token")}
#Azure KeyVault
#AUTH_HEADER = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")}
#Naughty way
AUTH_HEADER = {"Authorization" : "Bearer " + "dapixxxxxxxxxxxxxxxxxxxxxxxxx"}

# COMMAND ----------

# DBTITLE 1,Database and Table Config
DATABASE_NAME = "dbsql_logging"
# DATABASE_LOCATION = "/s3-location/"
QUERIES_TABLE_NAME = "queries"
WAREHOUSES_TABLE_NAME = "warehouses"
WORKFLOWS_TABLE_NAME = "workflows"
DASHBOARDS_TABLE_NAME = "dashboards_preview"

# COMMAND ----------

# DBTITLE 1,Delta Table Maintenance
QUERIES_ZORDER = "endpoint_id"
WAREHOUSES_ZORDER = "id"
WORKFLOWS_ZORDER = "job_id"
DASHBOARDS_ZORDER = "id"

VACUUM_RETENTION = 168
