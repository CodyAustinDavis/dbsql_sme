# Databricks notebook source
# MAGIC %run ./00-Config

# COMMAND ----------

# MAGIC %run ./01-Functions

# COMMAND ----------

# MAGIC %run ./02-Initialization

# COMMAND ----------

# DBTITLE 1,Imports
import requests
import time
import json
from datetime import date, datetime, timedelta
from pyspark.sql.functions import from_unixtime, lit, json_tuple, explode, current_date, current_timestamp
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Settings
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # needed for query history API
spark.sql("SET spark.databricks.delta.properties.defaults.minReaderVersion = 2") # needed for workspace API
spark.sql("SET spark.databricks.delta.properties.defaults.minWriterVersion = 5") # needed for workspace API
spark.sql("SET spark.databricks.delta.properties.defaults.columnMapping.mode = name") # needed for workspace API

# COMMAND ----------

# DBTITLE 1,Fetch from Warehouses API
response = requests.get(WAREHOUSE_URL, headers=AUTH_HEADER)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()

warehouses_json = response_json["warehouses"]

# A quirk in Python's and Spark's handling of JSON booleans requires us to converting True and False to true and false
boolean_keys_to_convert = set(get_boolean_keys(warehouses_json))

for warehouse_json in warehouses_json:
  for key in boolean_keys_to_convert:
    warehouse_json[key] = str(warehouse_json[key]).lower()

warehouses = spark.read.json(sc.parallelize(warehouses_json)).withColumn("snapshot_time",current_timestamp())

warehouses.write.format("delta").option("overwriteSchema", "true").mode("append").saveAsTable(DATABASE_NAME + "." + WAREHOUSES_TABLE_NAME)

# COMMAND ----------

# DBTITLE 1,Fetch from Query History API 
start_date = datetime.now() - timedelta(hours=NUM_HOURS_TO_UPDATE)
start_time_ms = start_date.timestamp() * 1000
end_time_ms = datetime.now().timestamp() * 1000

next_page_token = None
has_next_page = True
pages_fetched = 0

while (has_next_page and pages_fetched < MAX_PAGES_PER_RUN):
  print("Starting to fetch page " + str(pages_fetched))
  pages_fetched += 1
  if next_page_token:
    # Can't set filters after the first page
    request_parameters = {
      "max_results": MAX_RESULTS_PER_PAGE,
      "page_token": next_page_token,
      "include_metrics": True
    }
  else:
    request_parameters = {
      "max_results": MAX_RESULTS_PER_PAGE,
      "filter_by": {"query_start_time_range": {"start_time_ms": start_time_ms, "end_time_ms": end_time_ms}},
      "include_metrics": True
    }

  print ("Request parameters: " + str(request_parameters))
  
  response = requests.get(QUERIES_URL, headers=AUTH_HEADER, json=request_parameters)
  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()
  next_page_token = response_json["next_page_token"]
  has_next_page = response_json["has_next_page"]
  
  boolean_keys_to_convert = set(get_boolean_keys(response_json["res"]))
  for array_to_process in response_json["res"]:
    for key in boolean_keys_to_convert:
      array_to_process[key] = str(array_to_process[key]).lower()
    
# for 'include_metrics' you need to rewrite to pull out the nested structures. This is hella ugly, but it was the best I could do 
  df = spark.read.json(sc.parallelize([json.dumps(response_json)])).select(explode('res')).createOrReplaceTempView("df")
  query_results = spark.sql("""select col.*, col.metrics.* from df""")
  
  # For querying convience, add columns with the time in seconds instead of milliseconds
  query_results_clean = query_results \
    .withColumn("query_start_time", from_unixtime(query_results.query_start_time_ms / 1000)) \
    .withColumn("query_end_time", from_unixtime(query_results.query_end_time_ms / 1000))

  # The error_message column is not present in the REST API response when none of the queries failed.
  # In that case we add it as an empty column, since otherwise the Delta merge would fail in schema
  # validation
  if "error_message" not in query_results_clean.columns:
    query_results_clean = query_results_clean.withColumn("error_message", lit(""))
  
  if not check_table_exist(db_tbl_name="{0}.{1}".format(DATABASE_NAME, QUERIES_TABLE_NAME)):
    query_results_clean.write.format("delta").saveAsTable("{0}.{1}".format(DATABASE_NAME, QUERIES_TABLE_NAME)) 
  else:
    # Merge this page of results into the Delta table. Existing records that match on query_id have
    # all their fields updated (needed because the status, end time, and error may change), and new
    # records are inserted.
    queries_table = DeltaTable.forName(spark, "{0}.{1}".format(DATABASE_NAME, QUERIES_TABLE_NAME))
    queries_table.alias("queryResults").merge(
        query_results_clean.alias("newQueryResults"),
        "queryResults.query_id = newQueryResults.query_id") \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()

# COMMAND ----------

# DBTITLE 1,Fetch from Workflows API
base_url = f'{WORKFLOWS_URL}?expand_tasks=true&limit=25'
empty_response = {'has_more': False}

res = get_offset_result(base_url, 0, AUTH_HEADER)
data = [result_to_json(res)]

offest = 25

while res.json() != empty_response:
  res = get_offset_result(base_url, offest, AUTH_HEADER)
  data.append(result_to_json(res))
  print(f'Offset {offest} done')
  offest += 25

df = spark.read.json(sc.parallelize(data))

df_exploded = df.select(explode('jobs'))\
                .createOrReplaceTempView('df_exploded')

workflows_results = spark.sql('''select col.created_time, col.creator_user_name, col.job_id, col.settings.*, current_timestamp() as snapshot_time from df_exploded''')

if not check_table_exist(db_tbl_name="{0}.{1}".format(DATABASE_NAME, WORKFLOWS_TABLE_NAME)):
    workflows_results.write.format("delta").saveAsTable("{0}.{1}".format(DATABASE_NAME, WORKFLOWS_TABLE_NAME)) 
else:
    # Merge this page of results into the Delta table. Existing records that match on id have
    # all their fields updated (needed because the status, end time, and error may change), and new
    # records are inserted.
    workflows_table = DeltaTable.forName(spark, "{0}.{1}".format(DATABASE_NAME, WORKFLOWS_TABLE_NAME))
    workflows_table.alias("workflows").merge(
        workflows_results.alias("newWorkflows"),
        "workflows.job_id = newWorkflows.job_id") \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()

# COMMAND ----------

# DBTITLE 1,Fetch from Dashboards API 
## this is in preview and will be deprecated soon, so use at your own risk
## there is no next page token with this API so will need to iterate thorough all results until we get empty results

base_url = f'{DASHBOARDS_URL}/admin?page_size={PAGE_SIZE}'
empty_response = {'message': 'Page is out of range.'}

# initial request to set up the objects
res = get_page_result(base_url, 1, AUTH_HEADER)
data = [result_to_json(res)]

# from now, start at page 2 and incrememnt up in while loop
page = 2

while res.json() != empty_response:
    res = get_page_result(base_url, page, AUTH_HEADER)
    data.append(result_to_json(res))
    print(f'PAGE {page} done')
    page += 1

df = spark.read.json(sc.parallelize(data))

df_exploded = df.select(explode('results'))\
                .createOrReplaceTempView('df_exploded')

dashboards_results = spark.sql('''select col.* , current_timestamp() as snapshot_time from df_exploded''')

if not check_table_exist(db_tbl_name="{0}.{1}".format(DATABASE_NAME, DASHBOARDS_TABLE_NAME)):
    dashboards_results.write.format("delta").saveAsTable("{0}.{1}".format(DATABASE_NAME, DASHBOARDS_TABLE_NAME)) 
else:
    # Merge this page of results into the Delta table. Existing records that match on id have
    # all their fields updated (needed because the status, end time, and error may change), and new
    # records are inserted.
    dashboards_preview_table = DeltaTable.forName(spark, "{0}.{1}".format(DATABASE_NAME, DASHBOARDS_TABLE_NAME))
    dashboards_preview_table.alias("dasboards").merge(
        dashboards_results.alias("newDashboards"),
        "dasboards.id = newDashboards.id") \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
