# Databricks notebook source
import datetime, dateutil
import sys, os
import json
#import dbutils
import time


# COMMAND ----------

sys.path.append(f"{os.getcwd()}/../src")
#sys.path

# COMMAND ----------

import dbsql_query_history_sync.queries_api as queries_api
import dbsql_query_history_sync.delta_sync as delta_sync

# COMMAND ----------

import importlib

# COMMAND ----------

importlib.reload(queries_api)
importlib.reload(delta_sync)

# COMMAND ----------

# replace as required
workspace_url = 'e2-demo-field-eng.cloud.databricks.com'
warehouse_ids_list = ['475b94ddc7cd5211',]

# COMMAND ----------

# Replace as required
DBX_TOKEN = dbutils.secrets.get(scope='nishant-deshpande', key='dbsql-api-key')  # subst your scope + key to query the API


# COMMAND ----------

# Adjust the history period as required.
dt = datetime.datetime.now() - datetime.timedelta(minutes=5)
#dt = datetime.datetime.now() - datetime.timedelta(hours=1)
print(dt)
dt_ts = int(dt.timestamp() * 1000)
print(dt_ts)

# COMMAND ----------

# get queries as a list
x = queries_api.get_query_history(
  dbx_token=DBX_TOKEN, 
  workspace_url=workspace_url, warehouse_ids=warehouse_ids_list, start_ts_ms=dt_ts, end_ts_ms=None, user_ids=None, statuses=None, stop_fetch_limit=1000)

# COMMAND ----------

t_ts = int(datetime.datetime.now().timestamp())
sync_table = f'default.query_history_test_{t_ts}'  # change to your preferred table name.
print(sync_table)

# COMMAND ----------

# create the object
udbq = delta_sync.UpdateDBQueries(spark_session=spark, dbx_token=DBX_TOKEN, workspace_url=workspace_url,
                       warehouse_ids=warehouse_ids_list, earliest_query_ts_ms=dt_ts, table_name=sync_table)

# COMMAND ----------

# This updates the table with the query history one time
udbq.update_db()

# COMMAND ----------

# Check the table
display(spark.sql(f"""
select count(1), timestamp(min(query_start_time_ms)/1000), timestamp(max(query_start_time_ms)/1000)
from {sync_table}
"""))

# COMMAND ----------



# COMMAND ----------

# This will update the underlying table incrementally every 10 seconds.
udbq.update_db_repeat(interval_secs=10)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1), timestamp(min(query_start_time_ms)/1000), timestamp(max(query_start_time_ms)/1000)
# MAGIC from default.query_history_test_1695014139  -- update the table name to new table created above

# COMMAND ----------


