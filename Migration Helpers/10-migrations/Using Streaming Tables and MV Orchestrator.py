# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## This library helps orchestrate Streaming tables in conjunction with other tables that may depend on synchronous updated from the streaming table for classical EDW loading patterns
# MAGIC  
# MAGIC ## Assumptions / Best Practices
# MAGIC
# MAGIC 1. Assumes ST is NOT SCHEDULED in the CREATE STATEMENT (externally orchestrated) (that is a different loading pattern that is not as common in classical EDW)
# MAGIC
# MAGIC 2. Assumes that one or many pipelines are dependent upon the successful CREATe OR REFRESH of the streaming table, so this library will simply block the tasks from moving the job onto the rest of the DAG to ensure the downstream tasks actually read from the table when it finishes updated
# MAGIC
# MAGIC 3. This works best with a single node "Driver" notebook loading sql files from Git similar to how airflow would orchestrate locally. The single job node would then call spark.sql() to run the CREATE OR REFRESH and then you arent needing a warehouse and a DLT pipeline in the job for streaming refreshes. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Library Steps 
# MAGIC
# MAGIC ### This library only takes in 1 sql statement at a time, this is because if there are multiple and only some pass and others fail, then it would not be correct failing or passing the whole statement. Each ST/MV must be done separately. This can be done by simply calling the static methods multiple times.
# MAGIC
# MAGIC 1. Parse Streaming Table / MV Create / Refresh commmand
# MAGIC 2. Identify ST / MV table(s) for that command
# MAGIC 3. Run SQL command - CREATE / REFRESH ST/MV
# MAGIC 4. DESCRIBE DETAIL to get pipelines.pipelineId metadata
# MAGIC 5. Perform REST API Call to check for in-progress Refreshes
# MAGIC 6. Poll and block statement chain from "finishing" until all pipelines identified are in either "PASS/FAIL"
# MAGIC 7. If statement PASSES - then complete and return
# MAGIC 8. If statement FAILS - then throw REFRESH FAIL exception

# COMMAND ----------

from helperfunctions.stmvorchestrator import orchestrate_stmv_statement

# COMMAND ----------

sql_statement = """
CREATE OR REFRESH STREAMING TABLE main.iot_dashboard.streaming_tables_raw_data
  AS SELECT 
      id::bigint AS Id,
      device_id::integer AS device_id,
      user_id::integer AS user_id,
      calories_burnt::decimal(10,2) AS calories_burnt, 
      miles_walked::decimal(10,2) AS miles_walked, 
      num_steps::decimal(10,2) AS num_steps, 
      timestamp::timestamp AS timestamp,
      value  AS value -- This is a JSON object
  FROM STREAM read_files('dbfs:/databricks-datasets/iot-stream/data-device/*.json*', 
  format => 'json',
  maxFilesPerTrigger => 12 -- what does this do when you
  )
"""

# COMMAND ----------

orchestrate_stmv_statement(spark, dbutils, sql_statement=sql_statement)
