# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Controller notebook
# MAGIC
# MAGIC Identifies and Orcestrates the sub jobs

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Step 1: Logic to get unique list of events/sub directories that separate the different streams
# Design considerations
# Ideally the writer of the raw data will separate out event types by folder so you can use globPathFilters to create separate streams
# If ALL events are in one data source, all streams will stream from 1 table and then will be filtered for that event in the stream. To avoid many file listings of the same file, enable useNotifications = true in autoloader

# COMMAND ----------

# DBTITLE 1,Define Params
dbutils.widgets.text("Input Root Path", "")
dbutils.widgets.text("Parent Job Name", "")
dbutils.widgets.text("Child Task Name", "")

# COMMAND ----------

# DBTITLE 1,Get Params
root_input_path = dbutils.widgets.get("Input Root Path")
parent_job_name = dbutils.widgets.get("Parent Job Name")
child_task_name = dbutils.widgets.get("Child Task Name")

print(f"Root input path: {root_input_path}")
print(f"Parent Job Name: {parent_job_name}")
print(f"Event Task Name: {child_task_name}")

# COMMAND ----------

# DBTITLE 1,Define Dynamic Checkpoint Path
## Eeach stream needs its own checkpoint, we can dynamically define that for each event/table we want to create / teast out

checkpoint_path = f"dbfs:/checkpoints/<your_user_id_here>/{parent_job_name}/{child_task_name}/"

# COMMAND ----------

# DBTITLE 1,Target Location Definitions
spark.sql("""CREATE DATABASE IF NOT EXISTS iot_multiplexing_demo""")

# COMMAND ----------

# DBTITLE 1,Use Whatever custom event filtering logic is needed
filter_regex_string = "part-" + child_task_name + "*.json*"

print(filter_regex_string)

# COMMAND ----------

# DBTITLE 1,Read Stream
input_df = (spark
  .readStream
  .format("text")
  .option("multiLine", "true")
  .option("pathGlobFilter", filter_regex_string)
  .load(root_input_path)
  .withColumn("inputFileName", input_file_name()) ## you can filter using .option("globPathFilter") as well here
)

# COMMAND ----------

# DBTITLE 1,Transformation Logic on any events (can be conditional on event)
transformed_df = (input_df
  .withColumn("EventName", lit(child_task_name))
  .selectExpr("value:id::integer AS Id", 
              "EventName",
              "value:user_id::integer AS UserId",
              "value:device_id::integer AS DeviceId",
              "value:num_steps::decimal AS NumberOfSteps",
              "value:miles_walked::decimal AS MilesWalked",
              "value:calories_burnt::decimal AS Calories",
              "value:timestamp::timestamp AS EventTimestamp",
              "current_timestamp() AS IngestionTimestamp",
              "inputFileName")

)

# COMMAND ----------

# DBTITLE 1,Truncate this child stream and reload from all data

dbutils.fs.rm(checkpoint_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Dynamic Write Stream
(transformed_df
  .writeStream
  .trigger(once=True)
  .option("checkpointLocation", checkpoint_path)
  .toTable(f"iot_multiplexing_demo.iot_stream_event_{child_task_name}")
)
