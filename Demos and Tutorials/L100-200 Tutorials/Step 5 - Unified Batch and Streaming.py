# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # This notebook generates a full data pipeline from databricks dataset - iot-stream using Autoloader + Structured Streaming
# MAGIC
# MAGIC ## This creates 2 tables: 
# MAGIC
# MAGIC <b> Database: </b> iot_dashboard
# MAGIC
# MAGIC <b> Tables: </b> silver_sensors, silver_users 
# MAGIC
# MAGIC <b> Params: </b> StartOver (Yes/No) - allows user to truncate and reload pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg" >

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Autoloader Benefits: 
# MAGIC
# MAGIC 1. More Scalable Directory Listing (incremental + file notification)
# MAGIC 2. Rocks DB State Store - Faster State management
# MAGIC 3. Schema Inference + Merge Schema: https://docs.databricks.com/ingestion/auto-loader/schema.html
# MAGIC 4. File Notification Mode - For ultra high file volumes from S3/ADLS/GCS: https://docs.databricks.com/ingestion/auto-loader/file-detection-modes.html
# MAGIC 5. Complex Sources -- advanced Glob Path Filters: <b> .option("pathGlobFilter", "[a-zA-Z].csv") </b> 
# MAGIC 6. Rescue data - automatically insert "bad" data into a rescued data column so you never lose data <b> .option("cloudFiles.rescuedDataColumn", "_rescued_data")  </b>
# MAGIC 7: Flexible Schema Hints: <b> .option("cloudFiles.schemaHints", "tags map<string,string>, version int") </b> 
# MAGIC
# MAGIC Much more!
# MAGIC
# MAGIC ### Auto loader intro: 
# MAGIC https://docs.databricks.com/ingestion/auto-loader/index.html
# MAGIC
# MAGIC Rescue Data: https://docs.databricks.com/ingestion/auto-loader/schema.html#rescue
# MAGIC
# MAGIC
# MAGIC ### Auto Loader Full Options: 
# MAGIC
# MAGIC https://docs.databricks.com/ingestion/auto-loader/options.html
# MAGIC   
# MAGIC ## Meta data options: 
# MAGIC
# MAGIC Load the file metadata in auto loader for downstream continuity
# MAGIC   https://docs.databricks.com/ingestion/file-metadata-column.html
# MAGIC   

# COMMAND ----------


file_source_location = "dbfs:/databricks-datasets/iot-stream/data-device/"
checkpoint_location = f"dbfs:/FileStore/shared_uploads/intro_design_patterns/IotDemoCheckpoints/AutoloaderDemo/bronze"
checkpoint_location_silver = f"dbfs:/FileStore/shared_uploads/intro_design_patterns/IotDemoCheckpoints/AutoloaderDemo/silver"
autoloader_schema_location = f"dbfs:/FileStore/shared_uploads/intro_design_patterns/IotDemoCheckpoints/AutoloaderDemoSchema/"

# COMMAND ----------

# DBTITLE 1,Look at Raw Data Source

dbutils.fs.ls('dbfs:/databricks-datasets/iot-stream/data-device/')

# COMMAND ----------

# DBTITLE 1,Imports

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS iot_dashboard_autoloader
# MAGIC --LOCATION 's3a://<path>' or 'adls://<path>'

# COMMAND ----------

# DBTITLE 1,Start Over
# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS iot_dashboard_autoloader.bronze_sensors;

# COMMAND ----------

# DBTITLE 1,Let Autoloader Sample files for schema inference - backfill interval can refresh this inference
## for schema inference
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 100) ## 1000 is default

# COMMAND ----------

# DBTITLE 1,Read Stream with Autoloader - LOTS of Options for any type of data
df_raw = (spark
     .readStream
     .format("cloudFiles") ## csv, json, binary, text, parquet, avro
     .option("cloudFiles.format", "json")
     #.option("cloudFiles.useNotifications", "true")
     .option("cloudFiles.schemaLocation", autoloader_schema_location)
     #.option("schema", inputSchema)
     #.option("modifiedAfter", timestampString) ## option
     .option("cloudFiles.schemaHints", "calories_burnt FLOAT, timestamp TIMESTAMP")
     .option("cloudFiles.maxFilesPerTrigger", 10) ## maxBytesPerTrigger, 10mb
     .option("pathGlobFilter", "*.json.gz") ## Only certain files ## regex expr
     .option("ignoreChanges", "true")
     #.option("ignoreDeletes", "true")
     .load(file_source_location)
     #.select("*", "_metadata") ##_metadata exits with DBR 11.0 + 
     .withColumn("InputFileName", input_file_name())
    )

# COMMAND ----------

# DBTITLE 1,Display a Stream for testing
display(df_raw)

# COMMAND ----------

# DBTITLE 1,Delete Checkpoint Location to start over
dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Stream -- same as any other stream
(df_raw
.writeStream
.format("delta")
.option("checkpointLocation", checkpoint_location)
.trigger(availableNow=True) ## once=True, processingTime = '5 minutes', continuous ='1 minute'
.toTable("iot_dashboard_autoloader.bronze_sensors") ## We do not need to define the DDL, it will be created on write, but we can define DDL if we want to
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE iot_dashboard_autoloader.bronze_sensors ZORDER BY (time_stamp, tweet_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC value,
# MAGIC value:user_id AS unique_id,
# MAGIC value:user_idskdjfhsdk AS unique_id,
# MAGIC value:time_stamp::timestamp AS tweet_ts
# MAGIC FROM iot_dashboard_autoloader.bronze_sensors

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Now we have data streaming into a bronze table at any clip/rate we want. 
# MAGIC #### How can we stream into a silver table with merge?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <b> Stream options from a delta table: </b> https://docs.databricks.com/delta/delta-streaming.html
# MAGIC
# MAGIC
# MAGIC <li> <b> 1. Limit throughput rate: </b>  https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate
# MAGIC <li> <b> 2. Specify starting version or timestamp: </b>  https://docs.databricks.com/delta/delta-streaming.html#specify-initial-position
# MAGIC <li> <b> 3. Ignore updates/deletes: </b>  https://docs.databricks.com/delta/delta-streaming.html#ignore-updates-and-deletes

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_dashboard_autoloader.bronze_sensors;

# COMMAND ----------

# DBTITLE 1,Stream with Delta options
df_bronze = (
  spark.readStream
#.option("startingVersion", "1") ## Or .option("startingTimestamp", "2018-10-18") You can optionally explicitly state which version to start streaming from
.option("ignoreChanges", "true") ## .option("ignoreDeletes", "true")
#.option("useChangeFeed", "true")
.option("maxFilesPerTrigger", 100) ## Optional - FIFO processing
.table("iot_dashboard_autoloader.bronze_sensors")
)

#display(df_bronze)


# COMMAND ----------

# DBTITLE 1,Create Target Silver Table for Merge
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE iot_dashboard_autoloader.silver_sensors
# MAGIC AS SELECT * FROM iot_dashboard_autoloader.bronze_sensors WHERE 1=2;

# COMMAND ----------

# DBTITLE 1,Define function to run on each microBatch (every 1s - every day, anything)
from delta.tables import *
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

def mergeStatementForMicroBatch(microBatchDf, microBatchId):
  
  """
  #### Python way
  silverDeltaTable = DeltaTable.forName(spark, "iot_dashboard_autoloader.silver_sensors")
  
  ## Delete duplicates in source if there are any 
  
  updatesDeDupedInMicroBatch = (microBatchDf
                                .select("*", 
                                   F.row_number().over(Window.partitionBy("Id", "user_id", "device_id", "timestamp").orderBy("timestamp")).alias("dupRank")
                                       )
                                .filter(col("dupRank") == lit("1")) ## Get only 1 most recent copy per row
                               ) ## partition on natural key or pk for dups within a microBatch if there are any
  
  (silverDeltaTable.alias("target")
  .merge(
    updatesDeDupedInMicroBatch.distinct().alias("updates"),
    "target.Id = updates.Id AND updates.user_id = target.user_id AND target.device_id = updates.device_id"
        )
   .whenMatchedUpdate(set =
    {
      "calories_burnt": "updates.calories_burnt",
      "miles_walked": "updates.miles_walked",
      "num_steps": "updates.num_steps",
      "timestamp": "updates.timestamp"
    }
  )
  .whenNotMatchedInsertAll()
  .execute()
  )
  """
  ### SQL Way to do it inside the micro batch
  
  ## Register microbatch in SQL temp table and run merge using spark.sql
  microBatchDf.createOrReplaceGlobalTempView("updates_df")
  
  spark.sql("""
  
  MERGE INTO iot_dashboard.silver_sensors AS target
  USING (
         SELECT Id::integer,
                device_id::integer,
                user_id::integer,
                calories_burnt::decimal,
                miles_walked::decimal,
                num_steps::decimal,
                timestamp::timestamp,
                value::string
         FROM (
           SELECT *,
           ROW_NUMBER() OVER(PARTITION BY Id, user_id, device_id, timestamp ORDER BY timestamp DESC) AS DupRank
           FROM global_temp.updates_df
             )
         WHERE DupRank = 1
         )
                AS source
  ON source.Id = target.Id
  AND source.user_id = target.user_id
  AND source.device_id = target.device_id
  AND source.timestamp > target.timestamp -- ensures any old data passed through because of ignore changes option is filtered out and does not update data needlessly
  WHEN MATCHED THEN UPDATE SET 
    target.calories_burnt = source.calories_burnt,
    target.miles_walked = source.miles_walked,
    target.num_steps = source.num_steps,
    target.timestamp = source.timestamp
  WHEN NOT MATCHED THEN INSERT *;
  """)
  
  ## optimize table after the merge for faster queries
  spark.sql("""OPTIMIZE iot_dashboard_autoloader.silver_sensors ZORDER BY (timestamp, device_id, user_id)""")
  
  return

# COMMAND ----------

# DBTITLE 1,Delete checkpoint - each stream has 1 checkpoint
dbutils.fs.rm(checkpoint_location_silver, recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Stream as often as you want
(df_bronze
.writeStream
.option("checkpointLocation", checkpoint_location_silver)
.trigger(processingTime='3 seconds') ## processingTime='1 minute' -- Now we can run this merge every minute!
.foreachBatch(mergeStatementForMicroBatch)
.start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM iot_dashboard.silver_sensors;
