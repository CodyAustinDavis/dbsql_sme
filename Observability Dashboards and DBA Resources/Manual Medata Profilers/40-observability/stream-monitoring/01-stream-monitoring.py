# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ### Streaming Metrics Analysis
# MAGIC This notebook demonstrates how you can capture streaming metrics into JSON files
# MAGIC
# MAGIC Cluster Setup:
# MAGIC * This notebook has been tested with DBR 11.0 

# COMMAND ----------

# Import required packages

import os
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
from datetime import datetime

# COMMAND ----------

fmt = '%Y-%m-%d %H:%M:%S'
datetime.now().strftime(fmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Setup Source Stream

# COMMAND ----------

sourcePath = "/databricks-datasets/definitive-guide/data/activity-data/"

# COMMAND ----------

static = spark.read.json(sourcePath+"part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json")
dataSchema = static.schema

# COMMAND ----------

static.printSchema()

# COMMAND ----------

deltaStreamDF = (spark
                  .readStream
                  .format("delta")
                  .option("maxFilesPerTrigger", 1)
                  .schema(dataSchema)
                  .json(sourcePath)
                  .withColumnRenamed('Index', 'User_ID')
                  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Setup Monitoring

# COMMAND ----------

# SETUP THE TARGET LOCATION FOR STREAMING LOGS
streaming_logs_path = '/tmp/streaming1'

# COMMAND ----------

# Delete any files if they already exist in this path
dbutils.fs.rm(streaming_logs_path,True)

# Create the Target path for the logs
dbutils.fs.mkdirs(streaming_logs_path)

# COMMAND ----------

# SETUP THE FUNCTION TO WRITE STREAMING LOGS IN JSON FORMAT
# The process writes files to your target path set above
# Please ensure your target path doesn't already have /dbfs

from pyspark.sql.streaming import StreamingQueryListener

timestamp_fmt = '%Y_%m_%d_%H_%M_%S'

def logEvent(event):
 timestamp_string = datetime.now().strftime(timestamp_fmt)
 with open('/dbfs'+streaming_logs_path+'/stream_metrics_'+timestamp_string+'.json', 'w') as f:
   json.dump(json.JSONDecoder().decode(event), f)

# COMMAND ----------

# Test the monitoring logs
test_event = """{
  "id" : "bfda9930-7206-46e4-ba51-e15730d0f689",
  "runId" : "336b3055-dbc8-4d17-b445-b9a127d05461",
  "name" : "acitivty_streaming",
  "timestamp" : "2022-06-01T21:40:25.902Z",
  "batchId" : 2,
  "numInputRows" : 156024,
  "inputRowsPerSecond" : 16708.502891411437,
  "processedRowsPerSecond" : 17515.04265828469,
  "durationMs" : {
    "addBatch" : 8055,
    "commitOffsets" : 250,
    "getBatch" : 273,
    "latestOffset" : 156,
    "queryPlanning" : 8,
    "triggerExecution" : 8908,
    "walCommit" : 164
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "FileStreamSource[dbfs:/databricks-datasets/definitive-guide/data/activity-data]",
    "startOffset" : {
      "logOffset" : 1
    },
    "endOffset" : {
      "logOffset" : 2
    },
    "latestOffset" : null,
    "numInputRows" : 156024,
    "inputRowsPerSecond" : 16708.502891411437,
    "processedRowsPerSecond" : 17515.04265828469
  } ],
  "sink" : {
    "description" : "ForeachBatchSink",
    "numOutputRows" : -1
  }
}"""

logEvent(test_event)
display(dbutils.fs.ls(streaming_logs_path))

# COMMAND ----------

# SETUP THE STREAM QUERY LISTNER

from pyspark.sql.streaming import StreamingQueryListener


class ActivityStreamListener(StreamingQueryListener):
    def onQueryStarted(self, event):

        print("Streaming Query Started")

    def onQueryProgress(self, event):

        logEvent(event.progress.prettyJson)
        #print("Streaming Query Running")

    def onQueryTerminated(self, event):

        print("Streaming Query Terminated")


activity_listener = ActivityStreamListener()
    

# COMMAND ----------

# ADD THE LISTNER TO THE SPARK CONTEXT
spark.streams.addListener(activity_listener)
#spark.streams.removeListener(activity_listener)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Run Streaming Process

# COMMAND ----------

display(deltaStreamDF)

# COMMAND ----------

counter = 0
sqm = spark.streams
for q in sqm.active:
  counter = counter + 1
  print("Stream number:"+str(counter)+" with ID :"+q.id+" and Status: "+str(q.status))

# COMMAND ----------

display(dbutils.fs.ls(streaming_logs_path))

# COMMAND ----------

# Copy the test event and create json file out of it and capture the schema

dbutils.fs.put("/tmp/source.json", test_event, True)

source_df = spark.read.option("multiline", "true").json("/tmp/source.json")

jsonSchema = source_df.schema

# COMMAND ----------

# Create a temp table with the stream metrics
df = spark.read.option("multiline","true").json(streaming_logs_path)
df.createOrReplaceTempView("streaming_metrics")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,count(*) from streaming_metrics group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,timestamp,round(durationMs.triggerExecution/1000) as batch_duration,round(inputRowsPerSecond) input_rate,
# MAGIC round(processedRowsPerSecond) process_rate from streaming_metrics order by 1,2
