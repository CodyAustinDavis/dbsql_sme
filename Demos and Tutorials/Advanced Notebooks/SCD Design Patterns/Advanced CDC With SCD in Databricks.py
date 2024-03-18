# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Efficiently Managing Incoming Updates for Highly Scalable Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/06/How-to-Simplify-CDC-with-Delta-Lakes-Change-Data-Feed-blog-image6.jpg">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read in Raw Data into Bronze Delta Table -- Append Only

# COMMAND ----------

# DBTITLE 1,Start From Scratch
# MAGIC %sql
# MAGIC
# MAGIC DROP DATABASE IF EXISTS hls_cdc_delta_workshop CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS hls_cdc_delta_workshop;
# MAGIC USE hls_cdc_delta_workshop;

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random, string, uuid
from delta.tables import *
## For generating new Ids - In HLS Data - often need to manage creation of new Ids with source system ids and/or natural keys
uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# DBTITLE 1,Define Checkpoints for Streaming
bronze_silver_scd1_checkpoint = "/cody/hlsdemo/checkpoints/bronze_silver_scd1_checkpoint/"
bronze_silver_scd2_checkpoint = "/cody/hlsdemo/checkpoints/bronze_silver_scd2_checkpoint/"

# COMMAND ----------

dbutils.fs.rm(bronze_silver_scd1_checkpoint, recurse=True)
dbutils.fs.rm(bronze_silver_scd2_checkpoint, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Raw --> Bronze: Append Only All Updates

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ! Focusing on Patient Data - but these applies to almost ALL data sources and downstream analytics to run efficiently !

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/rwe/ehr/csv')

# COMMAND ----------

# DBTITLE 1,Read in of Raw Data
#dbutils.fs.ls('dbfs:/databricks-datasets/rwe/ehr/csv/patients.csv/')
raw_patient_data_stream = spark.read.format("csv").option("header", "true").load('dbfs:/databricks-datasets/rwe/ehr/csv/patients.csv')

# COMMAND ----------

# DBTITLE 1,Do ETL Logic - Filter out bad Data (Delta Live Tables)
df_raw_bronze = (raw_patient_data_stream.withColumn("IngestTimestamp", current_timestamp())
                 .filter(col("LAST").isNotNull() & col("FIRST").isNotNull() & col("SSN").isNotNull())
                )

# COMMAND ----------

# DBTITLE 1,View Incoming Data
display(df_raw_bronze)

# COMMAND ----------

# DBTITLE 1,Write to Table - Append Only in Raw to Bronze
(df_raw_bronze
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("BronzePatientUpdates")
)

# COMMAND ----------

# DBTITLE 1,Query Bronze Data
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM BronzePatientUpdates ORDER BY IngestTimestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze --> Silver (1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Bronze to Silver - SCD Type 1 With Delta CDF (What is the difference?)

# COMMAND ----------

# DBTITLE 1,Create Silver Stage if not Exists
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS PatientRecords_Silver_SCDTypeOne
# MAGIC (
# MAGIC Id STRING NOT NULL,
# MAGIC BIRTHDATE DATE, 
# MAGIC DEATHDATE DATE, 
# MAGIC SSN STRING,
# MAGIC DRIVERS STRING,
# MAGIC PASSPORT STRING, 
# MAGIC PREFIX STRING, 
# MAGIC FIRST STRING, 
# MAGIC LAST STRING NOT NULL, 
# MAGIC SUFFIX STRING,
# MAGIC MAIDEN STRING,
# MAGIC MARITAL STRING,
# MAGIC RACE STRING,
# MAGIC ETHNICITY STRING,
# MAGIC GENDER STRING,
# MAGIC BIRTHPLACE STRING,
# MAGIC ADDRESS STRING,
# MAGIC CITY STRING,
# MAGIC STATE STRING, 
# MAGIC ZIP STRING,
# MAGIC IngestTimestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC --LOCATION <file_path>
# MAGIC --PARTITIONED BY (cols)
# MAGIC ;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS PatientRecords_Silver_SCDTypeTwo
# MAGIC (
# MAGIC Id STRING NOT NULL,
# MAGIC BIRTHDATE DATE, 
# MAGIC DEATHDATE DATE, 
# MAGIC SSN STRING,
# MAGIC EffectiveDate TIMESTAMP, --SCD Type 2 
# MAGIC EndDate TIMESTAMP, --SCD Type 2 
# MAGIC IsCurrent BOOLEAN, --SCD Type 2 
# MAGIC DRIVERS STRING,
# MAGIC PASSPORT STRING, 
# MAGIC PREFIX STRING, 
# MAGIC FIRST STRING, 
# MAGIC LAST STRING NOT NULL, 
# MAGIC SUFFIX STRING,
# MAGIC MAIDEN STRING,
# MAGIC MARITAL STRING,
# MAGIC RACE STRING,
# MAGIC ETHNICITY STRING,
# MAGIC GENDER STRING,
# MAGIC BIRTHPLACE STRING,
# MAGIC ADDRESS STRING,
# MAGIC CITY STRING,
# MAGIC STATE STRING, 
# MAGIC ZIP STRING,
# MAGIC IngestTimestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC --LOCATION <file_path>
# MAGIC --PARTITIONED BY (cols)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Perform Initial Upsert  From Bronze --> Silver in streaming
bronze_df = spark.readStream.table("BronzePatientUpdates")

# COMMAND ----------

# DBTITLE 1,Define micro batch merge function for stream
def mergeFunctionPython(updatesDf, microBatchId):
  
  silver_table = DeltaTable.forName(spark, "PatientRecords_Silver_SCDTypeOne")
  
  ## We can do any merge logic here in pyspark (more in SCD Type 2 version)
  (silver_table.alias("target")
  .merge(updatesDf.alias("source"), "source.Id = target.Id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
  )
  
  return
  

# COMMAND ----------

# DBTITLE 1,Can Stream Merge Statement in SQL too!
def mergeFunctionSQL(updatesDf, microBatchId):
  
  silver_table = DeltaTable.forName(spark, "PatientRecords_Silver_SCDTypeOne")
  ## You can also just use the spakr content from updatesDf
  updatesDf.createOrReplaceGlobalTempView("updatesDf_bronze_silver_scd_1")
  
  spark.sql("""
  MERGE INTO PatientRecords_Silver_SCDTypeOne AS target
  USING global_temp.updatesDf_bronze_silver_scd_1 AS source
  ON source.Id = target.Id
  WHEN MATCHED UPDATE *
  WHEN NOT MATCHED INSERT *
  """)
  
  return

# COMMAND ----------

# DBTITLE 1,Write Stream and Perform Upsert
(bronze_df
.writeStream
.trigger(once=True) ## processingTime = '1 minute', availableNow = True
.option("checkpointLocation", bronze_silver_scd1_checkpoint)
.foreachBatch(mergeFunctionPython) #mergeFunctionSQL
.start()
)

# COMMAND ----------

# DBTITLE 1,Check out Silver Table
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PatientRecords_Silver_SCDTypeOne ORDER BY IngestTimestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## When to use Delta CDF
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/06/Small-fraction-of-records-updated-in-each-batch-blog-img-12.jpg">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Silver Updates with CDF

# COMMAND ----------

# DBTITLE 1,Enable CDF For All Tables on the Cluster
#set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed From Bronze Table of All Updates
# MAGIC %sql
# MAGIC
# MAGIC --We can also do this globally or at a cluster level for certain whole pipelines/jobs (also available in python api/scala)
# MAGIC ALTER TABLE BronzePatientUpdates SET TBLPROPERTIES(delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE PatientRecords_Silver_SCDTypeOne SET TBLPROPERTIES(delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE PatientRecords_Silver_SCDTypeTwo SET TBLPROPERTIES(delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,See Table Changes is empty (if ran linearly from the top)
# MAGIC %sql
# MAGIC
# MAGIC SELECt * FROM table_changes('BronzePatientUpdates', 1)

# COMMAND ----------

# DBTITLE 1,Simulate New Patient - With Incorrect Record (bad SSN - EXTREMELY expensive for downstream analytics - explodes graph)
df_new_patient = (sc.parallelize([{"Id": "cccccccccc-6cc4-48a8-a0b1-99999999ccccc", 
                                  "BIRTHDATE":"1991-12-01", 
                                  "DEATHDATE":"null", 
                                  "SSN":"9999-99-999",
                                  "DRIVERS":"33304040",
                                  "PASSPORT": "asdklhdfg", 
                                  "PREFIX": "Dr.", 
                                  "FIRST":"CODY", 
                                  "LAST":"Davis", 
                                  "SUFFIX":"null",
                                  "MAIDEN":"null",
                                  "MARITAL":"M",
                                  "RACE":"white",
                                  "ETHNICITY":"american",
                                  "GENDER":"M",
                                  "BIRTHPLACE":"Somewhere in Texas",
                                  "ADDRESS":"1308 Misty Meadow Dr.",
                                  "CITY":"FORT WORTH",
                                  "STATE":"TX", 
                                  "ZIP": "76133"}]).toDF()
                 ).withColumn("IngestTimestamp", current_timestamp())


df_new_patient.createOrReplaceTempView("patient_updates")

# COMMAND ----------

# DBTITLE 1,Simulate Raw --> Bronze Append Update for a new patient record
df_new_patient.write.format("delta").mode("append").saveAsTable("BronzePatientUpdates")

# COMMAND ----------

# DBTITLE 1,Dynamically Get Table Version -- To dynamically control CDF logic
## Get oldest version where pipeline was last run
## you can look for the Id of your job to get oldest version since was last run
version_df_bronze = int(spark.sql("""DESCRIBE HISTORY BronzePatientUpdates""").alias("version")
                     .selectExpr("*", "notebook.notebookId AS notebookId")
                     .filter((col("operation") == lit("WRITE")))
                     .agg(min("version").alias("startVersion"))
                     .collect()[0][0]
                    )

print(int(version_df_bronze))

# COMMAND ----------

# DBTITLE 1,Look at New Patient Record in BronzePatientUpdates

#Notice data columns out of order
#SSN is bad - really detrimental for MPI and downstream analytics
#The bad record Id: cccccccccc-6cc4-48a8-a0b1-99999999ccccc

tbl_changes = spark.sql(f"""SELECT * FROM table_changes('BronzePatientUpdates', {version_df_bronze})""")

display(tbl_changes)

# COMMAND ----------

# DBTITLE 1,Read new record from the Stream
## .option("readChangeFeed", "true") can be used if you simple want to inner join on the least changes or do a simple inner join on any previous version
## Can do any logic here such as windowing to get most recent record across multiple version, aggregates, etc. 
## You can use CDF inside or outside the microBatch, but not both. If possible, its always better to do it as upstream as possible to minimize data movement.

bronze_df = (spark
             .readStream
             .format("delta")
             .option("readChangeFeed", "true")
             #.option("startingVersion", 2) ## ! comment out to see the difference in data volumes the stream will merge (1 vs thousands)
             .table("BronzePatientUpdates")
            )

#display(bronze_df)

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY BronzePatientUpdates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze --> Silver: Merge Updates efficiently with Type 1 SCD (the above stream + CDC)

# COMMAND ----------

## With CDF
def mergeCDFFunctionPython(updates, microBatchId):
  
  silver_table = DeltaTable.forName(spark, "PatientRecords_Silver_SCDTypeOne")
  
  ## Get a new starting version for each microbatch (can get all the versions even made by other jobs != this one since this one last wrote)
  ## We can do any merge logic here in pyspark (more in SCD Type 2 version)
  (silver_table.alias("target")
  .merge(updates.option("readChangeFeed", "true").option("startVersion", 2).alias("source"),
         "source.Id = target.Id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
  )
  
  return

# COMMAND ----------

## With CDF
def mergeCDFFunctionSQL(updatesDf, microBatchId):
  
  silver_table = DeltaTable.forName(spark, "PatientRecords_Silver_SCDTypeOne")
  
  updatesDf.createOrReplaceGlobalTempView("updatesDf_bronze_silver_scd_1")
  
  ## In produciton, you can choose and manage the version of table changes you want to get
  ## Can do any logic here such as windowing to get most recent record across multiple version, aggregates, etc. 
  
  spark.sql("""
  MERGE INTO PatientRecords_Silver_SCDTypeOne AS target
  USING ( SELECT 
         raw_source.*
         FROM global_temp.updatesDf_bronze_silver_scd_1 AS raw_source
         INNER JOIN table_changes('BronzePatientUpdates', 2) AS cdfUpdates ON cdfUpdates.Id = raw_source.Id
         ) AS source
  ON source.Id = target.Id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  """)
  
  return

# COMMAND ----------

# DBTITLE 1,UPSERT WITH SQL/Pyspark - Type 1 SCD + CDF
## Get inner join of row-level change data I want to apply the more expensive merge operation on
## In production, you can decide and manage what version of the change_Data feed you want usually most recent version or table changes as of a timestamp

(
 bronze_df
.writeStream
.trigger(once=True) ## processingTime = '1 minute', availableNow = True
.option("checkpointLocation", bronze_silver_scd1_checkpoint)
.foreachBatch(mergeCDFFunctionSQL) #mergeCDFFunctionPython
.start()
)

# COMMAND ----------

# DBTITLE 1,Check out final Table!
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PatientRecords_Silver_SCDTypeOne ORDER BY IngestTimestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SCD Type 2 Changes in Delta + Streaming! (Photon is a plus here)

# COMMAND ----------

# DBTITLE 1,Track Version History for a Patient (For MPI, Auditing, Bad Updates, etc.) - SCD Type 2 SDC + Delta CDF
## With CDF
def mergeSCD2FunctionSQL(updatesDf, microBatchId):
  
  silver_table = DeltaTable.forName(spark, "PatientRecords_Silver_SCDTypeTwo")
  
  updatesDf.createOrReplaceGlobalTempView("updatesDf_bronze_silver_scd_2")
  
  ### Perform the merge logic
  spark.sql("""
  
  MERGE INTO PatientRecords_Silver_SCDTypeTwo AS target
  USING ( 
    
      WITH updatesCdf AS (
          SELECT raw_source.*
          FROM global_temp.updatesDf_bronze_silver_scd_2 AS raw_source
          --INNER JOIN table_changes('BronzePatientUpdates', 3) AS cdfUpdates ON cdfUpdates.Id = raw_source.Id
                        )
        -- These rows will either UPDATE the SSN (or other columns too) of existing patients or INSERT the new SSNs (others) of new patients
        SELECT 
        updates.Id AS mergeKey, updates.*
        FROM updatesCdf AS updates
     
        UNION ALL

        -- These rows will INSERT new addresses of existing customers 
        -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
        SELECT NULL as mergeKey, updates.*
        FROM updatesCdf AS updates
        INNER JOIN PatientRecords_Silver_SCDTypeTwo as patients
        ON updates.Id = patients.Id 
        WHERE patients.IsCurrent = true AND updates.SSN <> patients.SSN

         ) AS staged_updates
         
  ON target.Id = staged_updates.mergeKey
  
  WHEN MATCHED AND target.IsCurrent = true
               AND target.SSN <> staged_updates.SSN THEN  
               
  UPDATE SET target.IsCurrent = false, target.EndDate = current_timestamp()    -- Set current to false and endDate to source's effective date.
  
  WHEN NOT MATCHED THEN 
  INSERT(Id, BIRTHDATE, DEATHDATE, SSN, EffectiveDate, EndDate, IsCurrent, DRIVERS,PASSPORT, PREFIX, FIRST, LAST, SUFFIX, MAIDEN, MARITAL, RACE, ETHNICITY, GENDER, BIRTHPLACE, ADDRESS, CITY, STATE, ZIP, IngestTimestamp) 
  VALUES(staged_updates.Id, 
         staged_updates.BIRTHDATE,
         staged_updates.DEATHDATE,
         staged_updates.SSN, 
         current_timestamp(), 
         null,
         true,
         staged_updates.DRIVERS, staged_updates.PASSPORT, staged_updates.PREFIX, staged_updates.FIRST, staged_updates.LAST, staged_updates.SUFFIX, staged_updates.MAIDEN, staged_updates.MARITAL, staged_updates.RACE, staged_updates.ETHNICITY, staged_updates.GENDER, staged_updates.BIRTHPLACE, staged_updates.ADDRESS, staged_updates.CITY, staged_updates.STATE, staged_updates.ZIP, current_timestamp()
        ) -- Set current to true along with the new address and its effective date.
  
  """)
  
  return

# COMMAND ----------

# DBTITLE 1,Read Stream Before Data Correction
bronze_df = (spark
             .readStream
             .format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", 2) ## ! comment out to see the difference in data volumes the stream will merge (1 vs thousands)
             .table("BronzePatientUpdates")
            )

#display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Upsert Bad Record Change With Version History + Effective Dates -- SCD Type 2 Changes
## CDF Becomes more important the more complex the merge logic gets!
(
 bronze_df
.writeStream
.trigger(once=True) ## processingTime = '1 minute', availableNow = True
.option("checkpointLocation", bronze_silver_scd2_checkpoint)
.foreachBatch(mergeSCD2FunctionSQL) #mergeCDFFunctionPython
.start()
)

# COMMAND ----------

# DBTITLE 1,This is the current "Bad version"
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PatientRecords_Silver_SCDTypeTwo WHERE Id = "cccccccccc-6cc4-48a8-a0b1-99999999ccccc" ORDER BY IngestTimestamp DESC;

# COMMAND ----------

# DBTITLE 1,Simulate a correction of the bad patient record - Insert the update into bronze (this is often how we get HLS changes)
df_new_patient = (sc.parallelize([{"Id": "cccccccccc-6cc4-48a8-a0b1-99999999ccccc", 
                                  "BIRTHDATE":"1991-12-01", 
                                  "DEATHDATE":"null", 
                                  "SSN":"4383-43-921",
                                  "DRIVERS":"33304040",
                                  "PASSPORT": "asdklhdfg", 
                                  "PREFIX": "Dr.", 
                                  "FIRST":"CODY", 
                                  "LAST":"Davis", 
                                  "SUFFIX":"null",
                                  "MAIDEN":"null",
                                  "MARITAL":"M",
                                  "RACE":"white",
                                  "ETHNICITY":"american",
                                  "GENDER":"M",
                                  "BIRTHPLACE":"Somewhere in Texas",
                                  "ADDRESS":"1308 Misty Meadow Dr.",
                                  "CITY":"FORT WORTH",
                                  "STATE":"TX", 
                                  "ZIP": "76133"}]).toDF()
                 ).withColumn("IngestTimestamp", current_timestamp())


df_new_patient.createOrReplaceTempView("patient_updates")

df_new_patient.write.format("delta").mode("append").saveAsTable("BronzePatientUpdates") ## should be delta version 3

# COMMAND ----------

# DBTITLE 1,Stream Data Correction
bronze_df = (spark
             .readStream
             .format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", 3) ## ! comment out to see the difference in data volumes the stream will merge (1 vs thousands)
             .table("BronzePatientUpdates")
            )


## CDF Becomes more important the more complex the merge logic gets!
(
 bronze_df
.writeStream
.trigger(once=True) ## processingTime = '1 minute', availableNow = True
.option("checkpointLocation", bronze_silver_scd2_checkpoint)
.foreachBatch(mergeSCD2FunctionSQL) #mergeCDFFunctionPython
.start()
)

# COMMAND ----------

# DBTITLE 1,New Fixed Version with Change History!
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PatientRecords_Silver_SCDTypeTwo WHERE Id = "cccccccccc-6cc4-48a8-a0b1-99999999ccccc" ORDER BY IngestTimestamp DESC;

# COMMAND ----------

# DBTITLE 1,Python Version

def mergeSCD2Python(updatesDF, batchId):

  patientTable = DeltaTable.forName("PatientRecords_Silver_SCDTypeTwo")

  #Rows to INSERT new addresses of existing customers
  newPatientsToInsert = (updatesDF.alias("updates")
                      .join(patientTable.toDF.alias("patients"), on="Id")
                      .where("patients.current = true AND updates.SSN <> patients.SSN")
                        )

  # Stage the update by unioning two sets of rows
  # 1. Rows that will be inserted in the `whenNotMatched` clause
  # 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
  
  stagedUpdates = (newPatientsToInsert
                  .selectExpr("NULL as mergeKey", "updates.*")   # Rows for 1.
                  .union(
                    updatesDF.selectExpr("updates.customerId as mergeKey", "*")  # Rows for 2.
                        )
                  )

  # Apply SCD Type 2 operation using merge
  (patientTable
    .alias("patients")
    .merge(
      stagedUpdates.alias("staged_updates"),
      "patients.customerId = mergeKey AND patients.current = true AND patients.SSN <> staged_updates.SSN")
    .whenMatchedUpdate(set = { "IsCurrent" : False, "EndDate": current_timestamp() } )
  .whenNotMatchedInsert(values = {
      "Id": "staged_updates.Id",
      "BIRTHDATE": "staged_updates.BIRTHDATE",
      "DEATHDATE": "staged_updates.DEATHDATE",
      "SSN": "staged_updates.SSN",
      "EffectiveDate": current_timestamp(),
      "EndDate": None,
      "IsCurrent": True,
      "DRIVERS": "staged_updates.DRIVERS",
      "PASSPORT": "staged_updates.PASSPORT",
      "PREFIX": "staged_updates.PREFIX",
      "FIRST": "staged_updates.FIRST",
      "LAST": "staged_updates.LAST",
      "SUFFIX": "staged_updates.SUFFIX",
      "MAIDEN": "staged_updates.MAIDEN",
      "MARITAL": "staged_updates.MARITAL",
      "RACE": "staged_updates.RACE",
      "ETHNICITY": "staged_updates.ETHNICITY",
      "GENDER": "staged_updates.GENDER",
      "BIRTHPLACE": "staged_updates.BIRTHPLACE",
      "ADDRESS": "staged_updates.ADDRESS",
      "CITY": "staged_updates.CITY",
      "STATE": "staged_updates.STATE",
      "ZIP": "staged_updates.ZIP",
      "IngestTimestamp": current_timestamp()
    }) 
  .execute()
  )
  
  return

  
