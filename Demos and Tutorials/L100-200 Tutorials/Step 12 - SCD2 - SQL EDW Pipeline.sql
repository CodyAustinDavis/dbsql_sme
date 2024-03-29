-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # This notebook generates a full data pipeline from databricks dataset - iot-stream using INSERT ONLY SCD-2 Architecture
-- MAGIC
-- MAGIC ## This creates 2 tables: 
-- MAGIC
-- MAGIC <b> Database: </b> iot_dashboard
-- MAGIC
-- MAGIC <b> Tables: </b> silver_sensors_silver, silver_sensors_bronze (raw updates)
-- MAGIC
-- MAGIC <b> Params: </b> StartOver (Yes/No) - allows user to truncate and reload pipeline

-- COMMAND ----------

-- DBTITLE 1,Medallion Architecture
-- MAGIC %md
-- MAGIC
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg" >

-- COMMAND ----------

DROP DATABASE IF EXISTS iot_dashboard CASCADE;
CREATE DATABASE IF NOT EXISTS iot_dashboard;
USE CATALOG main;
USE SCHEMA iot_dashboard;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # DDL Documentation: 
-- MAGIC
-- MAGIC https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-alter-table.html

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS iot_dashboard.bronze_sensors_scd_2
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING,
ingest_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES("delta.targetFileSize"="128mb")
-- Other helpful properties
-- delta.dataSkippingNumIndexedCols -- decides how many columns are automatically tracked with statistics kepts (defaults to first 32)
-- LOCATION "s3://bucket-name/data_lakehouse/tables/data/bronze/bronze_senors/"
;

-- COMMAND ----------

-- DBTITLE 1,Look at Table Details
DESCRIBE TABLE EXTENDED iot_dashboard.bronze_sensors_scd_2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## New FEATURES IN DBR 11!
-- MAGIC
-- MAGIC 1. COPY INTO GENERIC TABLE
-- MAGIC 2. DROP COLUMN STATEMENT
-- MAGIC 3. Select all except: SELECT * EXCEPT (col1,...) FROM table
-- MAGIC
-- MAGIC https://docs.databricks.com/release-notes/runtime/11.0.html

-- COMMAND ----------


--With DBR 11, we dont need to specify DDL first
--CREATE TABLE IF NOT EXISTS iot_dashboard.bronze_sensors

--COPY INTO iot_dashboard.bronze_sensors
--FROM (SELECT 
--      id::bigint AS Id,
--      device_id::integer AS device_id,
--      user_id::integer AS user_id,
--      calories_burnt::decimal(10,2) AS calories_burnt, 
--      miles_walked::decimal(10,2) AS miles_walked, 
--      num_steps::decimal(10,2) AS num_steps, 
--     timestamp::timestamp AS timestamp,
--      value AS value -- This is a JSON object
--FROM "/databricks-datasets/iot-stream/data-device/")
--FILEFORMAT = json
--COPY_OPTIONS('force'='true') -- 'false' -- process incrementally
--option to be incremental or always load all files
 


-- COMMAND ----------

-- DBTITLE 1,Incrementally Ingest Source Data from Raw Files
COPY INTO iot_dashboard.bronze_sensors_scd_2
FROM (SELECT 
      id::bigint AS Id,
      device_id::integer AS device_id,
      user_id::integer AS user_id,
      calories_burnt::decimal(10,2) AS calories_burnt, 
      miles_walked::decimal(10,2) AS miles_walked, 
      num_steps::decimal(10,2) AS num_steps, 
      timestamp::timestamp AS timestamp,
      value  AS value, -- This is a JSON object,
      now() AS ingest_timestamp
FROM "/databricks-datasets/iot-stream/data-device/")
FILEFORMAT = json -- csv, xml, txt, parquet, binary, etc.
COPY_OPTIONS('force'='true') --'true' always loads all data it sees. option to be incremental or always load all files


--Other Helpful copy options:
/*
PATTERN('[A-Za-z0-9].json')
FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true') -- skips bad files for more robust incremental loads
COPY_OPTIONS ('mergeSchema' = 'true')
'ignoreChanges' = 'true' - ENSURE DOWNSTREAM PIPELINE CAN HANDLE DUPLICATE ALREADY PROCESSED RECORDS WITH MERGE/INSERT WHERE NOT EXISTS/Etc.
'ignoreDeletes' = 'true'
*/;

-- COMMAND ----------

-- DBTITLE 1,Create Silver Table for upserting updates
CREATE TABLE IF NOT EXISTS iot_dashboard.silver_sensors_scd_2
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING,
ingest_timestamp TIMESTAMP,
-- Processing Columns
_start_timestamp TIMESTAMP,
_end_timestamp TIMESTAMP,
_batch_run_id STRING,
_is_current BOOLEAN
)
USING DELTA 
PARTITIONED BY (_is_current)
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name') -- if update heavy, file sizes are great between 64-128 mbs. The more update heavy, the smaller the files (32-256mb)
--LOCATION s3://<path>/ -- Always specify location for production tables so you control where it lives in S3/ADLS/GCS
-- Not specifying location parth will put table in DBFS, a managed bucket that cannot be accessed by apps outside of databricks
;

-- COMMAND ----------

-- DBTITLE 1,Check incoming batch for data
SELECT * FROM iot_dashboard.bronze_sensors_scd_2

-- COMMAND ----------

-- DBTITLE 1,Perform SCD2 INSERT ONLY Upserts - Device Data
-- Step 1 - get state of the active batch

--DECLARE OR REPLACE VARIABLE var_batch_id STRING = uuid();

-- Optional intra-batch pre insert/merge de-cup
CREATE OR REPLACE TABLE iot_dashboard.temp_batch_to_insert
AS
WITH de_dup (
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              ingest_timestamp,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY ingest_timestamp DESC) AS DupRank
              FROM iot_dashboard.bronze_sensors_scd_2
              )
              
SELECT Id, device_id, user_id, calories_burnt, miles_walked, num_steps, timestamp, value, ingest_timestamp, 
now() AS _start_timestamp, 
true AS _is_current,
1001 AS _batch_run_id -- example batch run id
FROM de_dup
WHERE DupRank = 1
;

MERGE INTO iot_dashboard.silver_sensors_scd_2 AS target
USING ( 

      SELECT updates.Id AS merge_key_id,
        updates.user_id AS merge_key_user_id,
        updates.device_id AS merge_key_device_id,
        updates.* --merge key can be built in whatever way makes sense to get unique rows
      FROM iot_dashboard.temp_batch_to_insert AS updates
    
      UNION ALL

      -- These rows will INSERT updated rows of existing records and new rows
      -- Setting the merge_key to NULL forces these rows to NOT MATCH and be INSERTed.
      SELECT 
      NULL AS merge_key_id,
      NULL AS merge_key_user_id,
      NULL AS merge_key_device_id,
      updates.*
      FROM iot_dashboard.temp_batch_to_insert AS updates
      INNER JOIN iot_dashboard.silver_sensors_scd_2 as target_table
      ON updates.Id = target_table.Id
      AND updates.user_id = target_table.user_id
      AND updates.device_id = target_table.device_id  -- What makes the key unique
      -- This needs to be accounted for when deciding to expire existing rows
      WHERE updates.value <> target_table.value -- Only update if any of the data has changed

        ) AS source
        
ON target.Id = source.merge_key_id
AND target.user_id = source.merge_key_user_id
AND target.device_id = source.merge_key_device_id

WHEN MATCHED AND (target._is_current = true AND target.value <> source.value) THEN
UPDATE SET
target._end_timestamp = source._start_timestamp, -- start of new record is end of old record
target._is_current = false

WHEN NOT MATCHED THEN 
INSERT (id, device_id, user_id, calories_burnt, miles_walked, num_steps, value, timestamp, ingest_timestamp, _start_timestamp, _end_timestamp, _is_current, _batch_run_id)
VALUES  (
source.id, source.device_id, source.user_id, source.calories_burnt, source.miles_walked, source.num_steps, source.value, source.timestamp, 
source.ingest_timestamp,
source._start_timestamp, -- start timestamp -- new records
NULL ,-- end_timestamp 
source._is_current, -- is current record
source._batch_run_id --example batch run id
)
;

-- This calculate table stats for all columns to ensure the optimizer can build the best plan
-- THIS IS NOT INCREMENTAL
ANALYZE TABLE iot_dashboard.silver_sensors_scd_2 COMPUTE STATISTICS FOR ALL COLUMNS;

-- THIS IS INCREMENTAL
OPTIMIZE iot_dashboard.silver_sensors_scd_2 ZORDER BY (timestamp, device_id);

-- Truncate bronze batch once successfully loaded
-- If succeeds remove temp table
TRUNCATE TABLE iot_dashboard.temp_batch_to_insert;

-- COMMAND ----------

DESCRIBE HISTORY iot_dashboard.silver_sensors_scd_2

-- COMMAND ----------

-- DBTITLE 1,Select Raw Table
SELECT * FROM iot_dashboard.silver_sensors_scd_2

-- COMMAND ----------

-- DBTITLE 1,Get Amount of Expired Records
SELECT 
_is_current AS ActiveRecord,
COUNT(0)
FROM iot_dashboard.silver_sensors_scd_2
GROUP BY _is_current

-- COMMAND ----------

-- DBTITLE 1,Look at various batch timelines over time
SELECT 
`_start_timestamp` AS active_timestamp,
COUNT(0)
FROM iot_dashboard.silver_sensors_scd_2
GROUP BY `_start_timestamp`
ORDER BY active_timestamp

-- COMMAND ----------

-- DBTITLE 1,Create "Current" View
CREATE OR REPLACE VIEW iot_dashboard.silver_sensors_current
AS
SELECT * FROM iot_dashboard.silver_sensors_scd_2
WHERE _is_current = true

-- COMMAND ----------

-- DBTITLE 1,Create "Snapshotted Views"
CREATE OR REPLACE VIEW iot_dashboard.silver_sensors_snapshot_as_of_2023_10_10_19_30_00
AS
-- Get more recent record for each record as of a specific version
WITH de_dup (
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              ingest_timestamp,
              _start_timestamp,
              _is_current,
              _end_timestamp,
              ROW_NUMBER() OVER(PARTITION BY id ORDER BY _start_timestamp DESC) AS DupRank -- Get most recent record as of a specific point in time
              FROM iot_dashboard.silver_sensors_scd_2
              -- Point in time snapshot timestamp such as end of month
              WHERE _start_timestamp <= '2023-10-10T19:30:00'::timestamp
              )
              
SELECT *
FROM de_dup
WHERE DupRank = 1
;

-- COMMAND ----------

-- DBTITLE 1,Look at snapshot of most recent version of each record at a point in time
SELECT *
FROM iot_dashboard.silver_sensors_snapshot_as_of_2023_10_10_19_30_00