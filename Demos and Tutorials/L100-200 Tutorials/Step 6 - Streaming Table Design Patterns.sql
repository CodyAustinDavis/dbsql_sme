-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## This notebook will show a basic ETL design pattern on DBSQL with Streaming Tables, Materialized Views, and Hyrbid Strategies
-- MAGIC
-- MAGIC 1. Using read_files/read_kafka with Streaming tables for ingestion
-- MAGIC 2. How to orhcestrate -- refresh schedule or external orchestrator
-- MAGIC 3. Reading from streaming tables incrementally
-- MAGIC 4. Upserting data into target tables from Streaming Tables
-- MAGIC 5. Materialized Views as Gold Layer
-- MAGIC 6. Limitations
-- MAGIC
-- MAGIC ### FAQ: 
-- MAGIC
-- MAGIC 1. <b> Q: </b> If I REFRESH a streaming table in a Multi task job, and another task depends on it, will that task wait until the streaming table is complete? What do I do to make sure the previous refresh completes before the rest of the downstream tasks start?
-- MAGIC
-- MAGIC 2. <b> Q: </b> How can I read incrementally from a streaming table? Can I stream from it in python? Can I do batch style and delete data from the table once it is processed downstream without breaking things?
-- MAGIC
-- MAGIC 3. <b> Q: </b> What if I want to UPSERT/ MERGE data into a target table? Are materialized views my only option?
-- MAGIC
-- MAGIC 4. <b> Q: </b> Can I use streaming tables for ingesting into delta, then manage batches myself downstream with timestamp watermarking? This is a popular EDW loading pattern and I dont want to move to all DLT based development. All I want is streaming tables to create batches for me, then I want to do anything I want to, similar to staging tables in batch loading. This removes all restrictions I have downstream. How do I do this? 
-- MAGIC
-- MAGIC
-- MAGIC 5. <b> Q: </b> Can I optimize / sort my streaming tables? I would need to do this to do manual watermarking to filter on ingest timestamp / checksum. 
-- MAGIC
-- MAGIC <b> A: </b> Not streaming tables. You can only cluster/zorder complete / non-streaming tables. You would have to use a different method to efficiently incrementally process by sorted timestamps manually if not using MVs.
-- MAGIC
-- MAGIC 6. <b> Q: </b> If I cant, and I dont want to use materialized views or full loads for ALL downstream tables, then I would just use DBT + COPY INTO drops.
-- MAGIC
-- MAGIC 7. <b> Q: </b> Can I accomplish this with COPY INTO if I need a classical loading pattern? Would be great if I could do the same thing but with a Kafka data source as well. 
-- MAGIC
-- MAGIC 8. <b> Q: </b> Can I look at these ST and MV refreshes in the DBSQL query history? I want to look at performance of them.
-- MAGIC
-- MAGIC -----
-- MAGIC
-- MAGIC Streaming tables are refreshed and governed by DLT on the backend. With streaming tables, DLT is only the engine but orchestration is handled by the user either via schedules or external orchestrator Refreshes. 
-- MAGIC Streaming tables and Materialized Views run on Serverless pipelines under the DLT Serverless SKU: $0.4 /DBU Premium, $0.5 /DBU Enterprise. 
-- MAGIC This is CHEAPER than just the Serverless SQL warehouses - so it is a huge benefit to have automatically managed streaming tables and materialized views from a price/performance perspective
-- MAGIC Snowflake charges MORE or SAME for various workload types (such as the 1.5 multiplier just for tasks), this makes ETL on Databricks HUGELY price/performant

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Limitations
-- MAGIC
-- MAGIC 1. Cannot easily incrementally read from streaming tables without MVs
-- MAGIC 2. Cannot CLUSTER STs or MVs -- so even if you want to manually incrementally read it wont be efficient (Why cant you cluster MVs?)

-- COMMAND ----------

-- DBTITLE 1,ONLY UNITY CATALOG TABLES ARE SUPPORTED
CREATE DATABASE IF NOT EXISTS main.iot_dashboard;
USE main.iot_dashboard;

-- COMMAND ----------

-- DBTITLE 1,Reading Data From Raw Files / Stream in Streaming Tables for BRONZE layer
DROP TABLE IF EXISTS main.iot_dashboard.streaming_tables_raw_data;

-- Does not support CREATE OR REPLACE
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
  );

-- COMMAND ----------

-- DBTITLE 1,Select from Streaming Table
SELECT * FROM main.iot_dashboard.streaming_tables_raw_data

-- COMMAND ----------

SELECT COUNT(0) AS DataSourceRowCount FROM main.iot_dashboard.streaming_tables_raw_data

-- COMMAND ----------

DESCRIBE HISTORY main.iot_dashboard.streaming_tables_raw_data

-- COMMAND ----------

REFRESH TABLE main.iot_dashboard.streaming_tables_raw_data

-- COMMAND ----------

-- DBTITLE 1,Adding a Refresh schedule - Not for all design patterns
-- Adds a schedule to refresh the streaming table once a day
-- at midnight in Los Angeles
ALTER STREAMING TABLE main.iot_dashboard.streaming_tables_raw_data
ADD SCHEDULE CRON '0 0 0 * * ? *' 
AT TIME ZONE 'America/Los_Angeles' --Timezone optional -- defaults to timezone session is located in
;

-- COMMAND ----------

-- Remove the schedule
ALTER STREAMING TABLE main.iot_dashboard.streaming_tables_raw_data
DROP SCHEDULE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -----

-- COMMAND ----------

-- DBTITLE 1,Reading from a streaming table:  Stream --> Batch w Watermarking
--DROP TABLE IF EXISTS main.iot_dashboard.streaming_silver_staging;

-- You must explicitly drop a streaming table to create it with a different definition

CREATE OR REFRESH STREAMING TABLE main.iot_dashboard.streaming_silver_staging
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'='processed_watermark')
AS 
SELECT *,
now() AS processed_watermark
FROM STREAM main.iot_dashboard.streaming_tables_raw_data
;


-- COMMAND ----------

DESCRIBE DETAIL main.iot_dashboard.streaming_silver_staging

-- COMMAND ----------

-- DBTITLE 1,Option - Use a materialized VIEW for optimal incremental state management
CREATE MATERIALIZED VIEW IF NOT EXISTS main.iot_dashboard.mv_silver_staging
PARTITIONED BY (processed_watermark_date)
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'='processed_watermark')
AS 
SELECT *,
now() AS processed_watermark,
now()::date AS processed_watermark_date
FROM main.iot_dashboard.streaming_tables_raw_data
;

-- COMMAND ----------

REFRESH MATERIALIZED VIEW main.iot_dashboard.mv_silver_staging

-- COMMAND ----------

SELECT * FROM main.iot_dashboard.mv_silver_staging

-- COMMAND ----------

DESCRIBE HISTORY main.iot_dashboard.streaming_silver_staging

-- COMMAND ----------

-- DBTITLE 1,Truncate and Reload a Streaming Table
REFRESH STREAMING TABLE main.iot_dashboard.streaming_silver_staging FULL

-- COMMAND ----------

-- DBTITLE 1,Create Watermarking Table Materialized View for Downstream manual incremental loading

-- Option 1 for state tracking with watermarking - create an MV ledger that downstream processes can use as needed
-- This is better if downstream pipelines need to choose their own slice of data per pipeline
CREATE MATERIALIZED VIEW IF NOT EXISTS main.iot_dashboard.streaming_silver_process_ledger
AS
SELECT DISTINCT processed_watermark AS staging_table_processed_timestamp,
processed_watermark::date AS staging_table_processed_date
FROM main.iot_dashboard.streaming_silver_staging
ORDER BY staging_table_processed_timestamp DESC;

-- COMMAND ----------

SELECT * FROM main.iot_dashboard.streaming_silver_process_ledger

-- COMMAND ----------

-- DBTITLE 1,Silver Tables - Create DDL for 
CREATE OR REPLACE TABLE main.iot_dashboard.streaming_silver_sensors
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING,
processed_time TIMESTAMP
)
USING DELTA 
PARTITIONED BY (user_id);

-- COMMAND ----------

-- Option 2 for state tracking - just create an MV with the most recent processed date/timestamp at all times, like a counter. 
-- This is better if the downstream pipeline is ALWAYS going to run after this and it only needs to most recent processed date

-- This should be fast since stats should be collected on this column
CREATE OR REPLACE VIEW main.iot_dashboard.streaming_silver_high_watermark
AS
SELECT COALESCE(MAX(processed_time), '1900-01-01 00:00:00'::timestamp) AS high_watermark
FROM main.iot_dashboard.streaming_silver_sensors;

-- COMMAND ----------

SELECT * FROM main.iot_dashboard.streaming_silver_high_watermark

-- COMMAND ----------

-- DBTITLE 1,Incremental Upsert
-- Is this a way to make this really fast by sorting the source table?

MERGE INTO main.iot_dashboard.streaming_silver_sensors AS target
USING (
WITH de_dup (
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              processed_watermark,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY timestamp DESC) AS DupRank
              FROM main.iot_dashboard.streaming_silver_staging --main.iot_dashboard.mv_silver_staging filter streaming table directly
              WHERE processed_watermark > (SELECT high_watermark FROM main.iot_dashboard.streaming_silver_high_watermark)
              )
              
SELECT Id, device_id, user_id, calories_burnt, miles_walked, num_steps, timestamp, value, processed_watermark AS processed_time
FROM de_dup
WHERE DupRank = 1
) AS source
ON --source.processed_time > (SELECT high_watermark FROM main.iot_dashboard.streaming_silver_high_watermark) !! This will be supported once variables are ready
 source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 1,Show post update new high watermark
SELECT * FROM main.iot_dashboard.streaming_silver_high_watermark

-- COMMAND ----------

-- DBTITLE 1,Show updated table!
SELECT * FROM main.iot_dashboard.streaming_silver_sensors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Which one was more performant? Reading from an MV or an ST?

-- COMMAND ----------

-- DBTITLE 1,Does pruning work for filtering streaming tables?
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              processed_watermark,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY timestamp DESC) AS DupRank
              FROM main.iot_dashboard.streaming_silver_staging --main.iot_dashboard.mv_silver_staging filter streaming table directly
              WHERE processed_watermark > (SELECT high_watermark FROM main.iot_dashboard.streaming_silver_high_watermark)
