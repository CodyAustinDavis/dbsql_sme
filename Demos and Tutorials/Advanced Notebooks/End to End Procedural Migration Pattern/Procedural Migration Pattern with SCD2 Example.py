# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # End to End Procedural Programming for Data Warehousing Example for SCD2 Pipeline
# MAGIC
# MAGIC ## Overview: 
# MAGIC
# MAGIC This notebook shows how to use the popular delta helper libraries to implement end to end procedural data warehousing using the following: 
# MAGIC
# MAGIC 1. Simple Python + SQL for complex control flow
# MAGIC 2. DeltaLogger for easy logging and error tracking
# MAGIC 3. Serverless Client for Pushing SQL statements down to DBSQL Serverless Warehouse
# MAGIC 4. Multi Statement Transaction Manager for SCD2 Multi statement upserts pushed to DBSQL Serverless
# MAGIC
# MAGIC
# MAGIC ## Steps: 
# MAGIC
# MAGIC 0. Initialize Logger
# MAGIC 1. Create DDLS
# MAGIC 2. COPY INTO Bronze Tables
# MAGIC 3. MERGE Upserts (Multi Statement Transaction)
# MAGIC 4. Operational / Historical Snapshots Gold Tables
# MAGIC 5. Clean up staging tables
# MAGIC 6. Complete / Fail Runs in Logger
# MAGIC

# COMMAND ----------

# DBTITLE 1,Medallion Architecture
# MAGIC %md
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg" >

# COMMAND ----------

# DBTITLE 1,Optional, can build these libraries into wheel
# MAGIC %pip install sqlglot

# COMMAND ----------

# DBTITLE 1,Available Libraries for Procedural Management
from helperfunctions.deltalogger import DeltaLogger ## Easy logging OOTB
from helperfunctions.dbsqlclient import ServerlessClient ## Push Statement down to DBSQL from anyhere spark.sql() ==> serverless_client.sql
from helperfunctions.dbsqltransactions import DBSQLTransactionManager ## OOTB Multi-statement transactions to serverless SQL / DBSQL
from helperfunctions.deltahelpers import DeltaHelpers, DeltaMergeHelpers ## For Temp Tables and Concurrent Merge statements

# COMMAND ----------

# DBTITLE 1,Scope Session
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS main.iot_dashboard;
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA iot_dashboard;

# COMMAND ----------

# DBTITLE 1,Step 0:  Initialize Logger and Serverless Client
WAREHOUSE_ID = "<warehouse_id>"
HOST_NAME = "<host>"
TOKEN = "<token>"
LOGGER_TABLE = 'main.iot_dashboard.logger'
PIPELINE_PROCESS_NAME = 'iot_dashboard_scd2_end_to_end'

## Create Serverless Client
serverless_client = ServerlessClient(warehouse_id=WAREHOUSE_ID, host_name=HOST_NAME) #token=TOKEN

## Create Delta Logger
delta_logger = DeltaLogger(logger_table_name=LOGGER_TABLE, session_process_name=PIPELINE_PROCESS_NAME) # partition_cols=['start_date'], session_batch_id="12309821345"

## Optionally create transaction manager for multi statement transaction requirements (like SCD2 upserts)
serverless_transaction_manager = DBSQLTransactionManager(warehouse_id=WAREHOUSE_ID, host_name=HOST_NAME)

# COMMAND ----------

print(delta_logger.active_process_name)
print(delta_logger.active_run_id)
print(delta_logger.active_batch_id)

# COMMAND ----------

# DBTITLE 1,Start Run with Delta Logger
delta_logger.start_run(process_name='copy_into_command', batch_id="custom_batch_id")

# COMMAND ----------

print(delta_logger.active_run_start_ts)
print(delta_logger.active_run_status)

# COMMAND ----------

# DBTITLE 1,Step 1: Create DDLs
ddl_sql = """CREATE TABLE IF NOT EXISTS main.iot_dashboard.bronze_sensors_scd_2
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
;

CREATE TABLE IF NOT EXISTS main.iot_dashboard.silver_sensors_scd_2
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
PARTITIONED BY (_is_current, user_id)
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported', 'delta.columnMapping.mode' = 'name')
;
"""

## Simple Control Flow with Python, if/else, try/catch, for/while
try: 

  serverless_client.submit_multiple_sql_commands(ddl_sql)
  delta_logger.log_run_info(log_level='INFO', msg= f'DDLQuery runtime: 1 seconds')


except Exception as e:

  delta_logger.log_run_info(log_level='CRITICAL', msg='Failed to create DDLS with error')

  raise(e)



# COMMAND ----------

# DBTITLE 1,Step 2: Incrementally Ingest Source Data from Raw Files

copy_into_sql = """
COPY INTO main.iot_dashboard.bronze_sensors_scd_2
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
"""

## Simple Control Flow with Python, if/else, try/catch, for/while
try: 

  serverless_client.sql(copy_into_sql)

  batch_row_count = serverless_client.sql("SELECT COUNT(0) FROM main.iot_dashboard.bronze_sensors_scd_2").collect()[0][0]

  ## Log customer queryable metrics
  delta_logger.log_run_metric(run_metrics_dict={"Batch_Rows": batch_row_count})
  
  delta_logger.log_run_info(msg = 'COPY INTO complete')

except Exception as e:

  delta_logger.log_run_info(log_level='CRITICAL', msg='Failed to COPY INTO with error')
  raise(e)

# COMMAND ----------

# DBTITLE 1,Step 3: Multi Statement Transaction: Perform SCD2 INSERT ONLY Upserts - Device Data
mst_scd_transaction_sql = """

CREATE OR REPLACE TABLE main.iot_dashboard.temp_batch_to_insert
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
              FROM main.iot_dashboard.bronze_sensors_scd_2
              )
              
SELECT Id, device_id, user_id, calories_burnt, miles_walked, num_steps, timestamp, value, ingest_timestamp, 
now() AS _start_timestamp, 
true AS _is_current,
1 AS _batch_run_id -- example batch run id
FROM de_dup
WHERE DupRank = 1
;

MERGE INTO main.iot_dashboard.silver_sensors_scd_2 AS target
USING ( 

      SELECT updates.Id AS merge_key_id,
        updates.user_id AS merge_key_user_id,
        updates.device_id AS merge_key_device_id,
        updates.* --merge key can be built in whatever way makes sense to get unique rows
      FROM main.iot_dashboard.temp_batch_to_insert AS updates
    
      UNION ALL

      -- These rows will INSERT updated rows of existing records and new rows
      -- Setting the merge_key to NULL forces these rows to NOT MATCH and be INSERTed.
      SELECT 
      NULL AS merge_key_id,
      NULL AS merge_key_user_id,
      NULL AS merge_key_device_id,
      updates.*
      FROM main.iot_dashboard.temp_batch_to_insert AS updates
      INNER JOIN main.iot_dashboard.silver_sensors_scd_2 as target_table
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
"""


## Simple Control Flow with Python, if/else, try/catch, for/while

try: 

  #serverless_client.submit_multiple_sql_commands(mst_scd_transaction_sql)
  serverless_transaction_manager = DBSQLTransactionManager(warehouse_id=WAREHOUSE_ID)
  serverless_transaction_manager.execute_dbsql_transaction(sql_string=str(mst_scd_transaction_sql), tables_to_manage=['main.iot_dashboard.temp_batch_to_insert', 'main.iot_dashboard.silver_sensors_scd_2'])

except Exception as e:

  delta_logger.fail_run()

  raise(e)

# COMMAND ----------

# DBTITLE 1,Step 4: Clean up and Optimize Tables

## Simple Control Flow with Python, if/else, try/catch, for/while
try: 

  serverless_client.sql("TRUNCATE TABLE main.iot_dashboard.temp_batch_to_insert")
  delta_logger.log_run_info(msg='Batch cleared!')

except Exception as e:

  delta_logger.log_run_info(log_level='INFO', msg='couldnt find table, was already deleted')


## Optimize command
try: 

  serverless_client.sql("OPTIMIZE main.iot_dashboard.silver_sensors_scd_2 ZORDER BY (timestamp, device_id)")
  
  delta_logger.log_run_info(msg='Target tables optimized!')

except Exception as e:

  ## For these operations, they are not critical to the pipeline successs, so just log the event and keep going
  delta_logger.log_run_info(log_level='WARN', msg='couldnt find table, this should exist or a conflect happens')
  raise(e)


# COMMAND ----------

# DBTITLE 1,Create "Current" View
gold_views_sql = """
CREATE OR REPLACE VIEW main.iot_dashboard.silver_sensors_current
AS
SELECT * FROM main.iot_dashboard.silver_sensors_scd_2
WHERE _is_current = true;

CREATE OR REPLACE VIEW main.iot_dashboard.silver_sensors_snapshot_as_of_2023_10_10_19_30_00
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
              FROM main.iot_dashboard.silver_sensors_scd_2
              -- Point in time snapshot timestamp such as end of month
              WHERE _start_timestamp <= '2023-10-10T19:30:00'::timestamp
              )
              
SELECT *
FROM de_dup
WHERE DupRank = 1
;
"""


## Optimize command
try: 

  serverless_client.submit_multiple_sql_commands(gold_views_sql)

  delta_logger.log_run_info(msg='Operational View Created!')

except Exception as e:

  delta_logger.log_run_info(log_level='CRITICAL', msg='couldnt find table, all should exist')
  raise(e)


# COMMAND ----------

# DBTITLE 1,Complete Run!
delta_logger.complete_run()

# COMMAND ----------

delta_logger.full_table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *,
# MAGIC run_metadata:Batch_Rows -- our custom metrics we logged
# MAGIC FROM main.iot_dashboard.logger
# MAGIC ORDER BY run_id DESC
