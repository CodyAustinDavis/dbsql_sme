-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Bronze To Silver Incremental Processing with Custom CDF Checkpoints
-- MAGIC
-- MAGIC 1. Option 1 - Use custom CDF checkpoints if you have complex batch logic that requires you to need more control over processing intra batch
-- MAGIC
-- MAGIC 2. Option 2 - Use Streaming Pipelines and Materialized View for simple and easy use cases!

-- COMMAND ----------

USE CATALOG '${Catalog}';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('${Schema}');
USE IDENTIFIER(CONCAT('${Catalog}','.', '${Schema}'));

-- COMMAND ----------

-- DBTITLE 1,Define Latest Read Version for Target table(s)

---===== Step 5 - Create Dynamic Incremental Read Pattern
DECLARE OR REPLACE VARIABLE checkpoint_version_bronze_to_silver INT;
DECLARE OR REPLACE VARIABLE next_temp_checkpoint INT;

-- This can either be stored in the target table or in a separate state table that is smaller
SET VARIABLE checkpoint_version_bronze_to_silver = (SELECT COALESCE(MAX(latest_version), 0) FROM cdf_checkpoint_silver_sensor);


SET VARIABLE next_temp_checkpoint = (SELECT COALESCE(MAX(_commit_version), checkpoint_version_bronze_to_silver) AS tmp_v
                                  FROM table_changes('cdf_demo_bronze_sensors', checkpoint_version_bronze_to_silver)
                                  WHERE _commit_version > checkpoint_version_bronze_to_silver
                                    );


SELECT CONCAT('Latest Version Processed: ', checkpoint_version_bronze_to_silver::string) AS latest_check,
CONCAT('Next Checkpoint Max Version For Active Batch: ', next_temp_checkpoint::string) AS next_check

-- COMMAND ----------

-- DBTITLE 1,Perform Automatically Incremental Merge with CDF + Variables
---===== Step 6 - MERGE incremental reads into batch silver table
/*
Common scenarios to handle

1. Late arriving data (make sure silver latest_update_timestamp < source_record_update_timestamp)

2. Duplicate + late updates (decide on when the latest source upate timestamp for a given id can be)

3. Updates After Deletes (within a batch and across batches) --> Ignore Updates for Non matches to target table

4. Inserts After Deletes --> Insert if business logic dictates, if you do NOT want to insert a record that has already been deleted, then use SCD2 and check history to add merge condition on WHEN NOT MATHCED AND <found_deleted_record>


-- TIP - If you have deletes to deal with, you want to track your state in a separate table, because there are scenarios where the 
*/
MERGE INTO cdf_demo_silver_sensors AS target
USING (
    SELECT * 
    FROM table_changes('cdf_demo_bronze_sensors', checkpoint_version_bronze_to_silver) -- add one to increment the last version that has impacted the target layer
    WHERE 1=1
    AND _change_type != 'update_preimage'
    AND _commit_version > checkpoint_version_bronze_to_silver -- Only bring in new data from new CDF version, if no new data, gracefully merge no data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _commit_timestamp DESC) = 1 -- Get the most recent CDF behavior per unique record identifier just in case data source throws dup updates/deletes/inserts
) AS source
ON source.Id = target.Id
WHEN MATCHED 
    AND source._change_type IN ('insert' , 'update_postimage')
    AND ((source.bronze_update_timestamp > target.silver_update_timestamp) OR target.silver_update_timestamp IS NULL) -- Check to make sure older / late records do not sneak in
THEN UPDATE
 SET target.calories_burnt = source.calories_burnt,
     target.miles_walked = source.miles_walked,
     target.silver_update_timestamp = source._commit_timestamp,
     target.last_version = source._commit_version

WHEN MATCHED 
    AND source._change_type = 'delete' -- NON SCD Type 2 Deletion - has some considerations and potential to re-run old data if not a single record retains the new CDF commit version
    AND ((source._commit_timestamp > target.silver_update_timestamp) OR source.bronze_update_timestamp > target.silver_update_timestamp) 

THEN DELETE

WHEN NOT MATCHED AND source._change_type IN ('insert', 'update_postimage') -- Inserts + update for same record can be in same initial batch, where update wins as the most recent record
    -- Ignore 'delete' not matches, because you cant delete something that isnt there
    --- Then get most recent record if there are duplicate/multiple record updates in the source CDF batch
    THEN INSERT (Id, device_id, user_id, calories_burnt, miles_walked, num_steps, event_timestamp, value, silver_update_timestamp, last_version)
    VALUES(source.Id, source.device_id, source.user_id, source.calories_burnt, source.miles_walked, source.num_steps, source.event_timestamp, source.value, source._commit_timestamp, source._commit_version)
    ;

-- COMMAND ----------

-- DBTITLE 1,Update Checkpoint
INSERT INTO cdf_checkpoint_silver_sensor BY NAME 
SELECT next_temp_checkpoint AS latest_version, now() AS update_timestamp;

-- COMMAND ----------

-- DBTITLE 1,Check for Erroneous test record tests
-- We ran COPY INTO 2 times with force=true to simulate duplicate / bad data loads
-- We added a deletes with Id = 2 in bronze (could be manual or automated from CDF) -- Should return no results
-- We update a record with Id = 1 to num_steps = 99999;

SELECT * FROM cdf_demo_silver_sensors
WHERE Id = 2

-- COMMAND ----------

SELECT * FROM cdf_demo_silver_sensors 
WHERE Id = 1

-- COMMAND ----------

-- DBTITLE 1,Optimize Table - Liquid already contains cluster cols in DDL
OPTIMIZE cdf_demo_silver_sensors;
