-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Gold Aggregate / Custom Logic Table building with CDF or Materialized Views
-- MAGIC
-- MAGIC
-- MAGIC 1. Option 1 - CDF Manual Logic - If you have specific custom incremental update / aggregate logic you want to do, then you can do it all here in DBSQL!
-- MAGIC
-- MAGIC 2. Option 2 - If you want to keep it simple and easy, just use Streaming Tables + Materialized Views!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Option 1 Example - Lets say you want to incrementally update an aggregate for running total of user Id step in the latest X days of data. But you have SO much data or have more complex logic that makes you want to specifically control how the aggregations are build. 
-- MAGIC
-- MAGIC You can read the CDF from Silver, get the updates by user_id, and any user Id with an update, you can recalculate just those user Ids, regardless of how the table is partitioned
-- MAGIC

-- COMMAND ----------

USE CATALOG '${Catalog}';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('${Schema}');
USE IDENTIFIER(CONCAT('${Catalog}','.', '${Schema}')); 

-- COMMAND ----------

-- DBTITLE 1,Confirm Scope
SELECT current_catalog(), current_database()

-- COMMAND ----------

-- DBTITLE 1,Create DDL for Gold Aggregate Table
CREATE TABLE IF NOT EXISTS cdf_demo_gold_user_num_step_aggregate
(user_id INT,
total_steps DECIMAL,
gold_steps_update_timestamp TIMESTAMP,
latest_silver_version INT)
CLUSTER BY (user_id, gold_steps_update_timestamp); --Using liquid clustering

-- For small simple fact/dim tables, this can and should just be a materialized view
-- But if the logic for incrementally update this table is more complex, you may want more control. 


-- COMMAND ----------

-- DBTITLE 1,Define Latest Read Version for Target table(s)

---===== Step 5 - Create Dynamic Incremental Read Pattern
DECLARE OR REPLACE VARIABLE checkpoint_version_silver_to_gold_steps_agg INT;
DECLARE OR REPLACE VARIABLE next_temp_checkpoint INT;

-- This can either be stored in the target table or in a separate state table that is smaller
SET VARIABLE checkpoint_version_silver_to_gold_steps_agg = (SELECT COALESCE(MAX(latest_version), 0) FROM cdf_checkpoint_gold_users);


SET VARIABLE next_temp_checkpoint = (SELECT COALESCE(MAX(_commit_version), checkpoint_version_silver_to_gold_steps_agg) AS tmp_v
                                  FROM table_changes('cdf_demo_silver_sensors', checkpoint_version_silver_to_gold_steps_agg)
                                  WHERE _commit_version > checkpoint_version_silver_to_gold_steps_agg
                                    );


SELECT CONCAT('Latest Silver --> Gold Steps Agg Version Processed: ', checkpoint_version_silver_to_gold_steps_agg::string) AS latest_check,
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
MERGE INTO cdf_demo_gold_user_num_step_aggregate AS target
USING (

    WITH users_to_recalculate AS (
    SELECT user_id, MAX(_commit_version) AS latest_version
    FROM table_changes('cdf_demo_silver_sensors', checkpoint_version_silver_to_gold_steps_agg) -- add one to increment the last version that has impacted the target layer
    WHERE 1=1
    AND _commit_version > checkpoint_version_silver_to_gold_steps_agg -- Only bring in new data from new CDF version, if no new data, gracefully merge no data
    GROUP BY user_id
    )
    -- Prep already incrementalized Aggregate from CDF
    SELECT 
    s.user_id,
    SUM(s.num_steps) AS total_steps,
    MAX(latest_version) AS latest_version
    FROM cdf_demo_silver_sensors s 
    INNER JOIN users_to_recalculate AS update_users ON s.user_id = update_users.user_id
    GROUP BY s.user_id
    
) as source
ON source.user_id = target.user_id

WHEN MATCHED 
THEN UPDATE
 SET target.total_steps = source.total_steps,
     target.gold_steps_update_timestamp = current_timestamp(),
     target.latest_silver_version = source.latest_version

WHEN NOT MATCHED 
    THEN INSERT (user_id, total_steps, latest_silver_version, gold_steps_update_timestamp)
    VALUES(source.user_id, source.total_steps, source.latest_version, current_timestamp())
    ;

-- COMMAND ----------

-- DBTITLE 1,Update Checkpoint
INSERT INTO cdf_checkpoint_gold_users BY NAME 
SELECT next_temp_checkpoint AS latest_version, now() AS update_timestamp
WHERE next_temp_checkpoint > (SELECT COALESCE(MAX(latest_version), 0) FROM cdf_checkpoint_gold_users)
 ;

-- COMMAND ----------

-- DBTITLE 1,Look at checkpoint
SELECT * FROM cdf_checkpoint_gold_users
