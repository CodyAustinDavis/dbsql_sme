-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## BI Layer - Using Views to Dynamically Publish Specific Table States Until explicitly published
-- MAGIC
-- MAGIC Dynamically publishing tables / views can be helpful for all kinds for situations such as :
-- MAGIC
-- MAGIC 1. Blue/Green deployments
-- MAGIC 2. Custom Updates / Refreshes
-- MAGIC 3. Dynamic Masking for Serving

-- COMMAND ----------

USE CATALOG '${Catalog}';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('${Schema}');
USE IDENTIFIER(CONCAT('${Catalog}','.', '${Schema}')); 

-- COMMAND ----------

SELECT  current_catalog(), current_database()

-- COMMAND ----------

SELECT SUM(num_steps)
FROM cdf_demo_silver_sensors s
WHERE s.user_id = 1

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE active_silver INT;
DECLARE OR REPLACE VARIABLE prev_silver INT;
DECLARE OR REPLACE VARIABLE active_gold INT;

-- This can either be stored in the target table or in a separate state table that is smaller
SET VARIABLE (active_silver, prev_silver) = (WITH h AS (DESCRIBE HISTORY cdf_demo_silver_sensors)
SELECT MAX(version) AS v, COALESCE(MAX(version) - 1, 0) AS v_minus_1 FROM h
);


SELECT CONCAT('Latest Silver Table Version', active_silver::string) AS ActiveSilverTableVersion,
CONCAT('Previous Silver Table Version', prev_silver::string) AS PrevSilverTableVersion

-- COMMAND ----------

-- DBTITLE 1,Implement Custom Publishing Rules for the VIEW

--- Implement some data quality rules to determine which version of the table to use (or blue/green)
DECLARE OR REPLACE VARIABLE version_to_publish INT; 


SET VARIABLE version_to_publish = (WITH deployment_gate_rules AS (
                                    SELECT
                                    count_if(event_timestamp >= (now() - INTERVAL 10 YEARS))  AS NewishRecords,
                                    COUNT(0) AS Records
                                    FROM cdf_demo_silver_sensors
                                    )
                                    SELECT 
                                    
                                IF(
        (NewishRecords::float / Records::float ) > 0.95  -- RULE 1
        AND Records = 999999 -- RULE 2
        --AND -- Add more rules!
                                    , active_silver 
                                    , prev_silver) AS v
                                    FROM deployment_gate_rules
                                    );

SELECT CONCAT('Version of source table to publish in active view: ', version_to_publish) AS DeploymentVersion;

-- COMMAND ----------

-- DBTITLE 1,What if I want to create views from dynamic snapshots/paramters?
DECLARE OR REPLACE VARIABLE dynamic_view STRING;

SET VARIABLE dynamic_view = CONCAT("CREATE OR REPLACE VIEW bi_view_cumulative_steps
                                    AS 
                                    SELECt s.*,
                                    u.total_steps,
                                    SUM(s.num_steps) OVER (PARTITION BY s.user_id ORDER BY s.event_timestamp) AS cumulative_steps
                                    FROM cdf_demo_silver_sensors VERSION AS OF ", version_to_publish::int,  " s
                                    INNER JOIN cdf_demo_gold_user_num_step_aggregate u ON u.user_id = s.user_id
                                    WHERE s.user_id = 1
                                    ORDER BY s.user_id, s.event_timestamp");


EXECUTE IMMEDIATE dynamic_view

-- COMMAND ----------

-- DBTITLE 1,Build Queries on View
SELECT * 
FROM bi_view_cumulative_steps
WHERE user_id = 1
