-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## BI Layer - Using Views to Dynamically Publish Specific Table States Until explicitly published
-- MAGIC
-- MAGIC Dynamically publishing tables / views can be helpful for all kinds for situations such as :
-- MAGIC
-- MAGIC 1. Blue/Green deployments
-- MAGIC 2. Safer Updates / Refreshes
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

-- DBTITLE 1,Show current version
SELECt s.*,
u.total_steps,
SUM(s.num_steps) OVER (PARTITION BY s.user_id ORDER BY s.event_timestamp) AS cumulative_steps
FROM cdf_demo_silver_sensors s
INNER JOIN cdf_demo_gold_user_num_step_aggregate u ON u.user_id = s.user_id
WHERE s.user_id = 1
ORDER BY s.user_id, s.event_timestamp

-- COMMAND ----------

WITH h AS (DESCRIBE HISTORY cdf_demo_silver_sensors)
SELECT MAX(version) AS v , COALESCE(MAX(version) - 1, 0) AS v_minus_1 FROM h

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

-- DBTITLE 1,Create Active View
CREATE OR REPLACE VIEW bi_view_cumulative_steps_active_version
AS 
SELECt s.*,
u.total_steps,
SUM(s.num_steps) OVER (PARTITION BY s.user_id ORDER BY s.event_timestamp) AS cumulative_steps
FROM cdf_demo_silver_sensors s
INNER JOIN cdf_demo_gold_user_num_step_aggregate u ON u.user_id = s.user_id
WHERE s.user_id = 1
ORDER BY s.user_id, s.event_timestamp

-- COMMAND ----------

-- DBTITLE 1,What if I want to create views from dynamic snapshots/paramters?
DECLARE OR REPLACE VARIABLE dynamic_view STRING;

SET VARIABLE dynamic_view = CONCAT("CREATE OR REPLACE VIEW bi_view_cumulative_steps_previous_version
                                    AS 
                                    SELECt s.*,
                                    u.total_steps,
                                    SUM(s.num_steps) OVER (PARTITION BY s.user_id ORDER BY s.event_timestamp) AS cumulative_steps
                                    FROM cdf_demo_silver_sensors VERSION AS OF ", active_silver::int,  " s
                                    INNER JOIN cdf_demo_gold_user_num_step_aggregate u ON u.user_id = s.user_id
                                    WHERE s.user_id = 1
                                    ORDER BY s.user_id, s.event_timestamp");


EXECUTE IMMEDIATE dynamic_view
