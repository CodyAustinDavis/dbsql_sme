-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Syntax for Alerts in DBSQL
-- MAGIC
-- MAGIC This notebook contains snippets that may be useful to be alerted for on DBSQL usage. 
-- MAGIC
-- MAGIC Schedule this using workflows, or use the schedule dropdown on the top right of the notebook. The job should take <20 mins to run and consume ~1 DBU, there's very little data being processes here, even on busy workspaces.
-- MAGIC
-- MAGIC Alerts should be actionable. "Nice to know" information just acts as noise. Examples of actional alerts may be:
-- MAGIC * Terminating long running warehouses
-- MAGIC * Investigating long running queries
-- MAGIC * Sizing up warehouses that have specific query failures
-- MAGIC
-- MAGIC **Remember**, if you want to be notified of a query taking 2 hours to run, this job must be scheduled at least every two hours
-- MAGIC
-- MAGIC ### How to set up alerts with DBSQL
-- MAGIC 1. DBSQL > SQL Editor > create a query in DBSQL by coping the below or creating your own, name it, and save it
-- MAGIC 2. DBSQL > Alerts > Create Alert > select your query you have just saved, set the threshold for values to be alerted for, save, then change the destination if needed
-- MAGIC
-- MAGIC [Official docs](https://docs.databricks.com/sql/user/alerts/index.html)

-- COMMAND ----------

-- MAGIC %run ./00-Config

-- COMMAND ----------

-- %run ./03-APIs_to_Delta
-- Uncomment this if you would like to run as part of a job
-- Remove these comments too!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f' USE {DATABASE_NAME}')

-- COMMAND ----------

-- DBTITLE 1,Queries currently running that are over the 95th percentile
SELECT round(duration/1000/60/60,2) as duration_h, *
FROM queries
WHERE duration > (SELECT percentile(duration, 0.95) AS duration_95
                  FROM queries WHERE status = "FINISHED"
                  AND statement_type IN ("SELECT", "MERGE"))
AND status = "RUNNING"
ORDER BY duration DESC

-- COMMAND ----------

-- DBTITLE 1,Queries taking over 6 hours to run
SELECT round(duration/1000/60/60,2) as duration_h, *
FROM queries
WHERE duration > 21600000 --6 hours in miliseconds
AND status = "RUNNING"
ORDER BY duration DESC
