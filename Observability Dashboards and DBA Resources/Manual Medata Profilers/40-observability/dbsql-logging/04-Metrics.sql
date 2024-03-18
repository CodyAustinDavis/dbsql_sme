-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Contents
-- MAGIC * Dashbaords & Queries with owner
-- MAGIC * Queries to Optimise
-- MAGIC * Warehouse Metrics
-- MAGIC * Per User Metrics
-- MAGIC
-- MAGIC A note on tables: This has hard coded in the table names and doesn't reference them from /00-Config

-- COMMAND ----------

-- MAGIC %run ./00-Config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f' USE {DATABASE_NAME}')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dashbaords & Queries with owner

-- COMMAND ----------

-- DBTITLE 1,Workflows API
SELECT creator_user_name, col.sql_task.* FROM 
--only select from the latest snapshot
(SELECT creator_user_name, explode(tasks), RANK() OVER (ORDER BY snapshot_time DESC) AS rk 
 FROM workflows)  
WHERE rk = 1
AND col.sql_task.warehouse_id IS NOT NULL

-- COMMAND ----------

-- DBTITLE 1,Dashboards API
SELECT * EXCEPT (rk) FROM 
--only select from the latest snapshot
(SELECT  user.email, id as query_id, RANK() OVER (ORDER BY snapshot_time DESC) AS rk 
 FROM dashboards_preview)  
WHERE rk = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Queries to Optimize

-- COMMAND ----------

-- DBTITLE 1,Long Running Queries 
SELECT user_name
,round(total_time_ms/1000/60/60,2) AS total_time_h
,query_id
,query_text
FROM queries 
WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
ORDER BY total_time_ms DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Long Running Queries without Photon
SELECT user_name
,round(task_total_time_ms/1000/60,2) AS task_total_time_m
,round(photon_total_time_ms/1000/60,2) AS photon_time_m
,round((task_total_time_ms-photon_total_time_ms)/1000/60,2) AS non_photon_time_m
,round(photon_total_time_ms/task_total_time_ms * 100,2) as photon_pct
,query_id
,query_text
FROM queries 
WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
AND statement_type IN ("SELECT", "MERGE") --select & merge statements only
AND task_total_time_ms IS NOT NULL
AND photon_total_time_ms IS NOT NULL
AND task_total_time_ms > 600000 --longer than 10 mins to run
AND round(photon_total_time_ms/task_total_time_ms * 100,2) < 75 -- only queries under 75% non photon
ORDER BY photon_pct ASC, task_total_time_ms DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Queries with Spill
SELECT user_name, spill_to_disk_bytes, query_id, query_text
FROM queries 
WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
AND spill_to_disk_bytes > 0
ORDER BY spill_to_disk_bytes DESC
LIMIT 100

-- COMMAND ----------

-- DBTITLE 1,Large Row Read Counts
SELECT user_name, rows_read_count, query_id, query_text
FROM queries 
WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
AND rows_read_count > 100000
ORDER BY rows_read_count DESC
LIMIT 100

-- COMMAND ----------

-- DBTITLE 1,Multiple Failed Queries
 SELECT user_name
 ,error_description
 ,error_fq
 ,query_fq
 ,round(error_fq/query_fq*100,2) AS error_fq_pct
 ,query_duration_h
 ,error_duration_h
 ,round(error_duration_h/query_duration_h*100,2) AS error_duration_pct
 FROM (
  SELECT Q1.user_name
  ,CASE WHEN error_message IS null OR error_message = "" THEN ""
        WHEN contains(error_message, "[PARSE_SYNTAX_ERROR]") THEN "Parse Syntax Error"
        WHEN contains(error_message, "[PARSE_EMPTY_STATEMENT]") THEN "Parse Empty Statement"
        WHEN contains(error_message, "[MISSING_COLUMN]") THEN "Missing Column"
        WHEN contains(error_message, "[OPERATION_REQUIRES_UNITY_CATALOG]") THEN "Requires Unity Catalog"
        WHEN contains(error_message, "Catalog namespace is not supported") THEN "Catalog namespace is not supported"
        WHEN contains(error_message, "timed out") THEN "Timeout Error"
        WHEN contains(error_message, "ExecutorLostFailure") THEN "Executor Lost Failure"
        WHEN contains(error_message, "stage failure") THEN "Stage Failure"     
        WHEN contains(error_message, "Database") AND contains(error_message, "not found") THEN "Database not found"
        WHEN contains(error_message, "Table") AND contains(error_message, "not found") THEN "Table not found"
        WHEN contains(error_message, "Database") AND contains(error_message, "already exists") THEN "Database already exists"
        WHEN contains(error_message, "Table") AND contains(error_message, "already exists") THEN "Table already exists"
        WHEN contains(error_message, "column") AND contains(error_message, "already exists") THEN "Column already exists"
        WHEN contains(error_message, "is a view. DESCRIBE DETAIL is only supported for tables.") THEN "Describe on View"
        WHEN contains(error_message, "permission") OR contains(error_message, "User does not own") THEN "Permission Error"
        WHEN contains(error_message, "data type mismatch") THEN "Data Type Mismatch"
        WHEN contains(error_message, "ambiguous") THEN "Ambiguous Column"
        WHEN contains(error_message, "protocol") THEN "Delta Protocol Error"
        ELSE "Other"
        END AS error_description
  ,count(DISTINCT query_id) AS error_fq
  ,round(sum(total_time_ms)/1000/60/60,2) AS error_duration_h
  ,query_fq
  ,query_duration_h
  FROM queries Q1
  INNER JOIN (SELECT user_name
              ,count(DISTINCT query_id) AS query_fq
              ,round(sum(total_time_ms)/1000/60/60,2) AS query_duration_h
              FROM queries 
              WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
              GROUP BY user_name) Q2
  ON Q1.user_name = Q2.user_name
  WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
  GROUP BY Q1.user_name, error_description, query_fq, query_duration_h)
WHERE error_description <> ""
ORDER BY error_duration_pct DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Warehouse Metrics

-- COMMAND ----------

-- DBTITLE 1,Common Error Messages
SELECT W.id
,CASE WHEN error_message IS null OR error_message = "" THEN ""
      WHEN contains(error_message, "[PARSE_SYNTAX_ERROR]") THEN "Parse Syntax Error"
      WHEN contains(error_message, "[PARSE_EMPTY_STATEMENT]") THEN "Parse Empty Statement"
      WHEN contains(error_message, "[MISSING_COLUMN]") THEN "Missing Column"
      WHEN contains(error_message, "[OPERATION_REQUIRES_UNITY_CATALOG]") THEN "Requires Unity Catalog"
      WHEN contains(error_message, "Catalog namespace is not supported") THEN "Catalog namespace is not supported"
      WHEN contains(error_message, "timed out") THEN "Timeout Error"
      WHEN contains(error_message, "ExecutorLostFailure") THEN "Executor Lost Failure"
      WHEN contains(error_message, "stage failure") THEN "Stage Failure"     
      WHEN contains(error_message, "Database") AND contains(error_message, "not found") THEN "Database not found"
      WHEN contains(error_message, "Table") AND contains(error_message, "not found") THEN "Table not found"
      WHEN contains(error_message, "Database") AND contains(error_message, "already exists") THEN "Database already exists"
      WHEN contains(error_message, "Table") AND contains(error_message, "already exists") THEN "Table already exists"
      WHEN contains(error_message, "column") AND contains(error_message, "already exists") THEN "Column already exists"
      WHEN contains(error_message, "is a view. DESCRIBE DETAIL is only supported for tables.") THEN "Describe on View"
      WHEN contains(error_message, "permission") OR contains(error_message, "User does not own") THEN "Permission Error"
      WHEN contains(error_message, "data type mismatch") THEN "Data Type Mismatch"
      WHEN contains(error_message, "ambiguous") THEN "Ambiguous Column"
      WHEN contains(error_message, "protocol") THEN "Delta Protocol Error"
      ELSE "Other"
      END AS error_description
,count(DISTINCT query_id) AS status_fq
,round(sum(total_time_ms)/1000/60/60,2) AS status_duration_h
FROM (SELECT *, RANK() OVER(ORDER BY snapshot_time DESC) AS rk FROM warehouses) AS W
LEFT JOIN (SELECT query_id, error_message, status, endpoint_id, total_time_ms 
            FROM queries 
            WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
           ) AS Q
ON W.id = Q.endpoint_id
WHERE rk = 1 --take only the most recent snapshot
AND status IS NOT NULL --remove endpoints that have no recent queries
AND error_message IS NOT NULL
AND error_message <> ""
GROUP BY  W.id, error_description

-- COMMAND ----------

-- DBTITLE 1,%Queries failed
SELECT W.id, Q.status, count(distinct query_id) AS status_fq, round(sum(total_time_ms)/1000/60/60,2) AS status_duration_h
FROM (SELECT *, RANK() OVER(ORDER BY snapshot_time DESC) AS rk FROM warehouses) AS W
LEFT JOIN (SELECT query_id, status, endpoint_id, total_time_ms 
            FROM queries 
            WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
           ) AS Q
ON W.id = Q.endpoint_id
WHERE rk = 1 --take only the most recent snapshot
AND status IS NOT NULL --remove endpoints that have no recent queries
GROUP BY  W.id, Q.status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Per User Metrics

-- COMMAND ----------

WITH  user_list AS (SELECT DISTINCT user_name FROM queries)
     
     --
     ,failed_queries AS (SELECT user_name, count(query_id) AS freq_failed 
                         FROM queries 
                         WHERE timestamp_millis(query_start_time_ms) > CURRENT_DATE() - 30 AND status = 'FAILED'
                         GROUP BY user_name)

     -- total spill to disk across all queries                  
     ,spill AS          (SELECT user_name, sum(spill_to_disk_bytes) AS total_spill 
                         FROM queries 
                         WHERE timestamp_millis(query_start_time_ms) > CURRENT_DATE() - 30 
                         GROUP BY user_name) 
                
      --maximum non photon percent for queries over 10 mins
     ,non_photon_pct AS (SELECT user_name, max(round((total_time_ms - photon_total_time_ms)/total_time_ms * 100,2)) as non_photon_pct
                         FROM queries 
                         WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
                         AND statement_type IN ("SELECT", "MERGE") --select & merge statements only
                         AND total_time_ms IS NOT NULL AND photon_total_time_ms IS NOT NULL
                         AND total_time_ms > 600000 --longer than 10 mins to run
                         GROUP BY user_name)
       
       --Large Row Read
     ,rows_read AS      (SELECT user_name, max(rows_read_count) as max_rows_read
                         FROM queries 
                         WHERE timestamp_millis(query_start_time_ms) > current_date() - 30 
                         GROUP BY user_name) 
  
SELECT user_list.user_name
,coalesce(freq_failed,0) AS freq_failed
,coalesce(total_spill,0) AS total_spill
,coalesce(non_photon_pct,0) AS max_non_photon_pct
,coalesce(max_rows_read,0) AS max_rows_read
FROM user_list 
LEFT JOIN failed_queries 
  ON user_list.user_name = failed_queries.user_name
LEFT JOIN spill 
  ON user_list.user_name = spill.user_name
LEFT JOIN non_photon_pct 
  ON user_list.user_name = non_photon_pct.user_name
LEFT JOIN rows_read 
  ON user_list.user_name = rows_read.user_name
  
ORDER BY freq_failed DESC, total_spill DESC, max_non_photon_pct DESC, max_rows_read DESC;
