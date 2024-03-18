# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delta Table Optimization Methods Tutorial
# MAGIC
# MAGIC This notebook walks through the various methods and consideration when tuning / optimizing Delta tables in SQL

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Delta Tables Optimization Knobs
# MAGIC
# MAGIC ### File Sizes
# MAGIC
# MAGIC #### COMPACTION - OPTIMIZE
# MAGIC
# MAGIC ##### ZORDER / CLUSTER BY (liquid tables)
# MAGIC
# MAGIC ###### Bloom Filter

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Optimizing for UPSERTS
# MAGIC
# MAGIC Commands 4-8

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS iot_dashboard.bronze_sensors_optimization;
# MAGIC CREATE OR REPLACE TABLE iot_dashboard.bronze_sensors_optimization
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES("delta.targetFileSize"="64mb") --2-128 mb for tables with heavy updates or if used for BI
# MAGIC AS 
# MAGIC (SELECT * FROM iot_dashboard.silver_sensors LIMIT 10000) --Only load a subset for sample MERGE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS iot_dashboard.silver_sensors_optimization;
# MAGIC CREATE OR REPLACE TABLE iot_dashboard.silver_sensors_optimization
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES("delta.targetFileSize"="2mb")
# MAGIC AS 
# MAGIC SELECT * fROM iot_dashboard.silver_sensors;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO iot_dashboard.silver_sensors_optimization AS target
# MAGIC USING (SELECT Id::integer,
# MAGIC               device_id::integer,
# MAGIC               user_id::integer,
# MAGIC               calories_burnt::decimal,
# MAGIC               miles_walked::decimal,
# MAGIC               num_steps::decimal,
# MAGIC               timestamp::timestamp,
# MAGIC               value::string
# MAGIC               FROM iot_dashboard.bronze_sensors_optimization) AS source
# MAGIC ON source.Id = target.Id
# MAGIC AND source.user_id = target.user_id
# MAGIC AND source.device_id = target.device_id
# MAGIC AND target.timestamp > now() - INTERVAL 2 hours
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC   target.calories_burnt = source.calories_burnt,
# MAGIC   target.miles_walked = source.miles_walked,
# MAGIC   target.num_steps = source.num_steps,
# MAGIC   target.timestamp = source.timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC -- Without optimizing tables 8.82 seconds
# MAGIC -- After optimizing by merge columns 19 seconds

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run CREATE/REPLACE and MERGE statements, track runtime, then run OPTIMIZE statement and run all create/merge statements again to look at spark plan differences

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- You want to optimize by high cardinality columns like ids, timestamps, strings
# MAGIC -- ON MERGE COLUMNS, then timeseries columns, then commonly used columns in queries
# MAGIC
# MAGIC --This operation is incremental
# MAGIC --OPTIMIZE iot_dashboard.bronze_sensors_test1 ZORDER BY (Id, user_id, device_id);
# MAGIC OPTIMIZE iot_dashboard.silver_sensors_optimization ZORDER BY (user_id, device_id, Id);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What about queries on this table?
# MAGIC
# MAGIC 1. ZORDER by commonly joined columns
# MAGIC 2. Partition by larger chunks only if needed
# MAGIC 3. Keep important columns in front of tables
# MAGIC 4. For highly selective queries, use bloom indexes

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercise 1: Change optimization strategies for single point filters

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE iot_dashboard.silver_sensors_optimization ZORDER BY (user_id);
# MAGIC
# MAGIC -- by user_id, timestamp -- 8 files pruned
# MAGIC -- by just user id selecting on user_id -- 34 files pruned (1 read) all but one
# MAGIC -- by just timestamp -- no files pruned when selecting on user_id

# COMMAND ----------

# DBTITLE 1,Create gold aggregate VIEW
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW iot_dashboard.hourly_summary_statistics
# MAGIC AS
# MAGIC SELECT user_id,
# MAGIC date_trunc('hour', timestamp) AS HourBucket,
# MAGIC AVG(num_steps) AS AvgNumStepsAcrossDevices,
# MAGIC AVG(calories_burnt) AS AvgCaloriesBurnedAcrossDevices,
# MAGIC AVG(miles_walked) AS AvgMilesWalkedAcrossDevices
# MAGIC FROM iot_dashboard.silver_sensors_optimization
# MAGIC GROUP BY user_id,date_trunc('hour', timestamp) -- wrapping a function around a column
# MAGIC ORDER BY HourBucket

# COMMAND ----------

# DBTITLE 1,Exercise 1: Tuning for single column queries
# MAGIC %sql
# MAGIC
# MAGIC -- LOOK AT BEFORE AND AFTER QUERIES for OPTIMIZE PRE/POST
# MAGIC
# MAGIC -- After optimize look at user_id files pruned
# MAGIC -- by user_id, timestamp -- 8 files pruned
# MAGIC -- by just user id selecting on user_id -- 34 files pruned (1 read) all but one
# MAGIC -- by just timestamp -- no files pruned when selecting on user_is
# MAGIC
# MAGIC -- POST OPTIMIZE SCAN METRICS
# MAGIC --number of files pruned	33
# MAGIC -- number of files read	1
# MAGIC
# MAGIC SELECT * FROM iot_dashboard.hourly_summary_statistics WHERe user_id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercise 2: Multi-dimensional filters and optimzation

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT MIN(HourBucket), MAX(HourBucket)
# MAGIC FROM iot_dashboard.hourly_summary_statistics 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE iot_dashboard.silver_sensors_optimization ZORDER BY (user_id, timestamp);
# MAGIC
# MAGIC -- by user_id, timestamp -- 2 files pruned, 29 scanned
# MAGIC -- by timestamp, user_id --  does order matter? 2 files pruned, 29 scanned, - not really
# MAGIC -- How to make this more selective? -- Hour bucket is abstracting the filter pushdown, lets try just the raw table

# COMMAND ----------

# DBTITLE 1,Exercise 2: Optimizing Multi-dimensional queries
# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM iot_dashboard.hourly_summary_statistics 
# MAGIC WHERE user_id = 1
# MAGIC AND HourBucket BETWEEN "2018-07-22T00:00:00.000+0000" AND "2018-07-22T01:00:00.000+0000"

# COMMAND ----------

# DBTITLE 1,Lesson learned -- let Delta do the filtering first, then group and aggregate -- subqueries are actually better
# MAGIC %sql
# MAGIC
# MAGIC -- Look at SPARK QUERY PLAN SCAN node
# MAGIC -- How many files are pruned/read? 
# MAGIC -- Try optimizing the table on different columns (1,2,3) -- see what happens!
# MAGIC --28 pruned, 3 files read
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM iot_dashboard.silver_sensors_optimization
# MAGIC WHERE user_id = 1
# MAGIC AND timestamp BETWEEN "2018-07-22T00:00:00.000+0000"::timestamp AND "2018-07-22T01:00:00.000+0000"::timestamp

# COMMAND ----------

# DBTITLE 1,Automate Certain Pushdown Filter Rules in VIEWs
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW iot_dashboard.test_filter_pushdown
# MAGIC AS 
# MAGIC WITH raw_pushdown AS
# MAGIC (
# MAGIC   SELECT * 
# MAGIC   FROM iot_dashboard.silver_sensors_optimization
# MAGIC   WHERE user_id = 1
# MAGIC   AND timestamp BETWEEN "2018-07-22T00:00:00.000+0000"::timestamp AND "2018-07-22T01:00:00.000+0000"::timestamp
# MAGIC )
# MAGIC SELECT user_id,
# MAGIC date_trunc('hour', timestamp) AS HourBucket,
# MAGIC AVG(num_steps) AS AvgNumStepsAcrossDevices,
# MAGIC AVG(calories_burnt) AS AvgCaloriesBurnedAcrossDevices,
# MAGIC AVG(miles_walked) AS AvgMilesWalkedAcrossDevices
# MAGIC FROM raw_pushdown
# MAGIC GROUP BY user_id,date_trunc('hour', timestamp)
# MAGIC ORDER BY HourBucket

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Now pruning is automatically done and manual users do not have to remember each time for common views
# MAGIC SELECT * FROM iot_dashboard.test_filter_pushdown

# COMMAND ----------

# DBTITLE 1,Efficacy on More Complex VIEWs
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW iot_dashboard.smoothed_hourly_statistics
# MAGIC AS 
# MAGIC SELECT *,
# MAGIC -- Number of Steps
# MAGIC (avg(`AvgNumStepsAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           4 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       )) ::float AS SmoothedNumSteps4HourMA, -- 4 hour moving average
# MAGIC       
# MAGIC (avg(`AvgNumStepsAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           24 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedNumSteps12HourMA --24 hour moving average
# MAGIC ,
# MAGIC -- Calories Burned
# MAGIC (avg(`AvgCaloriesBurnedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           4 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedCalsBurned4HourMA, -- 4 hour moving average
# MAGIC       
# MAGIC (avg(`AvgCaloriesBurnedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           24 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedCalsBurned12HourMA --24 hour moving average,
# MAGIC ,
# MAGIC -- Miles Walked
# MAGIC (avg(`AvgMilesWalkedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           4 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedMilesWalked4HourMA, -- 4 hour moving average
# MAGIC       
# MAGIC (avg(`AvgMilesWalkedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           24 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedMilesWalked12HourMA --24 hour moving average
# MAGIC FROM iot_dashboard.hourly_summary_statistics

# COMMAND ----------

# DBTITLE 1,File Pruning on Complex VIEWs
# MAGIC %sql
# MAGIC
# MAGIC -- How are files being pruned in the SCAN node?
# MAGIC SELECt * FROM iot_dashboard.smoothed_hourly_statistics WHERE user_id = 1
