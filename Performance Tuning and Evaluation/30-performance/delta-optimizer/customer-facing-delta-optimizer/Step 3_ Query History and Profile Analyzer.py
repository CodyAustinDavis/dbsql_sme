# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Select from Results table to Look at Profiles of Queries, Tables, and Recommendations

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from deltaoptimizer import DeltaOptimizerBase, DeltaProfiler, QueryProfiler, DeltaOptimizer
import os

# COMMAND ----------

dbutils.widgets.text("Optimizer Output Database", "hive_metastore.delta_optimizer")

# COMMAND ----------

optimizer_location = dbutils.widgets.get("Optimizer Output Database").strip()
delta_optimizer = DeltaOptimizer(database_name=optimizer_location)

# COMMAND ----------

# DBTITLE 1,Get Most Recent Strategy Results
## This table by default has only 1 file, so it shouldnt be expensive to collect
table_mode = dbutils.widgets.get("table_mode")
include_table_list = [i.strip() for i in dbutils.widgets.get("include_list(csv)").split(",")]
exclude_table_list = [i.strip() for i in dbutils.widgets.get("exclude_list(csv)").split(",")]

if table_mode == "include_all_tables":
  df_results = (delta_optimizer.get_results()
               )
elif table_mode == "use_include_list":
  df_results = (delta_optimizer.get_results()
  .filter(col("TableName").isin(*include_table_list))
               )
  
elif table_mode == "use_exclude_list": 
  df_results = (delta_optimizer.get_results()
  .filter(~col("TableName").isin(*exclude_table_list))
               )
  
  
df_results.display()

# COMMAND ----------

# DBTITLE 1,Get Table Stats
df = spark.sql(f"""

SELECT * 
FROM {optimizer_location}.all_tables_table_stats

""")

df.display()


# COMMAND ----------

# DBTITLE 1,Get Cardinality Stats

df = spark.sql(f"""

SELECT * 
FROM {optimizer_location}.all_tables_cardinality_stats
WHERE IsUsedInReads = 1 OR IsUsedInWrites = 1
""")

df.display()

# COMMAND ----------

# DBTITLE Register Unique Queries
unqiue_queries = spark.sql(f"""SELECT * FROM {optimizer_location}.parsed_distinct_queries""")
unqiue_queries.createOrReplaceTempView("unique_queries")

# COMMAND ----------

# DBTITLE 1,Raw Query Runs Tables

""" This table contains ALL queries for the monitored warehouses that have been run over time, so you can build all kinds of visualizations on that. These are NOT distinct queries, they are every single query run
"""

raw_queries_df = spark.sql(f"""

SELECT *,
from_unixtime(query_start_time_ms/1000) AS QueryStartTime,
from_unixtime(query_end_time_ms/1000) AS QueryEndTime,
duration/1000 AS QueryDurationSeconds
FROM {optimizer_location}.raw_query_history_statistics

""")

raw_queries_df.createOrReplaceTempView("raw_queries")

raw_queries_df.display()

# COMMAND ----------

# DBTITLE 1,Most Expensive Queries in a all run history (user can add timestamp filter in a WHERE clause)
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC r.query_hash,
# MAGIC r.query_text,
# MAGIC SUM(r.duration/1000) AS TotalRuntimeOfQuery,
# MAGIC AVG(r.duration/1000) AS AvgDurationOfQuery,
# MAGIC COUNT(r.query_id) AS TotalRunsOfQuery,
# MAGIC COUNT(r.query_id) / COUNT(DISTINCT date_trunc('day', QueryStartTime)) AS QueriesPerDay,
# MAGIC SUM(r.duration/1000) / COUNT(DISTINCT date_trunc('day', QueryStartTime)) AS TotalRuntimePerDay
# MAGIC FROM raw_queries r
# MAGIC WHERE QueryStartTime >= (current_date() - 7)
# MAGIC GROUP BY r.query_hash, r.query_text
# MAGIC ORDER BY TotalRuntimePerDay DESC

# COMMAND ----------

# DBTITLE 1,Query Runs Over Time - General
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC date_trunc('hour', QueryStartTime) AS Date,
# MAGIC COUNT(query_id) AS TotalQueryRuns,
# MAGIC AVG(QueryDurationSeconds) AS AvgQueryDurationSeconds
# MAGIC FROM raw_queries
# MAGIC GROUP BY date_trunc('hour', QueryStartTime)
# MAGIC ORDER BY Date

# COMMAND ----------

# DBTITLE 1,Top 10 Queries with Most Total Runtime Per Day (Duration * # times run)
# MAGIC %sql
# MAGIC
# MAGIC WITH r AS (
# MAGIC   SELECT 
# MAGIC   date_trunc('day', r.QueryStartTime) AS Date,
# MAGIC   r.query_hash,
# MAGIC   SUM(r.duration/1000) AS TotalRuntimeOfQuery,
# MAGIC   AVG(r.duration/1000) AS AvgDurationOfQuery,
# MAGIC   COUNT(r.query_id) AS TotalRunsOfQuery
# MAGIC   FROM raw_queries r
# MAGIC   GROUP BY date_trunc('day', r.QueryStartTime), r.query_hash
# MAGIC ),
# MAGIC s as (
# MAGIC SELECT 
# MAGIC *,
# MAGIC DENSE_RANK() OVER (PARTITION BY Date ORDER BY TotalRuntimeOfQuery DESC) AS PopularityRank
# MAGIC FROM r
# MAGIC )
# MAGIC SELECT 
# MAGIC uu.query_text,
# MAGIC s.*
# MAGIC FROM s
# MAGIC LEFT JOIN unique_queries uu ON uu.query_hash = s.query_hash
# MAGIC  WHERE PopularityRank <= 10

# COMMAND ----------

# DBTITLE 1,Top 10 Longest Running Queries By Day
# MAGIC %sql
# MAGIC
# MAGIC WITH r AS (
# MAGIC   SELECT 
# MAGIC   date_trunc('day', r.QueryStartTime) AS Date,
# MAGIC   r.query_hash,
# MAGIC   SUM(r.duration/1000) AS TotalRuntimeOfQuery,
# MAGIC   AVG(r.duration/1000) AS AvgDurationOfQuery,
# MAGIC   COUNT(r.query_id) AS TotalRunsOfQuery
# MAGIC   FROM raw_queries r
# MAGIC   GROUP BY date_trunc('day', r.QueryStartTime), r.query_hash
# MAGIC ),
# MAGIC s as (
# MAGIC SELECT 
# MAGIC *,
# MAGIC DENSE_RANK() OVER (PARTITION BY Date ORDER BY AvgDurationOfQuery DESC) AS PopularityRank
# MAGIC FROM r
# MAGIC )
# MAGIC SELECT 
# MAGIC uu.query_text,
# MAGIC s.*
# MAGIC FROM s
# MAGIC LEFT JOIN unique_queries uu ON uu.query_hash = s.query_hash
# MAGIC  WHERE PopularityRank <= 10

# COMMAND ----------

# DBTITLE 1,Top 10 Most OFTEN ran queries by Day
# MAGIC %sql
# MAGIC
# MAGIC WITH r AS (
# MAGIC   SELECT 
# MAGIC   date_trunc('day', r.QueryStartTime) AS Date,
# MAGIC   r.query_hash,
# MAGIC   SUM(r.duration/1000) AS TotalRuntimeOfQuery,
# MAGIC   AVG(r.duration/1000) AS AvgDurationOfQuery,
# MAGIC   COUNT(r.query_id) AS TotalRunsOfQuery
# MAGIC   FROM raw_queries r
# MAGIC   GROUP BY date_trunc('day', r.QueryStartTime), r.query_hash
# MAGIC ),
# MAGIC s as (
# MAGIC SELECT 
# MAGIC *,
# MAGIC DENSE_RANK() OVER (PARTITION BY Date ORDER BY TotalRunsOfQuery DESC) AS PopularityRank
# MAGIC FROM r
# MAGIC )
# MAGIC SELECT 
# MAGIC uu.query_text,
# MAGIC s.*
# MAGIC FROM s
# MAGIC LEFT JOIN unique_queries uu ON uu.query_hash = s.query_hash
# MAGIC  WHERE PopularityRank <= 10

# COMMAND ----------

# DBTITLE 1,Most Expensive Table MERGE / DELETE operations
writes_df = spark.sql(f"""

SELECT * 
FROM {optimizer_location}.write_statistics_merge_predicate

""")

writes_df.display()
