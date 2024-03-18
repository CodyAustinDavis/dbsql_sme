-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query History Analysis via System Tables
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Business Questions: 
-- MAGIC

-- COMMAND ----------

-- We cant get exact skew ratio without number of cores of the cluster, but we CAN compare to average ratio for all queries in the group of a certain type
-- of ALL total task time summed up for ALL queries in the period (this is NOT including IDLE time), get % of total task time by query
-- How to divide by warehouse? Statement type?
-- Would need to hash query, pull query tak, etc. to group by more meaningful dimensions for % utilize task time

CREATE OR REPLACE TEMPORARY VIEW query_logs AS (
SELECT
statement_id,
executed_by,
statement_text,
warehouse_id,
execution_status,
f.statement_type,
total_duration_ms AS QueryRuntime,
total_task_duration_ms AS CPUTotalExecutionTime,
execution_duration_ms AS ExecutionQueryTime,
compilation_duration_ms AS CompilationQueryTime,
waiting_at_capacity_duration_ms AS QueueQueryTime,
waiting_for_compute_duration_ms AS StartUpQueryTime,
result_fetch_duration_ms AS ResultFetchTime,
start_time,
end_time,
update_time,
COALESCE(total_task_duration_ms / execution_duration_ms, NULL) AS TotalCPUTime_To_Execution_Time_Ratio,
COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0) AS ProportionQueueTime,
AVG(COALESCE(total_task_duration_ms / execution_duration_ms, NULL)) OVER (PARTITION BY f.statement_type) AS AvgTaskSkewRatio,
AVG(total_duration_ms)  OVER (PARTITION BY f.statement_type) AS AvgQueryRuntime,
AVG(waiting_at_capacity_duration_ms) OVER (PARTITION BY f.statement_type) AS AvgQueueTime,
AVG(COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0)) OVER (PARTITION BY f.statement_type) AS AvgProportionTimeQueueing,
SUM(total_task_duration_ms) OVER () AS TotalUsedTaskTimeInPeriod,
total_task_duration_ms / TotalUsedTaskTimeInPeriod AS ProportionOfTaskTimeUsedByQuery 
-- Can use this to chargeback (as long as you know denominator is only USED task time, not including idele time) )
FROM system.query.history f
WHERE warehouse_id = '${warehouse_id}'
AND start_time >= dateadd(DAY, -${lookback_days}::int, now())
)

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,Create Automated Parser for Query Tagging
CREATE OR REPLACE FUNCTION parse_query_tag(input_sql STRING, pattern STRING DEFAULT '--QUERY_TAG:(.*?)(\r\n|\n|$)', no_tag_default STRING DEFAULT 'UNTAGGED')
RETURNS STRING
DETERMINISTIC
READS SQL DATA
COMMENT 'Extract tag from a query based on regex template'
RETURN    
  WITH tagged_query AS (
      SELECT regexp_replace(regexp_extract(input_sql, pattern, 0), '--QUERY_TAG:', '') AS raw_tagged
      )
    SELECT CASE WHEN length(raw_tagged) >= 1 THEN raw_tagged ELSE no_tag_default END AS clean_tag FROM tagged_query
;

-- COMMAND ----------

-- DBTITLE 1,Query Runtime Allocation - Greedy Query Trends
-- We can now start to calculate query chargebacks if we manually define the cluster size / pricing

-- We can do a pareto analysis - find the top X queries that contribute to Y% of of the task time
WITH statement_aggs AS (
SELECT
statement_text,
SUM(CPUTotalExecutionTime/1000) AS TotalTaskTimeForStatement,
MAX(TotalUsedTaskTimeInPeriod/1000) AS TotalTaskTimeInPeriod,
SUM(CPUTotalExecutionTime/1000)/ MAX(TotalUsedTaskTimeInPeriod/1000) AS StatementTaskTimeProportion
FROM query_logs
WHERE CPUTotalExecutionTime > 0
GROUP BY statement_text
ORDER BY TotalTaskTimeForStatement DESC

), 

cumulative_sum AS (
SELECT 
*,
SUM(StatementTaskTimeProportion) OVER (ORDER BY StatementTaskTimeProportion DESC) AS CumulativeProportionOfTime
FROM statement_aggs
)

SELECT 
*,
parse_query_tag(statement_text) AS query_tag,
CASE WHEN CumulativeProportionOfTime::float <= '${pareto_point}'::float THEN statement_text ELSE 'Remaining Queries' END AS QueryPareto
FROM cumulative_sum
