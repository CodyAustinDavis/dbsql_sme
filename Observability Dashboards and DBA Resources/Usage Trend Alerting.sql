-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Montior Smoothed Usage Trends via System Tables

-- COMMAND ----------

WITH usage_aggregated AS (
  SELECT 
    date_trunc('hour', usage_start_time) AS usage_hour,
    SUM(usage_quantity) AS total_usage
  FROM system.billing.usage 
  WHERE usage_start_time >= (now() - INTERVAL 1 WEEK)
  GROUP BY date_trunc('hour', usage_start_time)
  ORDER BY usage_hour
),

smoothed_usage AS (
SELECT 
  usage_hour,
  total_usage,
  AVG(total_usage) OVER (ORDER BY usage_hour ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS fast_moving_average,
  AVG(total_usage) OVER (ORDER BY usage_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS slow_moving_average
FROM usage_aggregated
),

pop_change AS (
SELECT 
*,
fast_moving_average - LAG(fast_moving_average, 12, 0) OVER (ORDER BY usage_hour) AS p12h_change,
slow_moving_average - LAG(slow_moving_average, 24, 0) OVER (ORDER BY usage_hour) AS p24h_change
FROM smoothed_usage
),

final_change AS (
SELECT 
*,
COALESCE(ROUND((p12h_change / LAG(total_usage, 12, 0) OVER (ORDER BY usage_hour)) * 100, 2)::float, 0::float) AS p12h_change_percentage,
 ROUND((p24h_change / LAG(total_usage, 24, 0) OVER (ORDER BY usage_hour)) * 100, 2)::float AS p24h_change_percentage
FROM pop_change
)

SELECT 
*,
CASE WHEN p12h_change_percentage > 20.00 THEN 1 ELSE 0 END AS HighIncreaseFlag -- i.e. greater than 20% increase period over period
FROM final_change
