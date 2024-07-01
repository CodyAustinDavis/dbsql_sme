{{ 
  config(
    materialized='table',
    liquid_clustered_by='device_id, HourBucket'
  )
}}

SELECT 
device_id, HourBucket,
-- Number of Steps
(avg(`AvgNumStepsAcrossDevices`) OVER (
        ORDER BY `HourBucket`
        ROWS BETWEEN
          4 PRECEDING AND
          CURRENT ROW
      )) ::float AS SmoothedNumSteps4HourMA, -- 4 hour moving average
      
(avg(`AvgNumStepsAcrossDevices`) OVER (
        ORDER BY `HourBucket`
        ROWS BETWEEN
          24 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedNumSteps24HourMA --24 hour moving average
,
-- Calories Burned
(avg(`AvgCaloriesBurnedAcrossDevices`) OVER (
        ORDER BY `HourBucket`
        ROWS BETWEEN
          4 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedCalsBurned4HourMA, -- 4 hour moving average
      
(avg(`AvgCaloriesBurnedAcrossDevices`) OVER (
        ORDER BY `HourBucket`
        ROWS BETWEEN
          24 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedCalsBurned24HourMA --24 hour moving average,
,
-- Miles Walked
(avg(`AvgMilesWalkedAcrossDevices`) OVER (
        ORDER BY `HourBucket`
        ROWS BETWEEN
          4 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedMilesWalked4HourMA, -- 4 hour moving average
      
(avg(`AvgMilesWalkedAcrossDevices`) OVER (
        ORDER BY `HourBucket`
        ROWS BETWEEN
          24 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedMilesWalked24HourMA --24 hour moving average
FROM {{ ref('gold_hourly_summary_stats_7_day_rolling') }}
WHERE HourBucket >= ((SELECT MAX(HourBucket) FROM {{ ref('gold_hourly_summary_stats_7_day_rolling') }}) - INTERVAL '3 DAYS')