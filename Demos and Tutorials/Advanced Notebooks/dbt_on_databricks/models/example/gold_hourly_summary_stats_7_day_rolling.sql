{{ 
  config(
    materialized='table',
    liquid_clustered_by='device_id, HourBucket'
  )
}}

-- Get hourly aggregates for last 7 days
SELECT device_id,
date_trunc('hour', timestamp) AS HourBucket,
AVG(num_steps)::float AS AvgNumStepsAcrossDevices,
AVG(calories_burnt)::float AS AvgCaloriesBurnedAcrossDevices,
AVG(miles_walked)::float AS AvgMilesWalkedAcrossDevices
FROM {{ ref('silver_sensors_scd_1') }}
WHERE timestamp >= ((SELECT MAX(timestamp) FROM {{ ref('silver_sensors_scd_1') }}) - INTERVAL '7 DAYS')
GROUP BY device_id, date_trunc('hour', timestamp)
ORDER BY HourBucket
