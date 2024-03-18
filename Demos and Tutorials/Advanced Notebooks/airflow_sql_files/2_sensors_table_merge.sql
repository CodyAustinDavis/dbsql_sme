MERGE INTO main.iot_dashboard_airflow.silver_sensors AS target
USING (
WITH de_dup (
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY timestamp DESC) AS DupRank
              FROM main.iot_dashboard_airflow.bronze_sensors
              )
              
SELECT Id, device_id, user_id, calories_burnt, miles_walked, num_steps, timestamp, value
FROM de_dup
WHERE DupRank = 1
) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;