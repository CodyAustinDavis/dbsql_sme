-- This is ONLY the SELECT expression in COPY INTO without the word "SELECT"
id::bigint AS Id,
device_id::integer AS device_id,
user_id::integer AS user_id,
calories_burnt::decimal(10,2) AS calories_burnt, 
miles_walked::decimal(10,2) AS miles_walked, 
num_steps::decimal(10,2) AS num_steps, 
timestamp::timestamp AS timestamp,
value  AS value -- This is a JSON object