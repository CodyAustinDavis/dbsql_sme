{{ 
  config(
    materialized='incremental',
    unique_key='Id',
    incremental_strategy='merge',
    tblproperties={'delta.tuneFileSizesForRewrites': 'true', 'delta.feature.allowColumnDefaults': 'supported', 'delta.columnMapping.mode' : 'name'},
    liquid_clustered_by = 'timestamp, id, device_id',
    incremental_predicates= ["DBT_INTERNAL_DEST.timestamp > dateadd(day, -7, now())"],
    pre_hook=["{{ create_bronze_sensors_identity_table() }}",

              "{{ databricks_copy_into(target_table='bronze_sensors',
                    source='/databricks-datasets/iot-stream/data-device/',
                    file_format='json',
                    expression_list = 'id::bigint AS Id, device_id::integer AS device_id, user_id::integer AS user_id, calories_burnt::decimal(10,2) AS calories_burnt, miles_walked::decimal(10,2) AS miles_walked, num_steps::decimal(10,2) AS num_steps, timestamp::timestamp AS timestamp, value  AS value, now() AS ingest_timestamp',
                    copy_options={'force': 'true'}
                    ) }}",

              "OPTIMIZE {{target.catalog}}.{{target.schema}}.bronze_sensors",

              "ANALYZE TABLE {{target.catalog}}.{{target.schema}}.bronze_sensors COMPUTE STATISTICS FOR ALL COLUMNS"
    ],
    post_hook=[
        "OPTIMIZE {{ this }}",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
  ) 
}}


WITH de_dup (
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              ingest_timestamp,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY ingest_timestamp DESC, timestamp DESC) AS DupRank
              FROM {{target.catalog}}.{{target.schema}}.bronze_sensors
              -- Add Incremental Processing Macro here
              {% if is_incremental() %}

                WHERE ingest_timestamp > (SELECT MAX(ingest_timestamp) FROM {{ this }})

              {% endif %}
              )
              
SELECT Id, device_id, user_id, calories_burnt, miles_walked, num_steps, timestamp, value, ingest_timestamp
-- optional
/*
sha2(CONCAT(COALESCE(Id, ''), COALESCE(device_id, ''))) AS composite_key -- use this as the key if you have composite key
*/
FROM de_dup
WHERE DupRank = 1