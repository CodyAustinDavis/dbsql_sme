{{ 
  config(
    materialized='incremental',
    unique_key='userid',
    incremental_strategy='merge',
    liquid_clustered_by = 'userid',
    pre_hook=["{{ create_bronze_users_identity_table() }}",

              "{{ databricks_copy_into(target_table='bronze_users',
                    source='/databricks-datasets/iot-stream/data-user/',
                    file_format='csv',
                    expression_list = 'userid::bigint AS userid, gender AS gender, age::integer AS age, height::decimal(10,2) AS height, weight::decimal(10,2) AS weight, smoker AS smoker, familyhistory AS familyhistory, cholestlevs AS cholestlevs, bp AS bp, risk::decimal(10,2) AS risk, now() AS ingest_timestamp',
                    copy_options={'force': 'true'},
                    format_options={'header': 'true'}
                    ) }}",

              "OPTIMIZE {{target.catalog}}.{{target.schema}}.bronze_users"
    ],
    post_hook=[
        "OPTIMIZE {{ this }}",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
  ) 
}}


WITH de_dup (
SELECT 
      userid::bigint,
      gender::string,
      age::int,
      height::decimal, 
      weight::decimal,
      smoker,
      familyhistory,
      cholestlevs,
      bp,
      risk,
      ingest_timestamp,
       ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ingest_timestamp DESC) AS DupRank
      FROM {{target.catalog}}.{{target.schema}}.bronze_users
      -- Add Incremental Processing Macro here
      {% if is_incremental() %}

        WHERE ingest_timestamp > (SELECT MAX(ingest_timestamp) FROM {{ this }})

      {% endif %}
)              
SELECT *
FROM de_dup
WHERE DupRank = 1