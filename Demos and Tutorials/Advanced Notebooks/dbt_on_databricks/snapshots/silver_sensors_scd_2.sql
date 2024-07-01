{% snapshot sensors_snapshot %}

{{
    config(
      target_schema= target.schema + '_snapshots',
      unique_key='Id',

      strategy='timestamp',
      updated_at='ingest_timestamp',
    )
}}

select * from {{ source('dbt_optimized', 'silver_sensors_scd_1') }}

{% endsnapshot %}