{% snapshot users_snapshot %}

{{
    config(
      target_schema= target.schema + '_snapshots',
      unique_key='userid',

      strategy='check',
      check_cols=['age', 'height', 'weight', 'smoker', 'familyhistory', 'cholestlevs', 'bp', 'risk'],
      updated_at='ingest_timestamp',
    )
}}

select * from {{ source('dbt_optimized', 'silver_users_scd_1') }}

{% endsnapshot %}