{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    iceberg_expire_snapshots='False', 
    table_properties={'format-version': '2'},
    custom_location='s3://' ~ env_var('DBT_S3_BUCKET') ~ '/silver/metricflow_time_spine/'

) }}

-- Time spine requerido por MetricFlow (granularidad diaria, 2010-01-01 a 2030-12-31).
WITH days AS (
    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
)

SELECT cast(date_day AS date) AS date_day
FROM days
