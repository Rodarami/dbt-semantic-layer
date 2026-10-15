{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    iceberg_expire_snapshots='False', 
    table_properties={'format-version': '2'},
    partition_by=['created_at_date'],
    custom_location='s3://<BUCKET>/silver/silver_item/'
) }}


SELECT
    CAST(id AS BIGINT) AS item_id,
    name AS item_name,
    category,
    COALESCE(adjective, 'unknown') AS adjective,
    COALESCE(modifier, 'unknown') AS modifier,
    CAST(price AS DECIMAL(10, 2)) AS price,
    date_format(created_at, 'yyyy-MM-dd HH:mm:ss') AS created_at_ts,
    date(created_at) AS created_at_date

FROM
    -- reads from models/bronze/bronze_sources.yml
    {{ source('raw', 'items') }}