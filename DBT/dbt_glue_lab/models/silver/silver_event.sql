{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    iceberg_expire_snapshots='False', 
    table_properties={'format-version': '2'},
    partition_by=['event_date'],
    custom_location='s3://<BUCKET>/silver/silver_event/'
) }}


SELECT
    event_id,
    date_format(event_time, 'yyyy-MM-dd HH:mm:ss') AS event_at_ts,
    CAST(user_id AS BIGINT) AS user_id,
    get_json_object(event_payload, '$.event_name') AS event_name,
    get_json_object(event_payload, '$.platform') AS platform,
    get_json_object(event_payload, '$.parameter_name') AS parameter_name,
    get_json_object(event_payload, '$.parameter_value') AS parameter_value,
    year(event_time) AS event_year,
    date(event_time) AS event_date
                
FROM
    glue_catalog.{{ source('raw', 'events') }}
WHERE
    event_time IS NOT NULL