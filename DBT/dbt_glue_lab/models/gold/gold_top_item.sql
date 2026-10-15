{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    iceberg_expire_snapshots='False', 
    table_properties={'format-version': '2'},
    custom_location='s3://' ~ env_var('DBT_S3_BUCKET') ~ '/gold/gold_top_item/'
) }}

-- Use CTEs
WITH views_base AS (
    SELECT
        event_year,
        -- Cast the parameter_value to match the item_id type
        CAST(parameter_value AS BIGINT) AS item_id,
        platform
    FROM
        {{ ref('silver_event') }}
    WHERE
        event_name = 'view_item'
        AND parameter_name = 'item_id'
        AND parameter_value IS NOT NULL
),

total_views AS (
    SELECT
        item_id,
        event_year,
        COUNT(*) AS total_views
    FROM
        views_base
    GROUP BY
        item_id, event_year
),

platform_counts AS (
    SELECT
        item_id,
        event_year,
        platform,
        COUNT(*) AS platform_views
    FROM
        views_base
    GROUP BY
        item_id, event_year, platform
),

top_platform AS (
    SELECT
        item_id,
        event_year,
        platform AS most_used_platform
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY item_id, event_year
                ORDER BY platform_views DESC
            ) as rn
        FROM
            platform_counts
    )
    WHERE
        rn = 1
),

final_datamart AS (
    SELECT
        tv.item_id,
        tv.event_year,
        tv.total_views,
        tp.most_used_platform,
        -- replicate the final window function
        RANK() OVER (
            PARTITION BY tv.event_year
            ORDER BY tv.total_views DESC
        ) as item_rank_in_year
    FROM
        total_views AS tv
    LEFT JOIN
        top_platform AS tp
        ON tv.item_id = tp.item_id AND tv.event_year = tp.event_year
)

-- final join to add item names
SELECT
    dm.item_id,
    si.item_name,
    si.category,
    dm.event_year,
    dm.total_views,
    dm.item_rank_in_year,
    dm.most_used_platform
FROM
    final_datamart AS dm
LEFT JOIN
    {{ ref('silver_item') }} AS si
    ON dm.item_id = si.item_id