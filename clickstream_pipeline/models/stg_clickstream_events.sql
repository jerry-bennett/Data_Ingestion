{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT 
        event_id::VARCHAR AS event_id,
        timestamp::TIMESTAMP AS event_timestamp,
        event_name::VARCHAR AS event_name,
        user_context.user_id::VARCHAR AS user_id,
        user_context.session_id::VARCHAR AS session_id,
        user_context.device::VARCHAR AS device,
        event_properties.product_id::VARCHAR AS product_id,
        event_properties.value::DOUBLE AS event_value
    FROM {{ source('raw_tracking', 'bronze_events') }}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM raw_source
)

SELECT
    event_id,
    event_timestamp,
    event_name,
    user_id,
    session_id,
    device,
    product_id,
    event_value
FROM deduplicated
WHERE rn = 1