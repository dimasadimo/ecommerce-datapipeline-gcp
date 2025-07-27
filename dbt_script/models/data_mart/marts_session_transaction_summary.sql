{{ 
    config(
        materialized='table'
    )
}}

WITH session_transaction_summary AS (
    SELECT
        t.customer_id,
        t.session_id,
        t.created_at AS transaction_date,
        t.total_amount,
        s.traffic_source,
        s.device_type AS session_device_type,
        s.browser,
        s.session_duration,
        s.is_conversion
    FROM
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'fact_transaction') }} t
    LEFT JOIN
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_session') }} s
    ON
        t.session_id = s.session_id
)

SELECT * FROM session_transaction_summary
