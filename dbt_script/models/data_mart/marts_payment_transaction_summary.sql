{{ 
    config(
        materialized='table'
    )
}}

WITH payment_transaction_summary AS (
    SELECT
        pm.payment_id,
        pm.method_name,
        pm.payment_provider,
        pm.payment_type,
        SUM(t.total_amount) AS total_revenue,
        COUNT(t.payment_method) AS number_of_transactions,
        COUNTIF(t.payment_status = 'success') AS successful_transactions,
        COUNTIF(t.payment_status = 'failed') AS failed_transactions,
        SAFE_DIVIDE(COUNTIF(t.payment_status = 'success'), COUNT(t.payment_method)) AS success_rate
    FROM
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'fact_transaction') }} t
    LEFT JOIN
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_payment') }} pm
    ON
        t.payment_method = pm.payment_id
    GROUP BY
        1, 2, 3, 4
)

SELECT * FROM payment_transaction_summary
