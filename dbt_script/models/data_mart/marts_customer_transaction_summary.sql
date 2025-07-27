{{ 
  config(
    materialized='table'
  )
}}

WITH customer_transaction_summary AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.gender,
        c.age,
        c.province,
        c.registered_date,
        SUM(t.total_amount) AS total_spent,
        COUNT(DISTINCT t.created_at) AS number_of_transactions,
        AVG(t.total_amount) AS average_transaction_value,
        MIN(t.created_at) AS first_transaction_date,
        MAX(t.created_at) AS last_transaction_date
    FROM 
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_customer') }} AS c
    LEFT JOIN 
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'fact_transaction') }} AS t
    ON  
        c.customer_id = t.customer_id
    GROUP BY
        1,2,3,4,5,6,7
    ORDER BY
        total_spent DESC,
        number_of_transactions DESC
)

SELECT * FROM customer_transaction_summary