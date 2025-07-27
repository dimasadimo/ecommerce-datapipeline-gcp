{{ 
    config(
        materialized='table'
    )
}}

WITH customer_product_summary AS (
    SELECT
        t.customer_id,
        p.product_id,
        p.category,
        p.sub_category,
        p.product_name,
        p.gender,
        p.color,
        p.season,
        p.product_year,
        p.product_use,
        COUNT(t.product_id) AS number_of_purchases,
        SUM(t.product_quantity) AS total_quantity_purchased
    FROM 
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'fact_transaction') }} t
    LEFT JOIN 
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_product') }} p
    ON  
        t.product_id = p.product_id
    GROUP BY
        1,2,3,4,5,6,7,8,9,10
    ORDER BY 
        t.customer_id DESC
)

SELECT * FROM customer_product_summary
