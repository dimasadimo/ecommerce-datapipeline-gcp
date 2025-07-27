{{ 
    config(
        materialized='table'
    )
}}

WITH product_transaction_summary AS (
    SELECT
        p.product_id,
        p.category,
        p.sub_category,
        p.product_name,
        p.gender,
        p.color,
        p.season,
        p.product_year,
        p.product_use,
        SUM(t.product_quantity * t.product_price) AS total_product_revenue,
        SUM(t.product_quantity) AS total_quantity_sold,
        AVG(t.product_price) AS average_product_price
    FROM 
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_product') }} p
    LEFT JOIN 
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'fact_transaction') }} t
    ON  
        p.product_id = t.product_id
    GROUP BY 
        1,2,3,4,5,6,7,8,9
    ORDER BY 
        total_product_revenue DESC, 
        total_quantity_sold DESC
)

SELECT * FROM product_transaction_summary
