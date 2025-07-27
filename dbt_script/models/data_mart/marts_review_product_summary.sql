{{ 
    config(
        materialized='table'
    )
}}

WITH review_product AS (
    SELECT
        r.review_id,
        r.customer_id,
        r.rating,
        r.review_text,
        r.sentiment_score,
        r.is_flagged,
        r.created_at AS review_date,
        p.category,
        p.sub_category,
        p.product_name,
        p.color,
        p.season,
        p.product_year,
        p.product_use
    FROM
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'fact_review') }} r
    LEFT JOIN
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_product') }} p
    ON
        r.product_id = p.product_id
),

review_product_summary AS (
    SELECT
        rp.*,
        c.full_name,
        c.gender AS customer_gender,
        c.birthdate,
        c.age,
        c.device_type,
        c.device_version,
        c.address_latitude,
        c.address_longitude,
        c.province,
        c.registered_date AS customer_registered_date
    FROM
        review_product rp
    LEFT JOIN
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'dim_customer') }} c
    ON
        rp.customer_id = c.customer_id
)

SELECT * FROM review_product_summary