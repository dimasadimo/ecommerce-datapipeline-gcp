{{ 
    config(
        materialized='incremental',
        partition_by={
        "field": "created_at",
        "data_type": "timestamp"
        }
    ) 
}}

WITH source_data AS (
    SELECT 
        transaction_id,
        created_at,
        customer_id,
        session_id,
        product_metadata,
        payment_method,
        payment_status,
        shipment_fee,
        shipment_location_lat AS shipment_latitude,
        shipment_location_long AS shipment_longitude,
        total_amount
    FROM {{ source('jcdeol03_finalproject_dimasadihartomo', 'transaction_stg') }}
),

cleaned_and_transformed AS (
    SELECT 
        transaction_id,
        created_at,
        customer_id,
        session_id,
        CAST(JSON_EXTRACT_SCALAR(product_data, '$.product_id') AS INT64) AS product_id,
        CAST(JSON_EXTRACT_SCALAR(product_data, '$.quantity') AS INT64) AS product_quantity,
        CAST(JSON_EXTRACT_SCALAR(product_data, '$.item_price') AS INT64) AS product_price,
        payment_method,
        LOWER(payment_status) as payment_status,
        shipment_fee,
        shipment_latitude,
        shipment_longitude,
        total_amount
    FROM 
        source_data s,
        UNNEST(JSON_EXTRACT_ARRAY(s.product_metadata)) AS product_data

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM cleaned_and_transformed