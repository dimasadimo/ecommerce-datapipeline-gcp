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
        customer_id,
        first_name,
        last_name,
        username,
        email,
        gender,
        birthdate,
        device_type,
        device_version,
        home_location_lat AS address_latitude,
        home_location_long AS address_longitude,
        home_location AS province,
        home_country AS country,
        first_join_date AS registered_date,
        created_at
    FROM
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'customer_stg') }}
),

cleaned_and_transformed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        username,
        email,
        CASE
            WHEN gender = 'M' THEN 'Male'
            WHEN gender = 'F' THEN 'Female'
            ELSE 'Unknown'
        END AS gender,
        DATE_DIFF(CURRENT_DATE(), birthdate, YEAR) AS age,
        birthdate,
        device_type,
        device_version,
        address_latitude,
        address_longitude,
        province,
        country,
        registered_date,
        created_at,

    FROM
        source_data
    
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM cleaned_and_transformed