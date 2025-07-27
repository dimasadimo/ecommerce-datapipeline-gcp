{{ 
    config(
        materialized='incremental',
        partition_by={
        "field": "created_at",
        "data_type": "timestamp"
        }
    ) 
}}

WITH source_data_cleaned AS (
    SELECT
        id AS product_id,
        gender,
        masterCategory AS category,
        subCategory AS sub_category,
        articleType AS product_name,
        baseColour AS color,
        season,
        year AS product_year,
        usage AS product_use,
        created_at
    FROM
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'product_stg') }}

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source_data_cleaned