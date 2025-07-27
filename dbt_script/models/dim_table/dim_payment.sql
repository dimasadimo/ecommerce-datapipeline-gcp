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
        payment_method_id AS payment_id,
        method_name,
        provider AS payment_provider,
        payment_type,
        created_at
    FROM
        {{ source('jcdeol03_finalproject_dimasadihartomo', 'payment_stg') }}
    
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source_data_cleaned