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
    SELECT * 
    FROM {{ source('jcdeol03_finalproject_dimasadihartomo', 'session_stg') }}

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source_data