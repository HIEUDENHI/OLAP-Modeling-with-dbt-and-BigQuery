{{ config(
    materialized='incremental'
) }}

WITH product_dim_raw AS (
    SELECT
        Mobile_Model,
        SPLIT(Mobile_Model, '/')[OFFSET(0)] AS Brand,
        SPLIT(Mobile_Model, '/')[OFFSET(1)] AS Model,
        SPLIT(Mobile_Model, '/')[OFFSET(2)] AS Color,
        SPLIT(Mobile_Model, '/')[OFFSET(3)] AS Ram,
        SPLIT(Mobile_Model, '/')[OFFSET(4)] AS Rom
    FROM {{ ref('int_sales_us') }}
    UNION ALL
    SELECT
        Mobile_Model,
        SPLIT(Mobile_Model, '/')[OFFSET(0)] AS Brand,
        SPLIT(Mobile_Model, '/')[OFFSET(1)] AS Model,
        SPLIT(Mobile_Model, '/')[OFFSET(2)] AS Color,
        SPLIT(Mobile_Model, '/')[OFFSET(3)] AS Ram,
        SPLIT(Mobile_Model, '/')[OFFSET(4)] AS Rom
    FROM {{ ref('int_sales_in') }}
    UNION ALL
    SELECT
        Mobile_Model,
        SPLIT(Mobile_Model, '/')[OFFSET(0)] AS Brand,
        SPLIT(Mobile_Model, '/')[OFFSET(1)] AS Model,
        SPLIT(Mobile_Model, '/')[OFFSET(2)] AS Color,
        SPLIT(Mobile_Model, '/')[OFFSET(3)] AS Ram,
        SPLIT(Mobile_Model, '/')[OFFSET(4)] AS Rom
    FROM {{ ref('int_sales_fr') }}
)

SELECT
    DISTINCT
    {{ dbt_utils.generate_surrogate_key(['Mobile_Model']) }} AS product_id_pk,  -- Generate surrogate key
    Mobile_Model,
    Brand,
    Model,
    Color,
    Ram,
    Rom,
    'Y' AS isActive
FROM product_dim_raw

-- Incremental logic: Insert only if the surrogate key doesn't already exist
{% if is_incremental() %}
    WHERE {{ dbt_utils.generate_surrogate_key(['Mobile_Model']) }} NOT IN (
        SELECT product_id_pk FROM {{ this }}
    )
{% endif %}
