{{
  config(
    materialized='incremental')
}}

WITH region_dim_raw AS (
    -- Combine distinct Country and Region data from multiple sources
    SELECT 
        Country,
        Region
    FROM {{ ref('int_sales_us') }}
    UNION ALL
    SELECT 
        Country,
        Region
    FROM {{ ref('int_sales_in') }}
    UNION ALL
    SELECT 
        Country,
        Region
    FROM {{ ref('int_sales_fr') }}
)
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['Country', 'Region']) }} AS region_id_pk,
        Country,
        Region,
        'Y' AS isActive  -- Static column to indicate active regions
    FROM region_dim_raw



-- Incremental logic: Insert only new records where the surrogate key doesn't already exist
{% if is_incremental() %}
    WHERE {{ dbt_utils.generate_surrogate_key(['Country', 'Region']) }} NOT IN (
        SELECT region_id_pk FROM {{ this }}
    )
{% endif %}
