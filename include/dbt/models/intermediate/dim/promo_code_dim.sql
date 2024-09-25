{{
  config(
    materialized='incremental',
    unique_key='promo_code_id_pk'  
  )
}}

WITH promo_code_dim_raw AS (
    SELECT 
        CASE 
            WHEN Promotion_Code IS NULL THEN 'NA'
            ELSE Promotion_Code
        END AS Promotion_Code,
        Country,
        Region
    FROM {{ ref('int_sales_us') }} 
    UNION ALL
    SELECT 
        CASE 
            WHEN Promotion_Code IS NULL THEN 'NA'
            ELSE Promotion_Code
        END AS Promotion_Code,
        Country,
        Region
    FROM {{ ref('int_sales_fr') }} 
    UNION ALL
    SELECT 
        CASE 
            WHEN Promotion_Code IS NULL THEN 'NA'
            ELSE Promotion_Code
        END AS Promotion_Code,
        Country,
        Region
    FROM {{ ref('int_sales_in') }} 
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['Promotion_Code', 'Country', 'Region']) }} AS promo_code_id_pk,  -- Generate a surrogate key
    Promotion_Code,
    Country,
    Region,
    'Y' AS is_active  -- Static column indicating the promotion is active
FROM promo_code_dim_raw

-- Incremental logic: Only insert rows where `promo_code_id_pk` does not already exist in the target table
{% if is_incremental() %}
    WHERE {{ dbt_utils.generate_surrogate_key(['Promotion_Code', 'Country', 'Region']) }} NOT IN (
        SELECT promo_code_id_pk FROM {{ this }}
    )
{% endif %}
