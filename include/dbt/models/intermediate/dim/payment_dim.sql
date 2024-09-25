{{
  config(
    materialized='incremental' 
  )
}}

WITH payment_dim_raw AS (
    SELECT 
        Payment_Method,
        Payment_Provider,
        Country,
        Region
    FROM {{ ref('int_sales_us') }}  
    UNION ALL
    SELECT 
        Payment_Method,
        Payment_Provider,
        Country,
        Region
    FROM {{ ref('int_sales_fr') }}  
    UNION ALL
    SELECT 
        Payment_Method,
        Payment_Provider,
        Country,
        Region
    FROM {{ ref('int_sales_in') }}  
)


SELECT 
    DISTINCT
    {{ dbt_utils.generate_surrogate_key(['Payment_Method', 'Payment_Provider', 'Country', 'Region']) }} AS payment_id_pk,  -- Generate a surrogate key
    Payment_Method,
    Payment_Provider,
    Country,
    Region,
    'Y' AS is_active  -- Mark all records as active
FROM payment_dim_raw


-- For incremental runs, get the existing customer_dim data
{% if is_incremental() %}
    WHERE {{ dbt_utils.generate_surrogate_key(['Payment_Method', 'Payment_Provider', 'Country', 'Region']) }} NOT IN (
        SELECT payment_id_pk FROM {{ this }}
    )
{% endif %}

