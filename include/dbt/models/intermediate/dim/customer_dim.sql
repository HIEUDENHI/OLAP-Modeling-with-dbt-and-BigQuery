-- This ensures the model runs incrementally
{{ 
  config(
    materialized='incremental'
  ) 
}}

WITH customer_dim_raw AS (
    SELECT 
        Customer_Name,
        Phone,
        Delivery_Address,
        Country,
        Region
    FROM {{ ref('int_sales_us') }}  
    UNION ALL
    SELECT 
        Customer_Name,
        Phone,
        Delivery_Address,
        Country,
        Region
    FROM {{ ref('int_sales_fr') }}  
    UNION ALL
    SELECT 
        Customer_Name,
        Phone,
        Delivery_Address,
        Country,
        Region
    FROM {{ ref('int_sales_in') }}  
)

SELECT 
    DISTINCT
    {{ dbt_utils.generate_surrogate_key(['Customer_Name', 'Phone', 'Delivery_Address', 'Country', 'Region']) }} AS customer_id_pk,  -- Generate a surrogate key
    Customer_Name,
    Phone,
    Delivery_Address,
    Country,
    Region,
    'Y' AS is_active  -- Mark all records as active
FROM customer_dim_raw

-- For incremental runs, get the existing customer_dim data
{% if is_incremental() %}
    WHERE {{ dbt_utils.generate_surrogate_key(['Customer_Name', 'Phone', 'Delivery_Address', 'Country', 'Region']) }} NOT IN (
        SELECT customer_id_pk FROM {{ this }}
    )
{% endif %}
