{{ config(
    materialized='incremental',
    unique_key='Order_ID'  
) }}

WITH sales_fr AS (
    SELECT 
        s.*, 
        'FR' AS Country, 
        'EU' AS Region,
        (s.Order_Amount / r.USD_to_Euro) AS US_TOTAL_ORDER_AMT,  
        (s.Tax / r.USD_to_Euro) AS USD_TAX_AMT   
    FROM {{ source('mobile', 'staging_json_data') }} s
    LEFT JOIN {{ source('mobile', 'rate_exchange_data') }} r
        ON s.Order_Date = r.Date
    WHERE s.Payment_Status = 'Paid' 
      AND s.Shipping_Status = 'Delivered'
      
    {% if is_incremental() %}
      AND s.Order_Date > (SELECT MAX(Order_Date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM sales_fr
