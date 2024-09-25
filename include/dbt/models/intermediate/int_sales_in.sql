{{ config(
    materialized='incremental',
    unique_key='Order_ID'
) }}

WITH sales_in AS (
    SELECT 
        s.*, 
        'IN' AS Country, 
        'APAC' AS Region,
        (s.Order_Amount / r.USD_to_Rupee) AS US_TOTAL_ORDER_AMT,  -- Calculating US Total Order Amount
        (s.Tax / r.USD_to_Rupee) AS USD_TAX_AMT                
    FROM {{ source('mobile', 'staging_csv_data') }} s
    LEFT JOIN {{ source('mobile', 'rate_exchange_data') }} r
        ON s.Order_Date = r.Date
    WHERE s.Payment_Status = 'Paid' 
      AND s.Shipping_Status = 'Delivered'
    
    {% if is_incremental() %}
      AND s.Order_Date > (SELECT MAX(Order_Date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM sales_in
