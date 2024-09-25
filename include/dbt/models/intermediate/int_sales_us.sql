{{ config(
    materialized='incremental',
    unique_key='Order_ID'
) }}

WITH sales_us_formated AS (
    SELECT 
        Order_ID,
        Customer_Name,
        Mobile_Model,
        Quantity,
        Price_per_Unit,
        Total_Price,
        Promotion_Code,
        Order_Amount,
        Tax,
        CAST(Order_Date AS Date) AS Order_Date,
        Payment_Status,
        Shipping_Status,
        Payment_Method,
        Payment_Provider,
        Phone,
        Delivery_Address
    FROM {{ source('mobile', 'staging_parquet_data') }} s
),
sales_us AS (
    SELECT 
        sd.*, 
        'US' AS Country, 
        'NA' AS Region,
        sd.Order_Amount AS US_TOTAL_ORDER_AMT,  
        sd.Tax / r.USD_to_Rupee AS USD_TAX_AMT   
    FROM sales_us_formated sd
    LEFT JOIN {{ source('mobile', 'rate_exchange_data') }} r
        ON sd.Order_Date = r.Date
    WHERE sd.Payment_Status = 'Paid' 
      AND sd.Shipping_Status = 'Delivered'
    
    {% if is_incremental() %}
      AND sd.Order_Date > (SELECT MAX(Order_Date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM sales_us
