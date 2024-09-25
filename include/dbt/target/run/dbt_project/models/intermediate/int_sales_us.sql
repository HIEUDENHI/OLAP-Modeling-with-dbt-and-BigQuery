-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`India`.`int_sales_us` as DBT_INTERNAL_DEST
        using (WITH sales_us_formated AS(
    SELECT Order_ID,
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
    FROM `data-435516`.`India`.`staging_parquet_data` s
),
sales_us AS (
    SELECT 
        sd.*, 
        'US' AS Country, 
        'NA' AS Region,
        sd.Order_Amount AS US_TOTAL_ORDER_AMT,  -- Calculating US Total Order Amount
        sd.Tax / r.USD_to_Rupee AS USD_TAX_AMT   
    FROM sales_us_formated sd
    LEFT JOIN `data-435516`.`India`.`rate_exchange_data` r
        ON sd.Order_Date = r.Date
    WHERE sd.Payment_Status = 'Paid' 
      AND sd.Shipping_Status = 'Delivered'
)

SELECT *
FROM sales_us
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`Order_ID`, `Customer_Name`, `Mobile_Model`, `Quantity`, `Price_per_Unit`, `Total_Price`, `Promotion_Code`, `Order_Amount`, `Tax`, `Order_Date`, `Payment_Status`, `Shipping_Status`, `Payment_Method`, `Payment_Provider`, `Phone`, `Delivery_Address`, `Country`, `Region`, `US_TOTAL_ORDER_AMT`, `USD_TAX_AMT`)
    values
        (`Order_ID`, `Customer_Name`, `Mobile_Model`, `Quantity`, `Price_per_Unit`, `Total_Price`, `Promotion_Code`, `Order_Amount`, `Tax`, `Order_Date`, `Payment_Status`, `Shipping_Status`, `Payment_Method`, `Payment_Provider`, `Phone`, `Delivery_Address`, `Country`, `Region`, `US_TOTAL_ORDER_AMT`, `USD_TAX_AMT`)


    