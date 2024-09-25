-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`India`.`int_sales_fr` as DBT_INTERNAL_DEST
        using (WITH sales_fr AS (
    SELECT 
        s.*, 
        'FR' AS Country, 
        'EU' AS Region,
        (s.Order_Amount / r.USD_to_Euro) AS US_TOTAL_ORDER_AMT,  -- Calculating US Total Order Amount
        (s.Tax / r.USD_to_Euro) AS USD_TAX_AMT   
    FROM `data-435516`.`India`.`staging_json_data` s
    LEFT JOIN `data-435516`.`India`.`rate_exchange_data` r
        ON s.Order_Date = r.Date
    WHERE s.Payment_Status = 'Paid' 
      AND s.Shipping_Status = 'Delivered'
)

SELECT *
FROM sales_fr
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`Order_ID`, `Customer_Name`, `Mobile_Model`, `Quantity`, `Price_per_Unit`, `Total_Price`, `Promotion_Code`, `Order_Amount`, `Tax`, `Order_Date`, `Payment_Status`, `Shipping_Status`, `Payment_Method`, `Payment_Provider`, `Phone`, `Delivery_Address`, `Country`, `Region`, `US_TOTAL_ORDER_AMT`, `USD_TAX_AMT`)
    values
        (`Order_ID`, `Customer_Name`, `Mobile_Model`, `Quantity`, `Price_per_Unit`, `Total_Price`, `Promotion_Code`, `Order_Amount`, `Tax`, `Order_Date`, `Payment_Status`, `Shipping_Status`, `Payment_Method`, `Payment_Provider`, `Phone`, `Delivery_Address`, `Country`, `Region`, `US_TOTAL_ORDER_AMT`, `USD_TAX_AMT`)


    