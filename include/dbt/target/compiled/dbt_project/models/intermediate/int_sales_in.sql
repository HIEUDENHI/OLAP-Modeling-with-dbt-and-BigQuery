WITH sales_in AS (
    SELECT 
        s.*, 
        'IN' AS Country, 
        'APAC' AS Region,
        (s.Order_Amount / r.USD_to_Rupee) AS US_TOTAL_ORDER_AMT,  -- Calculating US Total Order Amount
        (s.Tax / r.USD_to_Rupee) AS USD_TAX_AMT                
    FROM `data-435516`.`India`.`staging_csv_data` s
    LEFT JOIN `data-435516`.`India`.`rate_exchange_data` r
        ON s.Order_Date = r.Date
    WHERE s.Payment_Status = 'Paid' 
      AND s.Shipping_Status = 'Delivered'
)

SELECT *
FROM sales_in