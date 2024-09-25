WITH sales_fr AS (
    SELECT 
        s.*, 
        'FR' AS Country, 
        'EU' AS Region,
        (s.Order_Amount / r.USD_to_Euro) AS US_TOTAL_ORDER_AMT,  -- Calculating US Total Order Amount
        (s.Tax / r.USD_to_Euro) AS USD_TAX_AMT   
    FROM `data-435516`.`Mobile`.`staging_json_data` s
    LEFT JOIN `data-435516`.`Mobile`.`rate_exchange_data` r
        ON s.Order_Date = r.Date
    WHERE s.Payment_Status = 'Paid' 
      AND s.Shipping_Status = 'Delivered'
)

SELECT *
FROM sales_fr