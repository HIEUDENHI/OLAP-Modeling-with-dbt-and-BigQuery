SELECT dd.Order_Date, SUM(sf.US_TOTAL_ORDER_AMT) AS Total_Sales
FROM {{ref('sales_fact')}} sf

JOIN {{ref('date_dim')}} dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020
GROUP BY dd.Order_Date