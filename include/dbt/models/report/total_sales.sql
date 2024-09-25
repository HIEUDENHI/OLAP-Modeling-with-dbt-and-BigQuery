SELECT ROUND(SUM(US_TOTAL_ORDER_AMT),0) AS Total_Sales
FROM {{ref('sales_fact')}} sf

JOIN {{ref('date_dim')}} dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020 AND dd.order_month=1