SELECT dd.Order_Date, SUM(sf.US_TOTAL_ORDER_AMT) AS Total_Sales
FROM `data-435516`.`Mobile`.`sales_fact` sf

JOIN `data-435516`.`Mobile`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020
GROUP BY dd.Order_Date