SELECT dd.Order_Date, SUM(sf.Quantity) AS Sales_Quantity
FROM `data-435516`.`India`.`sales_fact` sf

JOIN `data-435516`.`India`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020
GROUP BY dd.Order_Date