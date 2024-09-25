SELECT pr.Model, ROUND(SUM(sf.US_TOTAL_ORDER_AMT),0) AS Total_Sales
FROM `data-435516`.`India`.`sales_fact` sf

JOIN `data-435516`.`India`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
JOIN `data-435516`.`India`.`region_dim` rd ON rd.region_id_pk=sf.region_id_pk
JOIN `data-435516`.`India`.`product_dim` pr ON pr.product_id_pk=sf.product_id_pk
WHERE rd.Country='In' AND dd.order_year =2020
GROUP BY pr.Model