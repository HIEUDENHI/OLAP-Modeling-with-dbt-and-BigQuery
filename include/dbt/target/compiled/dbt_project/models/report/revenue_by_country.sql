SELECT rd.Country, ROUND(SUM(sf.US_TOTAL_ORDER_AMT),0) AS Total_Sales
FROM `data-435516`.`Mobile`.`sales_fact` sf

JOIN `data-435516`.`Mobile`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
JOIN `data-435516`.`Mobile`.`region_dim` rd ON rd.region_id_pk=sf.region_id_pk
WHERE dd.order_year =2020
GROUP BY rd.Country