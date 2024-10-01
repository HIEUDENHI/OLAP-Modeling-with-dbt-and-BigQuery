-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`Mobile`.`daily_sales_sum` as DBT_INTERNAL_DEST
        using (SELECT dd.Order_Date, SUM(sf.US_TOTAL_ORDER_AMT) AS Total_Sales
FROM `data-435516`.`Mobile`.`sales_fact` sf

JOIN `data-435516`.`Mobile`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020
GROUP BY dd.Order_Date
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`Order_Date`, `Total_Sales`)
    values
        (`Order_Date`, `Total_Sales`)


    