-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`India`.`daily_sales_quantity` as DBT_INTERNAL_DEST
        using (SELECT dd.Order_Date, SUM(sf.Quantity) AS Sales_Quantity
FROM `data-435516`.`India`.`sales_fact` sf

JOIN `data-435516`.`India`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020
GROUP BY dd.Order_Date
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`Order_Date`, `Sales_Quantity`)
    values
        (`Order_Date`, `Sales_Quantity`)


    