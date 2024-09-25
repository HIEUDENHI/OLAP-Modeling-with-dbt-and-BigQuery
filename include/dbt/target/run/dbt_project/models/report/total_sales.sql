-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`India`.`total_sales` as DBT_INTERNAL_DEST
        using (SELECT ROUND(SUM(US_TOTAL_ORDER_AMT),0) AS Total_Sales
FROM `data-435516`.`India`.`sales_fact` sf

JOIN `data-435516`.`India`.`date_dim` dd ON dd.Order_Date=sf.Order_Date
WHERE dd.order_year =2020 AND dd.order_month=1
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`Total_Sales`)
    values
        (`Total_Sales`)


    