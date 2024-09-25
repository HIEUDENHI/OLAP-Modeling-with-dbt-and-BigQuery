-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`India`.`payment_dim` as DBT_INTERNAL_DEST
        using (

WITH payment_dim_raw AS (
    SELECT 
        Payment_Method,
        Payment_Provider,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_us`  
    UNION ALL
    SELECT 
        Payment_Method,
        Payment_Provider,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_fr`  
    UNION ALL
    SELECT 
        Payment_Method,
        Payment_Provider,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_in`  
)


SELECT 
    DISTINCT
    to_hex(md5(cast(coalesce(cast(Payment_Method as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Payment_Provider as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS payment_id_pk,  -- Generate a surrogate key
    Payment_Method,
    Payment_Provider,
    Country,
    Region,
    'Y' AS is_active  -- Mark all records as active
FROM payment_dim_raw


-- For incremental runs, get the existing customer_dim data

    WHERE to_hex(md5(cast(coalesce(cast(Payment_Method as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Payment_Provider as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) NOT IN (
        SELECT payment_id_pk FROM `data-435516`.`India`.`payment_dim`
    )

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`payment_id_pk`, `Payment_Method`, `Payment_Provider`, `Country`, `Region`, `is_active`)
    values
        (`payment_id_pk`, `Payment_Method`, `Payment_Provider`, `Country`, `Region`, `is_active`)


    