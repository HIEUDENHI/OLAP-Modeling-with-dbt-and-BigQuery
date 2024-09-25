-- back compat for old kwarg name
  
  
        
    

    

    merge into `data-435516`.`India`.`customer_dim` as DBT_INTERNAL_DEST
        using (-- This ensures the model runs incrementally


WITH customer_dim_raw AS (
    SELECT 
        Customer_Name,
        Phone,
        Delivery_Address,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_us`  
    UNION ALL
    SELECT 
        Customer_Name,
        Phone,
        Delivery_Address,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_fr`  
    UNION ALL
    SELECT 
        Customer_Name,
        Phone,
        Delivery_Address,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_in`  
)

SELECT 
    DISTINCT
    to_hex(md5(cast(coalesce(cast(Customer_Name as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Phone as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Delivery_Address as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS customer_id_pk,  -- Generate a surrogate key
    Customer_Name,
    Phone,
    Delivery_Address,
    Country,
    Region,
    'Y' AS is_active  -- Mark all records as active
FROM customer_dim_raw

-- For incremental runs, get the existing customer_dim data

    WHERE to_hex(md5(cast(coalesce(cast(Customer_Name as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Phone as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Delivery_Address as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) NOT IN (
        SELECT customer_id_pk FROM `data-435516`.`India`.`customer_dim`
    )

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`customer_id_pk`, `Customer_Name`, `Phone`, `Delivery_Address`, `Country`, `Region`, `is_active`)
    values
        (`customer_id_pk`, `Customer_Name`, `Phone`, `Delivery_Address`, `Country`, `Region`, `is_active`)


    