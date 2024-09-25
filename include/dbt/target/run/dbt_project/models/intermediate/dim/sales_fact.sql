
  
    

    create or replace table `data-435516`.`India`.`sales_fact`
    
    

    OPTIONS()
    as (
      


WITH fct_sales_cte AS(
    SELECT 
        Order_ID,
        Order_Date,
        to_hex(md5(cast(coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS region_id_pk,
        to_hex(md5(cast(coalesce(cast(Customer_Name as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Phone as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Delivery_Address as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS customer_id_pk,
        to_hex(md5(cast(coalesce(cast(Mobile_Model as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS product_id_pk,  
        to_hex(md5(cast(coalesce(cast(Payment_Method as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Payment_Provider as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS payment_id_pk,  
        to_hex(md5(cast(coalesce(cast(Promotion_Code as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS promo_code_id_pk, 
        Quantity,
        US_TOTAL_ORDER_AMT,
        USD_TAX_AMT
    FROM `data-435516`.`India`.`int_sales_in`  
    UNION ALL
    SELECT 
        Order_ID,
        Order_Date,
        to_hex(md5(cast(coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS region_id_pk,
        to_hex(md5(cast(coalesce(cast(Customer_Name as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Phone as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Delivery_Address as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS customer_id_pk,  
        to_hex(md5(cast(coalesce(cast(Mobile_Model as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS product_id_pk,
        to_hex(md5(cast(coalesce(cast(Payment_Method as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Payment_Provider as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS payment_id_pk,  
        to_hex(md5(cast(coalesce(cast(Promotion_Code as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS promo_code_id_pk,  
        Quantity,
        US_TOTAL_ORDER_AMT,
        USD_TAX_AMT
    FROM `data-435516`.`India`.`int_sales_us`  
    UNION ALL
    SELECT 
        Order_ID,
        Order_Date,
        to_hex(md5(cast(coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS region_id_pk,
        to_hex(md5(cast(coalesce(cast(Customer_Name as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Phone as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Delivery_Address as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS customer_id_pk, 
        to_hex(md5(cast(coalesce(cast(Mobile_Model as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS product_id_pk,
        to_hex(md5(cast(coalesce(cast(Payment_Method as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Payment_Provider as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS payment_id_pk,  
        to_hex(md5(cast(coalesce(cast(Promotion_Code as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS promo_code_id_pk,  
        Quantity,
        US_TOTAL_ORDER_AMT,
        USD_TAX_AMT
    FROM `data-435516`.`India`.`int_sales_fr`  
)

SELECT 
    fc.Order_ID,
    fc.Order_Date,
    fc.region_id_pk,
    fc.customer_id_pk,
    fc.product_id_pk,
    fc.payment_id_pk,
    fc.promo_code_id_pk,
    fc.Quantity,
    fc.US_TOTAL_ORDER_AMT,
    fc.USD_TAX_AMT
FROM fct_sales_cte fc
INNER JOIN `data-435516`.`India`.`payment_dim` p ON p.payment_id_pk=fc.payment_id_pk
INNER JOIN `data-435516`.`India`.`date_dim` d ON d.Order_Date=fc.Order_Date
INNER JOIN `data-435516`.`India`.`promo_code_dim` pro ON pro.promo_code_id_pk=fc.promo_code_id_pk
INNER JOIN `data-435516`.`India`.`product_dim` pr ON pr.product_id_pk=fc.product_id_pk
INNER JOIN `data-435516`.`India`.`customer_dim` c ON c.customer_id_pk=fc.customer_id_pk
INNER JOIN `data-435516`.`India`.`region_dim` r ON r.region_id_pk=fc.region_id_pk


    );
  