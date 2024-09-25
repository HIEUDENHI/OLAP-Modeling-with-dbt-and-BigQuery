{{ config(
    materialized='incremental'
) }}


WITH fct_sales_cte AS(
    SELECT 
        Order_ID,
        Order_Date,
        {{ dbt_utils.generate_surrogate_key(['Country', 'Region']) }} AS region_id_pk,
        {{ dbt_utils.generate_surrogate_key(['Customer_Name', 'Phone', 'Delivery_Address', 'Country', 'Region']) }} AS customer_id_pk,
        {{ dbt_utils.generate_surrogate_key(['Mobile_Model']) }} AS product_id_pk,  
        {{ dbt_utils.generate_surrogate_key(['Payment_Method', 'Payment_Provider', 'Country', 'Region']) }} AS payment_id_pk,  
        {{ dbt_utils.generate_surrogate_key(['Promotion_Code', 'Country', 'Region']) }} AS promo_code_id_pk, 
        Quantity,
        US_TOTAL_ORDER_AMT,
        USD_TAX_AMT
    FROM {{ ref('int_sales_in') }}  
    UNION ALL
    SELECT 
        Order_ID,
        Order_Date,
        {{ dbt_utils.generate_surrogate_key(['Country', 'Region']) }} AS region_id_pk,
        {{ dbt_utils.generate_surrogate_key(['Customer_Name', 'Phone', 'Delivery_Address', 'Country', 'Region']) }} AS customer_id_pk,  
        {{ dbt_utils.generate_surrogate_key(['Mobile_Model']) }} AS product_id_pk,
        {{ dbt_utils.generate_surrogate_key(['Payment_Method', 'Payment_Provider', 'Country', 'Region']) }} AS payment_id_pk,  
        {{ dbt_utils.generate_surrogate_key(['Promotion_Code', 'Country', 'Region']) }} AS promo_code_id_pk,  
        Quantity,
        US_TOTAL_ORDER_AMT,
        USD_TAX_AMT
    FROM {{ ref('int_sales_us') }}  
    UNION ALL
    SELECT 
        Order_ID,
        Order_Date,
        {{ dbt_utils.generate_surrogate_key(['Country', 'Region']) }} AS region_id_pk,
        {{ dbt_utils.generate_surrogate_key(['Customer_Name', 'Phone', 'Delivery_Address', 'Country', 'Region']) }} AS customer_id_pk, 
        {{ dbt_utils.generate_surrogate_key(['Mobile_Model']) }} AS product_id_pk,
        {{ dbt_utils.generate_surrogate_key(['Payment_Method', 'Payment_Provider', 'Country', 'Region']) }} AS payment_id_pk,  
        {{ dbt_utils.generate_surrogate_key(['Promotion_Code', 'Country', 'Region']) }} AS promo_code_id_pk,  
        Quantity,
        US_TOTAL_ORDER_AMT,
        USD_TAX_AMT
    FROM {{ ref('int_sales_fr') }}  
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
INNER JOIN {{ref('payment_dim')}} p ON p.payment_id_pk=fc.payment_id_pk
INNER JOIN {{ref('date_dim')}} d ON d.Order_Date=fc.Order_Date
INNER JOIN {{ref('promo_code_dim')}} pro ON pro.promo_code_id_pk=fc.promo_code_id_pk
INNER JOIN {{ref('product_dim')}} pr ON pr.product_id_pk=fc.product_id_pk
INNER JOIN {{ref('customer_dim')}} c ON c.customer_id_pk=fc.customer_id_pk
INNER JOIN {{ref('region_dim')}} r ON r.region_id_pk=fc.region_id_pk

{% if is_incremental() %}
    WHERE Order_ID NOT IN (
        SELECT Order_ID FROM {{this}}
    )
{% endif %}