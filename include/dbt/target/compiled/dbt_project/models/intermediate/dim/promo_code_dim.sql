

WITH promo_code_dim_raw AS (
    SELECT 
        CASE 
            WHEN Promotion_Code IS NULL THEN 'NA'
            ELSE Promotion_Code
        END AS Promotion_Code,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_us` 
    UNION ALL
    SELECT 
        CASE 
            WHEN Promotion_Code IS NULL THEN 'NA'
            ELSE Promotion_Code
        END AS Promotion_Code,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_fr` 
    UNION ALL
    SELECT 
        CASE 
            WHEN Promotion_Code IS NULL THEN 'NA'
            ELSE Promotion_Code
        END AS Promotion_Code,
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_in` 
)

SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(Promotion_Code as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS promo_code_id_pk,  -- Generate a surrogate key
    Promotion_Code,
    Country,
    Region,
    'Y' AS is_active  -- Static column indicating the promotion is active
FROM promo_code_dim_raw

-- Incremental logic: Only insert rows where `promo_code_id_pk` does not already exist in the target table

    WHERE to_hex(md5(cast(coalesce(cast(Promotion_Code as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) NOT IN (
        SELECT promo_code_id_pk FROM `data-435516`.`India`.`promo_code_dim`
    )
