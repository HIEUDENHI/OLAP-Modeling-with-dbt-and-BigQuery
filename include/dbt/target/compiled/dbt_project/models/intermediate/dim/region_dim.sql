

WITH region_dim_raw AS (
    -- Combine distinct Country and Region data from multiple sources
    SELECT 
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_us`
    UNION ALL
    SELECT 
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_in`
    UNION ALL
    SELECT 
        Country,
        Region
    FROM `data-435516`.`India`.`int_sales_fr`
)
    SELECT DISTINCT
        to_hex(md5(cast(coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS region_id_pk,
        Country,
        Region,
        'Y' AS isActive  -- Static column to indicate active regions
    FROM region_dim_raw



-- Incremental logic: Insert only new records where the surrogate key doesn't already exist

    WHERE to_hex(md5(cast(coalesce(cast(Country as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Region as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) NOT IN (
        SELECT region_id_pk FROM `data-435516`.`India`.`region_dim`
    )
