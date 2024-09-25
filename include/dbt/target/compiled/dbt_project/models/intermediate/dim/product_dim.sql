

WITH product_dim_raw AS (
    SELECT
        Mobile_Model,
        SPLIT(Mobile_Model, '/')[OFFSET(0)] AS Brand,
        SPLIT(Mobile_Model, '/')[OFFSET(1)] AS Model,
        SPLIT(Mobile_Model, '/')[OFFSET(2)] AS Color,
        SPLIT(Mobile_Model, '/')[OFFSET(3)] AS Ram,
        SPLIT(Mobile_Model, '/')[OFFSET(4)] AS Rom
    FROM `data-435516`.`India`.`int_sales_us`
    UNION ALL
    SELECT
        Mobile_Model,
        SPLIT(Mobile_Model, '/')[OFFSET(0)] AS Brand,
        SPLIT(Mobile_Model, '/')[OFFSET(1)] AS Model,
        SPLIT(Mobile_Model, '/')[OFFSET(2)] AS Color,
        SPLIT(Mobile_Model, '/')[OFFSET(3)] AS Ram,
        SPLIT(Mobile_Model, '/')[OFFSET(4)] AS Rom
    FROM `data-435516`.`India`.`int_sales_in`
    UNION ALL
    SELECT
        Mobile_Model,
        SPLIT(Mobile_Model, '/')[OFFSET(0)] AS Brand,
        SPLIT(Mobile_Model, '/')[OFFSET(1)] AS Model,
        SPLIT(Mobile_Model, '/')[OFFSET(2)] AS Color,
        SPLIT(Mobile_Model, '/')[OFFSET(3)] AS Ram,
        SPLIT(Mobile_Model, '/')[OFFSET(4)] AS Rom
    FROM `data-435516`.`India`.`int_sales_fr`
)

SELECT
    DISTINCT
    to_hex(md5(cast(coalesce(cast(Mobile_Model as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS product_id_pk,  -- Generate surrogate key
    Mobile_Model,
    Brand,
    Model,
    Color,
    Ram,
    Rom,
    'Y' AS isActive
FROM product_dim_raw

-- Incremental logic: Insert only if the surrogate key doesn't already exist

    WHERE to_hex(md5(cast(coalesce(cast(Mobile_Model as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) NOT IN (
        SELECT product_id_pk FROM `data-435516`.`India`.`product_dim`
    )
