{{ config(
    materialized='incremental'
) }}

WITH combined_data AS (
    -- Combine the order_dt columns from all three staging tables
    SELECT Order_Date FROM {{ ref('int_sales_fr') }}
    UNION ALL
    SELECT Order_Date FROM {{ ref('int_sales_us') }}
    UNION ALL
    SELECT Order_Date FROM {{ ref('int_sales_in') }}
),

date_range AS (
    -- Get the min and max order dates from the combined data
    SELECT
        MIN(Order_Date) AS min_order_dt,
        MAX(Order_Date) AS max_order_dt
    FROM combined_data
),

generated_dates AS (
    -- Generate a series of dates between min_order_dt and max_order_dt using BigQuery's GENERATE_DATE_ARRAY
    SELECT
        Order_Date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        (SELECT min_order_dt FROM date_range), 
        (SELECT max_order_dt FROM date_range)
    )) AS Order_Date
),

date_dim AS (
    -- Create the date dimension table with all required columns
    SELECT
        DISTINCT
        Order_Date,
        EXTRACT(YEAR FROM Order_Date) AS order_year,
        EXTRACT(MONTH FROM Order_Date) AS order_month,
        EXTRACT(QUARTER FROM Order_Date) AS order_quarter,
        EXTRACT(DAY FROM Order_Date) AS order_day,  -- Corrected
        EXTRACT(DAYOFWEEK FROM Order_Date) AS order_dayofweek,
        FORMAT_TIMESTAMP('%A', Order_Date) AS order_dayname,
        EXTRACT(DAY FROM Order_Date) AS order_dayofmonth,  -- Corrected
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM Order_Date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS order_weekday,
        ROW_NUMBER() OVER (ORDER BY Order_Date) AS day_counter -- Calculate DayCounter
    FROM generated_dates
)

-- Perform an incremental insert to only add new dates
SELECT *
FROM date_dim

{% if is_incremental() %}

-- Insert only new records
WHERE Order_Date > (
    SELECT MAX(Order_Date) FROM {{ this }}
)

{% endif %}
