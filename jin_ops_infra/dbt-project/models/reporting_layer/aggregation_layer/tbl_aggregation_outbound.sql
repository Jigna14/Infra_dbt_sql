{{ config(materialized='incremental',
              unique_key = ['surrogate_key'],
              merge_update_columns=[
                    'dwh_modified_timestamp',
                    'metric_numerator',
                    'metric_denominator'])}}

WITH cte AS (

SELECT
    b.warehouse_name,
    c.country_long,
    '310'  AS metric_id, 
    "Small Parcel Fill Rate - Yesterday"  AS metric_name, 
    CAST(a.report_date AS DATE) AS time_base_date,
    fill_rate_mm AS metric_numerator,
    1 AS metric_denominator,
FROM {{ref('tbl_metric_daily_weekly_fill_rate')}} a
LEFT JOIN {{ref('tbl_dim_castlegate_warehouse')}} b 
    USING (warehouse_id)
LEFT JOIN {{ref('tbl_dim_country')}} c 
    USING (country_id)
WHERE 1=1
AND report_date=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND cadence = "daily" AND ship_class = "SP"
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
    b.warehouse_name,
    c.country_long,
    '310'  AS metric_id, 
    "Small Parcel Fill Rate - Last 3 Days Average [For graph]"  AS metric_name, 
    CAST(a.report_date AS DATE) AS time_base_date,
    fill_rate_mm AS metric_numerator,
    1 AS metric_denominator,
FROM {{ref('tbl_metric_daily_weekly_fill_rate')}} a
LEFT JOIN {{ref('tbl_dim_castlegate_warehouse')}} b 
    USING (warehouse_id)
LEFT JOIN {{ref('tbl_dim_country')}} c 
    USING (country_id)
WHERE 1=1
AND report_date=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND cadence = "last3days" AND ship_class = "SP"
GROUP BY 1,2,3,4,5,6

)


SELECT
    {{ dbt_utils.generate_surrogate_key(['metric_id', 'metric_name', 'time_base_date', 'warehouse_name', 'country_long'])}} AS surrogate_key,
    CURRENT_TIMESTAMP() AS dwh_creation_timestamp,
    CURRENT_TIMESTAMP() AS dwh_modified_timestamp,
    metric_id,
    metric_name,
    time_base_date,
    warehouse_name,
    country_long,
    metric_numerator,
    metric_denominator
FROM cte
