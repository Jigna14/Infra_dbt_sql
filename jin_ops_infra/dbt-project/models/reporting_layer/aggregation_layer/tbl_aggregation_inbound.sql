
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
    '174'  AS metric_id, 
    "Yard Utilization"  AS metric_name, 
    CAST(a.report_date AS DATE) AS time_base_date,
    amount_of_units AS metric_numerator,
    a.yard_capacity AS metric_denominator,
FROM {{ref('tbl_metric_yard_utilization')}} a
LEFT JOIN {{ref('tbl_dim_castlegate_warehouse')}} b 
    USING (warehouse_id)
LEFT JOIN {{ref('tbl_dim_country')}} c 
    USING (country_id)
WHERE 1=1
AND cast(report_date as date)=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

UNION ALL
    
SELECT
    b.warehouse_name,
    c.country_long,
    '527'  AS metric_id, 
    "EOD IB Units on Yard & Doors"  AS metric_name, 
    CAST(a.report_date AS DATE) AS time_base_date,
    SUM(a.total_amount_units) AS metric_numerator,
    1 AS metric_denominator,
FROM {{ref('tbl_metric_inbound_trailers')}} a
LEFT JOIN {{ref('tbl_dim_castlegate_warehouse')}} b 
    USING (warehouse_id)
LEFT JOIN {{ref('tbl_dim_country')}} c 
    USING (country_id)
WHERE 1=1
AND report_date=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1,2,3,4,5

UNION ALL
    
SELECT
    b.warehouse_name,
    c.country_long,
    '1474'  AS metric_id, 
    "Trailers out of Compliance vs 72 Hr Yard Arrival to Unload SLA"  AS metric_name, 
    CAST(a.report_date AS DATE) AS time_base_date,
    SUM(a.out_of_arrival_72_hours) AS metric_numerator,
    1 AS metric_denominator,
FROM {{ref('tbl_metric_inbound_trailers')}} a
LEFT JOIN {{ref('tbl_dim_castlegate_warehouse')}} b 
    USING (warehouse_id)
LEFT JOIN {{ref('tbl_dim_country')}} c 
    USING (country_id)
WHERE 1=1
AND report_date=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1,2,3,4,5
  
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
