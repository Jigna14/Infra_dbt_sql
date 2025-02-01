{{ config(
         materialized = 'incremental',
         incremental_strategy = 'insert_overwrite',
         unique_key = ['report_date','warehouse_id'],
         partition_by = {
            "field": "report_date",
            "data_type": "date",
            "granularity": "day" },
         cluster_by = ['warehouse_id']
         )
         }}

WITH cte AS(

SELECT
    upload_date AS report_date,
    warehouse_id,
    amount_of_units,
    yard_capacity 
FROM
   {{ref("tbl_stg_yard_utilization")}}
WHERE
    upload_date >= '2024-01-01'
),

cte_3 AS(
  SELECT
          upload_date AS report_date,
          yard_capacity+350  AS yard_capacity --yard_capacity for PerrisLP (where offset value is 183)
FROM
   {{ref("tbl_stg_yard_utilization")}}
   WHERE warehouse_id = 9
)


SELECT
a.report_date,
a.warehouse_id,
a.amount_of_units,
CASE 
    WHEN a.warehouse_id IN (18, 9) THEN c.yard_capacity
    ELSE a.yard_capacity 
END AS yard_capacity
FROM cte a
LEFT JOIN cte_3 c
USING(report_date)
WHERE report_date = current_date() -1
AND a.warehouse_id IS NOT NULL






{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}