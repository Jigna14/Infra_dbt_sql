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


WITH units_actual_wtd AS (
    SELECT
      CAST(warehouse_id AS INT64) AS warehouse_id,
      SUM(inbound_receiving_units) AS inbound_receiving_units_wtd
    FROM {{ref ('tbl_stg_tran_log_units')}}
    WHERE
      DATE(log_datetime_local) 
    BETWEEN 
      DATE_ADD(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 Day) AS DATE), INTERVAL 1 - EXTRACT(DAYOFWEEK FROM cast(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS DATE)) DAY)
    AND 
      DATE_ADD(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL EXTRACT(DAYOFWEEK FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) DAY), INTERVAL 1 WEEK) 
    GROUP BY 1
),

units_actual_daily AS (
    SELECT
        CAST(warehouse_id AS INT64) AS warehouse_id,
        SUM(inbound_receiving_units) AS inbound_receiving_units_daily
    FROM {{ ref ('tbl_stg_tran_log_units')}}
    WHERE DATE(log_datetime_local) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY 1
),

ib_unit_capacity_rank AS (
    SELECT
      warehouse_id,
      day_of_unit_capacity AS day_of_inbound_unit_capacity,
      inbound_unit_capacity,
      upload_date AS inbound_unit_capacity_upload_date,
    FROM {{ ref('vw_stg_receiving_units')}} 
    QUALIFY RANK() OVER (PARTITION BY warehouse_id,day_of_unit_capacity ORDER BY upload_date DESC) = 1
),

pre_capacity_lag2 AS (
    SELECT
      warehouse_id,
      day_of_unit_capacity AS day_of_inbound_unit_capacity,
      inbound_unit_capacity,
      upload_date AS inbound_unit_capacity_upload_date,
    FROM {{ ref('vw_stg_receiving_units')}} 
    WHERE 1=1
    AND  DATE_TRUNC(upload_date,Week) =DATE_TRUNC(current_date(),Week)-14
    AND day_of_unit_capacity=current_date()-1
),

ib_unit_capacity_rank_lag2 AS (--782 for Aberdeen
    SELECT
      warehouse_id,
      day_of_inbound_unit_capacity,
      inbound_unit_capacity,
      inbound_unit_capacity_upload_date,
    FROM pre_capacity_lag2
    QUALIFY RANK() OVER (PARTITION BY warehouse_id,day_of_inbound_unit_capacity ORDER BY inbound_unit_capacity_upload_date DESC) = 1
),

ib_unit_capacity_daily_lag2 AS (
    SELECT
      warehouse_id,
      SUM(inbound_unit_capacity) AS sum_ib_unit_capacity_daily
    FROM ib_unit_capacity_rank_lag2
    WHERE day_of_inbound_unit_capacity = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY 1

),

ib_unit_capacity_wtd AS (
    SELECT
      CAST(warehouse_id AS INT64) AS warehouse_id,
      SUM(inbound_unit_capacity) AS sum_ib_unit_capacity_wtd
    FROM ib_unit_capacity_rank
    WHERE 
        DATE(day_of_inbound_unit_capacity)       
    BETWEEN 
        DATE_ADD(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 Day) AS DATE), INTERVAL 1 - EXTRACT(DAYOFWEEK FROM cast(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS DATE)) DAY)
    AND
        DATE_ADD(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL EXTRACT(DAYOFWEEK FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) DAY), INTERVAL 1 WEEK) 
    GROUP BY 1

),
ib_unit_capacity_daily AS (
    SELECT
      warehouse_id,
      SUM(inbound_unit_capacity) AS sum_ib_unit_capacity_daily
    FROM ib_unit_capacity_rank
    WHERE day_of_inbound_unit_capacity = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY 1

),

lpsbreak as (
SELECT warehouse_id,  cast(log_datetime_local as date) date, CASE WHEN freight_class_number = '1' then "Bulk_Picks_SP_Bins"
when freight_class_number > '1' then "Bulk_Picks_LP"
when freight_class_number is NULL then "Bulk_Picks_SP_Bins" end as Freight_Class
    , sum(bulk_pick_units)+sum(bins_pick_units) as BulkPick_Units
FROM {{ ref('tbl_stg_tran_logs_units_freight')}} tr
inner join {{source('xyz-prod_aad','t_freight_class')}} t   on t.freight_class_id = tr.freight_class_id
group by warehouse_id, date, freight_class_number
order by warehouse_id, date

),

lpsbreak2 as (
select warehouse_id, DATE, Bulk_Picks_LP, Bulk_Picks_SP_Bins
from lpsbreak as SourceTable
Pivot (sum(BulkPick_Units) for Freight_Class in ("Bulk_Picks_LP", "Bulk_Picks_SP_Bins")) as PivotTable
),

Daily AS( 
select warehouse_id
, sum(Bulk_Picks_LP)+sum(Bulk_Picks_SP_Bins) as  fc_total_units_shipped_actuals --On
from lpsbreak2
where cast(DATE as date) = cast(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as date)
group by warehouse_id
order by warehouse_id
    )
    ,

fc_unit_shipped_daily AS (
    SELECT 
    CAST(f.warehouse_id AS INT64) AS warehouse_id,
    CAST(f.fc_total_units_shipped_actuals AS FLOAT64)  AS fc_total_units_shipped_actuals_day
     FROM Daily f
    GROUP BY 1,2  
),

weekly AS( 
select warehouse_id
, sum(Bulk_Picks_LP)+sum(Bulk_Picks_SP_Bins) as  fc_total_units_shipped_actuals --On
from lpsbreak2
         WHERE
-- For weekly (from last Sunday to current day)
DATE(DATE) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK) and CURRENT_DATE()
group by warehouse_id
order by warehouse_id
    ),

fc_unit_shipped_weekly AS (
    SELECT 
    CAST(f.warehouse_id AS INT64) AS warehouse_id,
    CAST(f.fc_total_units_shipped_actuals AS FLOAT64)  AS fc_total_units_shipped_actuals_week
     FROM weekly f
    GROUP BY 1,2

  )  

SELECT
     DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS report_date,
     a.warehouse_id,
     a.inbound_receiving_units_wtd AS received_units_actual_week_to_date,
     b.inbound_receiving_units_daily AS received_units_actual_reporting_daily,
     c.sum_ib_unit_capacity_wtd AS inbound_unit_receiving_capacity_week_to_date,
     d.sum_ib_unit_capacity_daily AS inbound_unit_capacity_reporting_daily,
     (a.inbound_receiving_units_wtd - c.sum_ib_unit_capacity_wtd) AS throughput_variance_week_to_date_num,
     NULLIF(c.sum_ib_unit_capacity_wtd,0) AS throughput_variance_week_to_date_denominator,
      b.inbound_receiving_units_daily AS throughput_variance_daily_num,
     NULLIF(g.sum_ib_unit_capacity_daily,0) AS throughput_variance_daily_denominator,
     ((a.inbound_receiving_units_wtd - c.sum_ib_unit_capacity_wtd) / NULLIF(c.sum_ib_unit_capacity_wtd,0)) AS throughput_variance_week_to_date,
     ((b.inbound_receiving_units_daily - d.sum_ib_unit_capacity_daily) / NULLIF(d.sum_ib_unit_capacity_daily,0)) AS throughput_variance_daily,
    e.fc_total_units_shipped_actuals_day AS ob_fc_total_units_shipped_actuals_day,
    f.fc_total_units_shipped_actuals_week AS ob_fc_total_units_shipped_actuals_week
FROM units_actual_wtd a
LEFT JOIN units_actual_daily b 
    ON a.warehouse_id = b.warehouse_id
LEFT JOIN ib_unit_capacity_wtd c 
    ON a.warehouse_id = c.warehouse_id
LEFT JOIN ib_unit_capacity_daily d 
    ON a.warehouse_id = d.warehouse_id
LEFT JOIN fc_unit_shipped_daily e
    ON a.warehouse_id = e.warehouse_id
LEFT JOIN fc_unit_shipped_weekly f
    ON a.warehouse_id = f.warehouse_id
LEFT JOIN ib_unit_capacity_daily_lag2 g 
    ON a.warehouse_id = g.warehouse_id


{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}