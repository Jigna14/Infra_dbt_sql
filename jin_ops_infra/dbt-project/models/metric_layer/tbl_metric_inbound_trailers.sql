{{ config(
         materialized = 'incremental',
         incremental_strategy = 'insert_overwrite',
         unique_key = 'report_date',
         partition_by = {
            "field": "report_date",
            "data_type": "date",
            "granularity": "day" },
         cluster_by = ['warehouse_id']
         ) 
         }}

WITH 

all_trailers 
AS
(
SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS report_date,
    inbound_trailer_reservation_id AS inbound_reservation_id,
    warehouse_id,
    SUM(sum_trailer_units) AS sum_trailer_units, --metric 527 - Units on Yard EoD
FROM {{ ref ('tbl_stg_unit_metrics_prep_reservation_id_snapshot_3')}} 
WHERE (subwarehouse LIKE ('%Inbound%') OR subwarehouse LIKE ('%Receiv%') OR subwarehouse LIKE ('%VAS%') OR subwarehouse LIKE ('SubSatelliteLotRomeoville') OR subwarehouse LIKE ('%Rug%')) 
GROUP BY 1,2,3 
)
,

sla_calculation
AS
(
SELECT 
    ug.report_date,
    ug.inbound_reservation_id,
    ug.warehouse_id,
    DATETIME_DIFF(DATETIME_ADD(
    DATETIME_ADD(DATETIME(current_date('America/New_York'), TIME(00, 00, 00)), 
    INTERVAL 0 HOUR), 
    INTERVAL 4 HOUR
) ,yt.trailer_arrival_datetime_local,  HOUR) AS unload_diff_arrival,
    DATETIME_DIFF(DATETIME_ADD(
    DATETIME_ADD(DATETIME(current_date('America/New_York'), TIME(00, 00, 00)), 
    INTERVAL 0 HOUR), 
    INTERVAL 0 HOUR
),yt.trailer_scheduled_start_datetime_local,  HOUR)/24 AS unload_diff_appt,
    sum_trailer_units
    FROM all_trailers AS ug
    LEFT JOIN {{ ref('tbl_dim_yard_trailer')}}  yt
    ON ug.inbound_reservation_id=yt.inbound_trailer_reservation_id
    LEFT JOIN {{ ref('tbl_dim_castlegate_warehouse')}}  dw
    ON ug.warehouse_id=dw.warehouse_id
    WHERE 1=1
  --  AND  yt.is_trailer_inbound IS TRUE
  --  AND (yt.trailer_unload_datetime_local IS NULL OR CAST(yt.trailer_unload_datetime_local AS DATE)>CURRENT_DATE-1)
    GROUP BY 1,2,3,4,5,6
)
,

sla_numbers
AS
(
SELECT
    report_date,
    warehouse_id,
    inbound_reservation_id,
    sum_trailer_units, --EOD for not unloaded trailers
    CASE WHEN unload_diff_arrival > 72 THEN 1 ELSE 0 END AS out_of_arrival_72_hours,
    CASE WHEN unload_diff_appt > 1  THEN 1 ELSE 0 END AS out_of_appt_1_day
    FROM sla_calculation
)


SELECT
    a.warehouse_id,
    a.report_date,
    a.inbound_reservation_id,
    SUM(b.sum_trailer_units) AS total_amount_units,
    IFNULL(out_of_arrival_72_hours,0) AS out_of_arrival_72_hours,
    IFNULL(out_of_appt_1_day,0)AS out_of_appt_1_day,
    CASE WHEN IFNULL(out_of_arrival_72_hours,0)=1 OR IFNULL(out_of_appt_1_day,0)=1 THEN 1 ELSE 0 END AS out_of_both_sla
FROM sla_calculation a
LEFT JOIN sla_numbers b
    ON a.inbound_reservation_id=b.inbound_reservation_id
GROUP BY 1,2,3,5,6,7


{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}