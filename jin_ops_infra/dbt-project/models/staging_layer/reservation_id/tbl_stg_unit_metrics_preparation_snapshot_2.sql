{{ config (materialized ='table')}}

SELECT 
    yt.inbound_trailer_reservation_id,
    yt.warehouse_id,
    dw.time_zone_hours_offset,
    CASE
        WHEN CAST(ur.trailer_arrival_datetime_local AS DATE) < CAST(yt.trailer_scheduled_start_datetime_local AS DATE) THEN 'arrived_early'
        WHEN CAST(ur.trailer_arrival_datetime_local AS DATE) = CAST(yt.trailer_scheduled_start_datetime_local AS DATE) THEN 'arrived_on_time'
        WHEN CAST(ur.trailer_arrival_datetime_local AS DATE) > CAST(yt.trailer_scheduled_start_datetime_local AS DATE) THEN 'arrived_late'
        ELSE NULL
    END AS arrival_status,
    DATETIME_DIFF(yt.trailer_scheduled_start_datetime_local, yt.trailer_arrival_datetime_local, HOUR) AS unload_time_window_at_arrival_hours,
    DATETIME_DIFF(yt.trailer_scheduled_start_datetime_local, DATETIME_ADD(CURRENT_DATETIME('America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset AS INT64) HOUR), HOUR) AS remaining_unload_time_hours,
    EXTRACT(WEEK FROM CAST(yt.trailer_scheduled_start_datetime_local AS DATE)) AS trailer_scheduled_start_week_date,
    CAST(yt.trailer_scheduled_start_datetime_local AS DATETIME) AS trailer_scheduled_start_datetime_local,
    CAST(yt.trailer_arrival_datetime_local AS DATETIME) AS trailer_arrival_datetime_local,
    SUM(
        CASE 
            WHEN (ur.trailer_inbound_reservation_units IS NOT NULL OR mod(ur.trailer_inbound_reservation_units, 400) = 0) THEN trailer_inbound_reservation_units
            WHEN (ur.trailer_inbound_reservation_units IS NULL OR mod(ur.trailer_inbound_reservation_units, 400) = 0) THEN IFNULL(ur.xd_trailer_reservation_units,average_subwarehouse_qty)
            WHEN mod(ur.trailer_inbound_reservation_units, 400) = 0 THEN ur.xd_trailer_reservation_units
            WHEN ur.xd_trailer_reservation_units = 0 THEN ur.average_subwarehouse_qty
        END) AS sum_trailer_units,
    ur.subwarehouse
FROM {{ ref('tbl_dim_yard_trailer')}} yt
INNER JOIN {{ ref ('tbl_stg_units_reservation_id')}} ur
    USING (inbound_trailer_reservation_id) 
LEFT JOIN {{ ref('tbl_dim_castlegate_warehouse')}} dw
    USING (warehouse_id)
WHERE 1=1
AND yt.is_trailer_cancelled IS FALSE
AND (yt.trailer_arrival_datetime_local) BETWEEN  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 25 DAY) AND 
  DATETIME_ADD(
    DATETIME_ADD(DATETIME(current_date('America/New_York'), TIME(00, 00, 00)), 
    INTERVAL 0 HOUR), 
    INTERVAL 4 HOUR
) 
AND (yt.trailer_depart_datetime_local IS NULL OR yt.trailer_depart_datetime_local >DATETIME_ADD(
    DATETIME_ADD(DATETIME(current_date('America/New_York'), TIME(00, 00, 00)), 
    INTERVAL 0 HOUR), 
    INTERVAL 4 HOUR
) )

AND  yt.trailer_arrival_datetime_local IS NOT NULL

AND (yt.trailer_unload_datetime_local IS NULL OR yt.trailer_unload_datetime_local >DATETIME_ADD(
    DATETIME_ADD(DATETIME(current_date('America/New_York'), TIME(00, 00, 00)), 
    INTERVAL 0 HOUR), 
    INTERVAL 4 HOUR
) )
GROUP BY 1,2,3,4,5,6,7,8,9,11

{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}