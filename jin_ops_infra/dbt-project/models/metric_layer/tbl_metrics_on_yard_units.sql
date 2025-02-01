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



WITH accountability AS (
    SELECT 
        ug.inbound_trailer_reservation_id AS inbound_reservation_id,
        CASE WHEN ug.warehouse_id IN (9,18) THEN cr.WarehouseID ELSE ug.warehouse_id END AS warehouse_id,
        CASE
            WHEN arrival_status = 'arrived_early' AND remaining_unload_time_hours > 0
                 THEN   CASE
                            WHEN    trailer_scheduled_start_week_date = EXTRACT(WEEK FROM DATETIME_ADD(CURRENT_DATETIME('America/New_York'), INTERVAL CAST(ug.time_zone_hours_offset AS INT64) HOUR)) AND
                                    trailer_scheduled_start_datetime_local > CAST(DATETIME_ADD(CURRENT_DATETIME('America/New_York'), INTERVAL CAST(ug.time_zone_hours_offset AS INT64) HOUR) AS DATE)
                            THEN 'appointed_later_this_week'
                            WHEN    trailer_scheduled_start_week_date > EXTRACT(WEEK FROM DATETIME_ADD(CURRENT_DATETIME('America/New_York'), INTERVAL CAST(ug.time_zone_hours_offset AS INT64) HOUR))
                            THEN 'appointed_future_weeks'
                        END
            WHEN arrival_status = 'arrived_late' AND unload_time_window_at_arrival_hours < 0
                 THEN   CASE
                            WHEN remaining_unload_time_hours BETWEEN -48 AND 0
                            THEN 'arrived_late_within_48hr_unload_window'    
                            ELSE 'arrived_late_not_within_48hr_unload_window'
                        END     
            WHEN arrival_status in ('arrived_early','arrived_on_time')
                 AND remaining_unload_time_hours <= 48
                 THEN  CASE
                            WHEN remaining_unload_time_hours BETWEEN -48 AND 0
                            THEN 'within_48hr_window'    
                            ELSE 'past_48hr_window'
                        END       
                 ELSE 'dude!'
        END AS accountability_bucket,
        SUM(ug.sum_trailer_units) AS sum_trailer_units
    FROM {{ref ('tbl_stg_unit_metrics_prep_reservation_id')}} AS ug
    LEFT JOIN {{ source('xyz-prod_curated','reservation')}} cr ON ug.inbound_trailer_reservation_id=cr.ReservationID --temp solution for mix up betwen Perris1 and Perris2 in CDL sources for DNPR specific
    WHERE (ug.subwarehouse LIKE ('%SPDomesticInbound%') OR ug.subwarehouse LIKE ('%SPCGFInbound%') OR ug.subwarehouse LIKE ('%Receiv%') OR ug.subwarehouse LIKE ('%VAS%') OR ug.subwarehouse LIKE ('%SPO%') ) 
    GROUP BY 1,2,3
),

dates
AS
(
    SELECT
        MAX(upload_date)  AS upload_date
    FROM {{ref ('vw_stg_receiving_units')}}
)

SELECT 
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS report_date,
    a.inbound_reservation_id,
    a.warehouse_id,
    a.accountability_bucket,
    a.sum_trailer_units,
    b.inbound_unit_capacity,
    ROUND(SAFE_DIVIDE(a.sum_trailer_units,b.inbound_unit_capacity),1) AS on_yard_units_metric_value
FROM accountability a
LEFT JOIN {{ ref('vw_stg_receiving_units')}} b 
    ON a.warehouse_id = b.warehouse_id 
    AND b.day_of_unit_capacity = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
INNER JOIN dates c
    ON b.upload_date=c.upload_date
WHERE a.sum_trailer_units IS NOT NULL



{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}