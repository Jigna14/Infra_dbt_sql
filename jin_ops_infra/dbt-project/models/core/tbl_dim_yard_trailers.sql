{{config (materialized ='incremental'
            , unique_key='trailer_visit_id'
            , merge_update_columns=[
                'dwh_modified_timestamp',
                'inbound_trailer_reservation_id',
                'outbound_trailer_reservation_id',
                'subwarehouse',
                'carrier_id',
                'trailer_number',
                'site_id',
                'site_name',
                'load_id',
                'trailer_status',
                'trailer_type',
                'campus_id',
                'campus_name',
                'spot_id',
                'spot_type_id',
                'spot_type',
                'is_spot_offsite',
                'is_trailer_lost',
                'is_trailer_inbound',
                'warehouse_id',
                'warehouse_name',
                'country_id',
                'trailer_scheduled_start_datetime_local',
                'trailer_depart_datetime_local',
                'trailer_arrival_datetime_local',
                'trailer_unload_datetime_local',
                'trailer_load_datetime_local',
                'trailer_gated_out_datetime_local',
                'trailer_depart_datetime_utc',
                'trailer_gated_out_datetime_utc',
                'is_arrival_null_and_scheduledstart_before_refresh',
                'is_arrival_date_equal_scheduled_date',
                'is_arrival_datetime_before_scheduled_datetime',
                'is_arrival_date_before_scheduled_date',
                'is_arrival_date_after_scheduled_date',
                'is_trailer_unloaded',
                'is_trailer_cancelled'
              ]
            , cluster_by=[
                'trailer_visit_id'
            ]
)}}


WITH ym_reservation_event AS (
    SELECT  
        tr.ReservationID,
        a.ActionID,
        a.EventDate,
        vwd.BolNumber,
        tr.TrailerNumber,
        DATETIME_ADD(DATETIME(a.EventDate), INTERVAL CAST(o.time_zone_hours_offset AS INT64) HOUR) AS trailer_loading_complete_datetime,
        ROW_NUMBER() OVER(PARTITION BY tr.ReservationID, ActionID ORDER BY EventDate) AS rn -- Rank the event by reservation to get the first timestamp if a reservation has multiple events for the same action id
    FROM {{ source('xyz-prod_curated','tbl_reservation')}} tr
    INNER JOIN {{ source('xyz-prod_curated','Reservation_event')}} a 
    ON tr.reservationID  = a.ReservationID
    INNER JOIN {{ ref('tbl_dim_castlegate_warehouse')}} AS o 
    ON tr.warehouseid = o.warehouse_id --joining on dim_warehouse to get accurate hours offset 
    LEFT JOIN {{ source('xyz-prod_curated','referncecontent')}} cr 
    ON  tr.reservationid = cr.ReservationID
    LEFT JOIN {{ source('xyz-prod_curated','contentdetail')}} vwd 
    ON cr.ContentRefID = vwd.ContentRefID  
    WHERE 1=1
        {% if is_incremental() %}
    AND
-- this filter will only be applied on an incremental run
    (CAST(a.CreatedDate AS DATE) >= {{regular_6_month_backfill()}})
{% endif %}
), load_res_happy AS (
   SELECT
       l.load_id,
       r1. reservationID,
       ROW_NUMBER() OVER(PARTITION BY l.load_id ORDER BY l.load_id) AS no_duplicates
   FROM {{ source('xyz-prod_curated','load_master')}} l
   LEFT JOIN ym_reservation_event r1
   ON l.reference_number = r1.BolNumber AND r1.rn = 1
), load_res_sad AS (
   SELECT
       l.load_id,
       r2.reservationID,
       ROW_NUMBER() OVER(PARTITION BY l.load_id ORDER BY l.load_id) AS no_duplicates
    FROM {{ source('xyz-prod_curated','load_master')}} AS l
    LEFT JOIN ym_reservation_event AS r2  
    ON REGEXP_REPLACE(l.trailer_number, r"^[a-zA-Z_-]+([0-9]+$)", "\\1") = REGEXP_REPLACE(r2.TrailerNumber, r"^[a-zA-Z_-]+([0-9]+$)", "\\1") 
        AND CAST(l.actual_ship_date AS DATE) BETWEEN CAST(DATETIME_SUB(r2.trailer_loading_complete_datetime, INTERVAL 1 DAY) AS DATE) 
        AND CAST(DATETIME_ADD(r2.trailer_loading_complete_datetime, INTERVAL 1 DAY) AS DATE)
        AND r2.rn = 1 
    --Backup source for load_id and reservation_id mapping if happy path (load_res_happy) doesn't work
), load_res_fin AS (
   SELECT
        load_id
        , reservationID
    FROM load_res_happy
    WHERE no_duplicates = 1
    AND reservationID IS NOT NULL
    UNION ALL
    SELECT
        rs.load_id
        , rs.reservationID
    FROM load_res_sad rs
    LEFT JOIN load_res_happy rh USING (load_id)
    LEFT JOIN load_res_happy rh2 ON rs.reservationID = rh2.reservationID
    WHERE rs.no_duplicates = 1
    AND rh.reservationID IS NULL --removing resID that we found in the happy path
    AND rh2.load_id IS NULL
    AND rs.reservationID IS NOT NULL
), epv AS(
   SELECT
       lr.load_id,
       lr.reservationID,
       shipperGateOutTimestamp
   FROM load_res_fin lr
   LEFT JOIN wf-gcp-us-wcp-inbound-prod.trailer_visibility_public.tblJourney epv
   ON lr.load_id = epv.loadId --for every loadid we need a reservation id to map epv.shipperGateOutTimestamp
   AND shipperGateOutTimestamp is not null
   QUALIFY ROW_NUMBER() OVER(PARTITION BY reservationID ORDER BY lr.reservationID) = 1 -- No duplicate records allowed for 1:1 trailer_number:reservation_number
), cte AS (
    SELECT
        cdl.trailer_visit_id,
        CURRENT_TIMESTAMP() AS dwh_creation_timestamp,
        CURRENT_TIMESTAMP() AS dwh_modified_timestamp,
        CASE 
            WHEN tr.IsInbound IS TRUE THEN unnested_res_id
            WHEN tr_ul.IsInbound IS TRUE THEN unnest_unload_res_id
            ELSE NULL
        END AS inbound_trailer_reservation_id,
        CASE 
            WHEN tr.IsInbound IS FALSE THEN unnested_res_id
            ELSE NULL
        END AS outbound_trailer_reservation_id,
        IFNULL(tr_ul.subwarehouse,tr.subwarehouse) AS subwarehouse,
        IFNULL(tr_ul.CarrierID,tr.CarrierID) AS carrier_id,
        IFNULL(tr_ul.TrailerNumber,tr.TrailerNumber) AS trailer_number,
        CASE WHEN unnest_unload_res_id IS NULL THEN tr.siteID ELSE tr_ul.siteID END AS site_id,
        CASE WHEN unnest_unload_res_id IS NULL THEN dw3.warehouse_name ELSE dw2.warehouse_name END AS site_name,
        epv.load_id as load_id,
        unnested_trailer_id AS trailer_id,
        unnested_trailer_status AS trailer_status,
        unnested_trailer_type AS trailer_type,
        unnested_campus_id AS campus_id,
        unnested_campus AS campus_name,
        unnested_spot_id AS spot_id,
        unnested_spot_type_id AS spot_type_id,
        unnested_spot_type AS spot_type,
        unnested_offsite_spot AS is_spot_offsite,
        unnested_trailer_lost AS is_trailer_lost,
        IFNULL(tr_ul.IsInbound,tr.IsInbound) AS is_trailer_inbound,
        IFNULL(tr_ul.IsTerminated,tr.IsTerminated) AS is_trailer_cancelled,
        CASE WHEN unnest_unload_res_id IS NULL THEN dw3.warehouse_id ELSE dw2.warehouse_id END AS warehouse_id,
        CASE WHEN unnest_unload_res_id IS NULL THEN dw3.warehouse_name ELSE dw2.warehouse_name END AS warehouse_name,
        CASE WHEN unnest_unload_res_id IS NULL THEN dw3.country_id ELSE dw2.country_id END AS country_id,
        DATETIME_ADD(DATETIME((CASE WHEN unnest_unload_res_id IS NULL THEN tr.ScheduledStartDate ELSE tr_ul.ScheduledStartDate END),'America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS  trailer_scheduled_start_datetime_local,
        DATETIME_ADD(DATETIME(trailer_depart_unnest,'America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS trailer_depart_datetime_local,
        DATETIME_ADD((CASE WHEN trailer_arrival_unnest IS NULL THEN yr_arrival.EventDate ELSE DATETIME(trailer_arrival_unnest,'America/New_York') END), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS trailer_arrival_datetime_local,
        DATETIME_ADD(DATETIME(trailer_unload_unnest,'America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS trailer_unload_datetime_local,   
        DATETIME_ADD(DATETIME(trailer_load_unnest,'America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS trailer_load_datetime_local,
        DATETIME_ADD(DATETIME(TIMESTAMP(epv.shipperGateOutTimestamp) ,'America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS trailer_gated_out_datetime_local,--EPV Timestamp
        DATETIME_ADD(CURRENT_DATETIME('America/New_York'), INTERVAL CAST(dw.time_zone_hours_offset  AS INT64) HOUR) AS refresh_datetime_local,
        DATETIME(trailer_depart_unnest) as trailer_depart_datetime_utc,
        DATETIME(TIMESTAMP(epv.shipperGateOutTimestamp)) as trailer_gated_out_datetime_utc,
        cdl.event_created_timestamp as cdl_event_created_timestamp
    FROM {{ source ('xyz-prod_curated','tbl_yard_trailer_visit')}} cdl
    LEFT JOIN UNNEST (cdl.site_id) AS wh_site_id
    LEFT JOIN UNNEST (cdl.site) AS wh_site_name
    LEFT JOIN UNNEST (cdl.trailer_id) AS unnested_trailer_id
    LEFT JOIN UNNEST (cdl.trailer_number) AS unnested_trailer_number
    LEFT JOIN UNNEST (cdl.trailer_status) AS unnested_trailer_status
    LEFT JOIN UNNEST (cdl.trailer_type) AS unnested_trailer_type
    LEFT JOIN UNNEST (cdl.campus_id) AS unnested_campus_id
    LEFT JOIN UNNEST (cdl.campus) AS unnested_campus
    LEFT JOIN UNNEST (cdl.spot_id) AS unnested_spot_id
    LEFT JOIN UNNEST (cdl.spot_type_id) AS unnested_spot_type_id
    LEFT JOIN UNNEST (cdl.spot_type) AS unnested_spot_type
    LEFT JOIN UNNEST (cdl.is_spot_offsite) AS unnested_offsite_spot
    LEFT JOIN UNNEST (cdl.is_trailer_lost) AS unnested_trailer_lost
    LEFT JOIN UNNEST (cdl.trailer_reservation_id) AS unnested_res_id
    LEFT JOIN UNNEST (cdl.trailer_unload_reservation_id) AS unnest_unload_res_id
    LEFT JOIN UNNEST (cdl.facility_id) as warehouse_id
    LEFT JOIN UNNEST (cdl.trailer_arrival_date) AS trailer_arrival_unnest
    LEFT JOIN UNNEST (cdl.trailer_unload_complete_date) AS trailer_unload_unnest
    LEFT JOIN UNNEST (cdl.trailer_load_complete_date) AS trailer_load_unnest
    LEFT JOIN UNNEST (cdl.trailer_depart_date) AS trailer_depart_unnest
    LEFT JOIN UNNEST (cdl.trailer_reservation_cancelled_date) AS trailer_cancelled_date
    LEFT JOIN {{ source ('xyz-prod_curated','reservation')}} tr 
        ON unnested_res_id = tr.reservationID
    LEFT JOIN {{ source ('xyz-prod_curated','reservation')}} tr_ul
        ON unnest_unload_res_id = tr_ul.reservationID
    LEFT JOIN ym_reservation_event yr_arrival
        ON tr_ul.reservationID = yr_arrival.ReservationID
        AND yr_arrival.ActionID = 28 -- trailer arrival event code
        AND yr_arrival.rn = 1 -- Get the first trailer arrival event
    INNER JOIN {{ ref('tbl_dim_castlegate_warehouse')}} dw 
        ON warehouse_id = dw.warehouse_id
    LEFT JOIN {{ ref('tbl_dim_castlegate_warehouse')}} dw2
        ON tr_ul.WarehouseID = dw2.warehouse_id
    LEFT JOIN {{ ref('tbl_dim_castlegate_warehouse')}} dw3
        ON tr.WarehouseID = dw3.warehouse_id
     LEFT JOIN epv epv
        ON tr.reservationID = epv.reservationID
    WHERE 1=1
    {% if is_incremental() %}
    AND
-- this filter will only be applied on an incremental run
    (CAST(cdl.event_created_timestamp AS DATE) >= {{regular_6_month_backfill()}})
{% endif %}
)
SELECT
    trailer_visit_id,
    dwh_creation_timestamp,
    dwh_modified_timestamp,
    inbound_trailer_reservation_id,
    outbound_trailer_reservation_id,
    subwarehouse,
    carrier_id,
    trailer_number,
    site_id,
    site_name,
    load_id,
    trailer_id,
    trailer_status,
    trailer_type,
    campus_id,
    campus_name,
    spot_id,
    spot_type_id,
    spot_type,
    is_spot_offsite,
    is_trailer_lost,
    is_trailer_inbound,
    warehouse_id,
    warehouse_name,
    country_id,
    trailer_scheduled_start_datetime_local,
    trailer_depart_datetime_local,
    trailer_arrival_datetime_local,
    trailer_unload_datetime_local,
    trailer_load_datetime_local,
    trailer_gated_out_datetime_local,
    trailer_depart_datetime_utc,
    trailer_gated_out_datetime_utc,
    refresh_datetime_local,
    cdl_event_created_timestamp,
    CASE 
      WHEN trailer_arrival_datetime_local IS NULL AND trailer_scheduled_start_datetime_local <= CURRENT_DATETIME() THEN TRUE
      ELSE FALSE 
    END AS is_arrival_null_and_scheduledstart_before_refresh,
    CASE 
      WHEN CAST(trailer_scheduled_start_datetime_local AS DATE) = CAST(trailer_arrival_datetime_local AS DATE) THEN TRUE
      ELSE FALSE 
    END AS is_arrival_date_equal_scheduled_date,
    CASE 
      WHEN trailer_scheduled_start_datetime_local > trailer_arrival_datetime_local THEN TRUE
      ELSE FALSE 
    END AS is_arrival_datetime_before_scheduled_datetime,
    CASE 
      WHEN CAST(trailer_scheduled_start_datetime_local AS DATE) > CAST(trailer_arrival_datetime_local AS DATE) THEN TRUE
      ELSE FALSE 
    END AS is_arrival_date_before_scheduled_date,
    CASE 
      WHEN CAST(trailer_scheduled_start_datetime_local AS DATE) < CAST(trailer_arrival_datetime_local AS DATE) THEN TRUE
      ELSE FALSE 
    END AS is_arrival_date_after_scheduled_date,   
    CASE 
        WHEN trailer_unload_datetime_local IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_trailer_unloaded,
    is_trailer_cancelled
FROM cte



{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}