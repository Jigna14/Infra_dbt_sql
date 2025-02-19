{{ config(materialized='table') }}

WITH lri AS (
    SELECT
        inbound_trailer_reservation_id,
        warehouse_id,
        subwarehouse,
        trailer_arrival_datetime_local,
        is_trailer_unloaded,
        ROW_NUMBER() OVER (PARTITION BY inbound_trailer_reservation_id ORDER BY trailer_visit_id DESC) AS latest_res_id
    FROM {{ ref ('tbl_dim_yard_trailer')}} yt
    WHERE is_trailer_inbound IS TRUE
    AND inbound_trailer_reservation_id IS NOT NULL
),
happy AS (
    SELECT
        lri.inbound_trailer_reservation_id,
        lri.warehouse_id,
        hvwd.Qty,
        hvwd.IsContentRefDetailDeleted,
        hvwd.ContentType,
        (SPLIT(hvwd.spoid, '_'))[SAFE_ORDINAL(1)] AS CleanSpoID,
        'happy_path' AS qty_path
    FROM lri 
    INNER JOIN {{ source('ops-dev_yard_staging','tblReservationontent')}} cr 
        ON lri.inbound_trailer_reservation_id = cr.reservationid
    LEFT JOIN {{ source('ops-dev_yard_staging','tblcontentDetail')}} hvwd 
        ON cr.ContentRefID = hvwd.ContentRefID 
    WHERE lri.latest_res_id = 1

),
sad AS (
    SELECT
        lri.inbound_trailer_reservation_id,
        lri.warehouse_id,
        svwd.Qty,
        svwd.IsContentRefDetailDeleted,
        svwd.ContentType,
        (SPLIT(svwd.spoid, '_'))[SAFE_ORDINAL(1)] AS CleanSpoID,
        'sad_path' AS qty_path
    FROM lri 
    INNER JOIN {{ source('ops-dev_yard_staging','tblReservationContent')}} cr 
        ON lri.inbound_trailer_reservation_id = cr.reservationid
    LEFT JOIN {{ source('ops-dev_yard_staging','tblcontentDetail')}} svwd 
        ON lri.inbound_trailer_reservation_id = svwd.ReservationID
    LEFT JOIN happy h
        ON h.CleanSpoID=(SPLIT(svwd.spoid, '_'))[SAFE_ORDINAL(1)] 
    WHERE lri.latest_res_id = 1
    AND h.inbound_trailer_reservation_id IS NULL
),
base AS (
    SELECT 
        inbound_trailer_reservation_id,
        warehouse_id,
        Qty,
        IsContentRefDetailDeleted,
        ContentType,
        CleanSpoID,
        qty_path,
    FROM happy
    UNION ALL
    SELECT 
        inbound_trailer_reservation_id,
        warehouse_id,
        Qty,
        IsContentRefDetailDeleted,
        ContentType,
        CleanSpoID,
        qty_path,
    FROM sad
),
happy_units AS (
    SELECT 
        b.inbound_trailer_reservation_id,
        SUM(b.Qty) AS trailer_reservation_units
    FROM base b
    WHERE b.IsContentRefDetailDeleted IS FALSE 
    AND b.ContentType = 1
    GROUP BY 1
),
sad_units AS (
    SELECT 
        b.inbound_trailer_reservation_id,
        SUM(b.Qty) AS trailer_reservation_units
    FROM base b
    WHERE b.IsContentRefDetailDeleted IS FALSE 
    GROUP BY 1
),
--get units from DBF using script yard prod managment team shared: https://infohub.corp.wayfair.com/display/YMS/Helpful+BigQuery+Queries
dbf_raw AS (
    SELECT 
        re.ReservationID AS inbound_trailer_reservation_id,  
        JSON_QUERY(EventDetail, '$.reservationDetails.totalQuantity') AS total_quantity,
        ROW_NUMBER() OVER (PARTITION BY re.ReservationID ORDER BY re.EventDate desc) AS row_num 
    FROM {{ source('ops-dev_yard_staging','vwEventQueue')}} eq
    INNER JOIN {{ source('ops-dev_yard_staging','vwreservationEvent')}} re 
        ON re.EventQueueID = eq.ID
),
dbf_units AS (
    SELECT  
        lri.inbound_trailer_reservation_id,
        cast(dbf.total_quantity AS INT64) AS trailer_reservation_units
    FROM  lri 
    LEFT JOIN dbf_raw dbf 
        ON lri.inbound_trailer_reservation_id = dbf.inbound_trailer_reservation_id 
        AND dbf.row_num = 1
    LEFT JOIN happy_units hp 
        ON lri.inbound_trailer_reservation_id = hp.inbound_trailer_reservation_id -- to exclude reservationIDs that we already get unit counts using the happy path
    LEFT JOIN sad_units sp 
        ON lri.inbound_trailer_reservation_id = sp.inbound_trailer_reservation_id --to exclude reservationIDs that we already get unit counts using the sad path
    WHERE hp.inbound_trailer_reservation_id IS NULL 
    AND lri.latest_res_id = 1
    AND sp.inbound_trailer_reservation_id IS NULL
),
union_paths AS (
    SELECT  
        inbound_trailer_reservation_id,
        trailer_reservation_units,
        'happy_path_yard_management' AS path,
        CASE 
            WHEN trailer_reservation_units = 400 THEN 2
            ELSE 1 
        END AS priority
    FROM happy_units
    UNION ALL
    SELECT  
        inbound_trailer_reservation_id,
        trailer_reservation_units,
        'sad_path_yard_management' AS path,
        CASE 
            WHEN trailer_reservation_units = 400 THEN 2
            ELSE 1 
        END AS priority,
    FROM sad_units
    UNION ALL
    SELECT  
        inbound_trailer_reservation_id,
        trailer_reservation_units,
        'dbf_path_yard_management' AS path,
        CASE 
            WHEN trailer_reservation_units = 400 THEN 2
            ELSE 1 
        END AS priority    
    FROM dbf_units
),
c3_units AS (
    SELECT
        inbound_trailer_reservation_id,
        path,
        trailer_reservation_units,
        ROW_NUMBER() OVER (PARTITION BY inbound_trailer_reservation_id ORDER BY priority) AS row_num
    FROM union_paths
),
--mapping reservationID to TruckID in order to pull the Loaded Carton Count from vw_stg_truckload_volume as well as pull spiqty for qty received and planned
res_truck AS (
    SELECT 
        b.inbound_trailer_reservation_id,
        b.warehouse_id,
        b.ContentType,
        slm_hp.SlmTruckID AS slm_truck_id,
        slm_hp.slmID AS slm_id
    FROM base b
    LEFT JOIN {{ source ('sql-data-dev_etl_order','tbl_shipping_load_manifest')}} slm_hp 
        ON SAFE_CAST(b.CleanSpoID AS INT64) = slm_hp.SlmTruckID 
        

),     
loaded_carton AS (
    SELECT  
        ROW_NUMBER() OVER (PARTITION BY rt.inbound_trailer_reservation_id ORDER BY rt.inbound_trailer_reservation_id) AS row_num,
        rt.inbound_trailer_reservation_id,
        CASE 
            WHEN tv.loaded_carton_count = 0 THEN tv.total_cartons 
            ELSE tv.loaded_carton_count 
        END AS loaded_carton_count
    FROM res_truck AS rt
    LEFT JOIN {{ ref ('vw_stg_truckload_volume')}} AS tv 
        ON rt.slm_truck_id = tv.truck_id
    WHERE rt.slm_truck_id IS NOT NULL
        AND rt.ContentType IN (2,3)

),
rec_units AS (
    SELECT 
        b.inbound_trailer_reservation_id,
        COUNT(tu.shipment_destination_unloaded_date IS NOT NULL) AS trailer_received_qty,
        COUNT(tu.transport_unit_id) AS trailer_outstanding_qty,
        ROW_NUMBER() OVER (PARTITION BY b.inbound_trailer_reservation_id ORDER BY b.inbound_trailer_reservation_id) row_num
    FROM base b 
    LEFT JOIN {{ source ('layer_curated','tbl_shipment_manifest_latest')}} sml 
        ON SAFE_CAST(b.CleanSpoID AS INT64) = sml.truck_id
    INNER JOIN {{ source ('layer_curated','tbl_transport_unit_shipment_latest')}} tu 
        ON sml.bill_of_lading_number = tu.bill_of_lading_number
        AND tu.shipment_destination_entity_type = 'Warehouse'
    GROUP BY 1
),
trailer_qc AS (
    SELECT  
        ur.inbound_trailer_reservation_id,
        ROW_NUMBER() OVER (PARTITION BY ur.inbound_trailer_reservation_id ORDER BY ur.inbound_trailer_reservation_id) AS row_num,
        SUM(ROUND(SpiQty * 0.25,0)) AS trailer_qc_units,
    FROM lri ur
    LEFT JOIN {{ source('ops-dev_yard_staging','tblReservationContentReference')}} cr 
        ON ur.inbound_trailer_reservation_id = cr.reservationid
    LEFT JOIN {{ source('ops-dev_yard_staging','vwContentDetail')}} vwd 
        ON cr.ContentRefID = vwd.ContentRefID AND vwd.ReservationID = cr.ReservationID 
    LEFT JOIN {{ source ( 'sql-data-dev_etl_order','tbl_shipping_load_item')}} sli 
        ON sli.SliSlmID = vwd.SpiID
    INNER JOIN {{ source ('sql-data-dev_csn_wms','tbl_stock_purchase_order_item')}} spi 
        ON sli.SliSpiID = spi.SpiID
    INNER JOIN {{ source ('sql-data-dev_csn_wms','tbl_item_activity')}} ia ON spi.SpiSprID = ia.SprID AND ia.ActivityCode IN (63,6)
    WHERE vwd.ContentType IN (2,3)
    GROUP BY 1
),
--backup xd_reservation_units
xd_units_raw AS (
    SELECT  
        ur.inbound_trailer_reservation_id,
        ROW_NUMBER() OVER (PARTITION BY ur.inbound_trailer_reservation_id ORDER BY ur.inbound_trailer_reservation_id) AS row_num,
        SUM(spi.SpiReceivedQty) AS xd_received_units,
        SUM(spi.SpiQty) AS xd_trailer_reservation_units
    FROM base ur
    LEFT JOIN {{ source ( 'sql-data-dev_etl_order','tbl_shipping_load_manifest') }} slm 
        ON safe_cast(ur.CleanSpoID as int64) = slm.slmtruckid 
        AND ur.ContentType IN (2,3)
    LEFT JOIN {{ source ( 'sql-data-dev_etl_order','tbl_shipping_load_item')}} sli 
        ON sli.SliSlmID = slm.slmid
    LEFT JOIN {{ source ('sql-data-dev_csn_wms','tbl_stock_purchase_order_item')}} spi 
        ON sli.SliSpiID = spi.SpiID
    INNER JOIN {{ source ('sql-data-dev_csn_wms','tbl_stock_purchase_order')}} spo 
        ON spi.SpiSpoID = spo.SpoId
        AND spo.SpoId <> spo.SpoParentSpoId --used to distinguish IXD units
        AND spo.SpoType = 3
    WHERE 1=1
    AND left(ur.CleanSpoID,1) <> 'M'
    AND ur.IsContentRefDetailDeleted IS FALSE
    AND spi.spiqty <> 0
    GROUP BY 1
),
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--CONSOLIDATING UNITS AND ADDING COALESCE LOGIC -- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
units_logic AS (
    SELECT 
        lri.inbound_trailer_reservation_id,
        lri.trailer_arrival_datetime_local, 
        lri.subwarehouse,
        COALESCE(rec.trailer_received_qty,xd.xd_received_units) AS trailer_received_units,
        CASE
            WHEN lri.subwarehouse LIKE '%Recei%' 
                OR lri.subwarehouse LIKE '%SPDomesticInbound%'
                OR lri.subwarehouse LIKE '%SPCGFInbound%'
                OR lri.subwarehouse LIKE '%PPXD%' 
            THEN COALESCE(xd.xd_trailer_reservation_units,lc.loaded_carton_count) 
        END AS xd_trailer_reservation_units,
        rec.trailer_outstanding_qty AS total_trailer_units,
        qc.trailer_qc_units,
        lc.loaded_carton_count,
        IFNULL(ru.trailer_reservation_units,rec.trailer_outstanding_qty) AS trailer_inbound_reservation_units,
        ru.path,
        lri.is_trailer_unloaded,
    FROM lri 
    LEFT JOIN loaded_carton lc 
        ON lri.inbound_trailer_reservation_id = lc.inbound_trailer_reservation_id 
        AND lc.row_num = 1 
    LEFT JOIN rec_units rec
        ON lri.inbound_trailer_reservation_id = rec.inbound_trailer_reservation_id
        AND rec.row_num = 1
    LEFT JOIN trailer_qc qc
        ON lri.inbound_trailer_reservation_id = qc.inbound_trailer_reservation_id
        AND qc.row_num = 1
    LEFT JOIN c3_units ru 
        ON lri.inbound_trailer_reservation_id = ru.inbound_trailer_reservation_id 
        AND ru.row_num = 1
    LEFT JOIN xd_units_raw xd 
        ON lri.inbound_trailer_reservation_id = xd.inbound_trailer_reservation_id 
        AND xd.row_num = 1
    WHERE lri.latest_res_id = 1
),
median_backup AS (
    SELECT
        subwarehouse,
        PERCENTILE_DISC(IFNULL(trailer_inbound_reservation_units,0) + IFNULL(xd_trailer_reservation_units,0) ,0.5) OVER (PARTITION BY subwarehouse) as inbound_qty_median
    FROM units_logic
    WHERE (subwarehouse LIKE ('%SPDomesticInbound%') OR subwarehouse LIKE ('%SPCGFInbound%') OR subwarehouse LIKE ('%Receiv%'))
    AND CAST(trailer_arrival_datetime_local AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)   
    AND trailer_arrival_datetime_local < CURRENT_DATETIME()
    AND IFNULL(trailer_inbound_reservation_units,0) + IFNULL(xd_trailer_reservation_units,0) + IFNULL(total_trailer_units,0) > 0
),
sub AS (
    SELECT 
        subwarehouse,
        AVG(inbound_qty_median) AS average_subwarehouse_qty
    FROM median_backup
    GROUP BY 1
)
    SELECT 
        ul.inbound_trailer_reservation_id,
        ul.trailer_arrival_datetime_local,
        ul.subwarehouse,
        ul.trailer_qc_units,
        ul.trailer_inbound_reservation_units,
        ul.trailer_received_units,
        ul.xd_trailer_reservation_units,
        s.average_subwarehouse_qty,
        ul.loaded_carton_count,
        ul.total_trailer_units,
        ul.is_trailer_unloaded,
        ul.path
    FROM units_logic ul
    LEFT JOIN sub s 
        ON ul.subwarehouse = s.subwarehouse
    
{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}
