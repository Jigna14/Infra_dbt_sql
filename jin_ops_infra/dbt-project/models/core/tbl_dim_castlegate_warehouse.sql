{{ config(unique_key = 'warehouse_id',
          materialized = 'incremental',
          merge_update_columns=['dwh_modified_timestamp',
                                'hash_record',
                               'record_source',
                               'warehouse_id',
                               'warehouse_string_id',
                               'region',
                               'campus_id',
                               'time_zone_hours_offset',
                               'warehouse_city',
                               'warehouse_state',
                               'warehouse_zip',
                               'warehouse_supplier_id',
                               'country_id',
                               'is_dedicated_break_bulk_facility',
                               'is_physical_retail_store_warehouse',
                               'is_active',
                               'is_fulfillment_center']
          ) }}
with cte_castlegate_warehouse as (
    select
        wh.wh_id,
        case 
            when wh.wh_id = '20' then 904 -- DEKassel
            when wh.wh_id = '33' then 1190 -- DELich 
            else wh.sva_id
        end as sva_id,
        wh.sty_id,
        wh.wms_wh_id,
        s.CampusID,
        case 
            when wh.wh_id = '05' then 'Hebron'
            when wh.wh_id = '09' then 'PerrisLP'
            when wh.wh_id = '10' then 'Erlanger'
            when wh.wh_id = '13' then 'Cranbury'
            when wh.wh_id = '15' then 'Greensboro'
            when wh.wh_id = '17' then 'McDonough'
            when wh.wh_id = '18' then 'PerrisSP'
            when wh.wh_id = '20' then 'Kassel'
            when wh.wh_id = '21' then 'Mississauga'
            when wh.wh_id = '22' then 'Florence'
            when wh.wh_id = '23' then 'CranburyLargeParcel'
            when wh.wh_id = '24' then 'Lancaster'
            when wh.wh_id = '25' then 'PortWentworth'
            when wh.wh_id = '26' then 'Lathrop'
            when wh.wh_id = '27' then 'Mallow'
            when wh.wh_id = '28' then 'Jacksonville'
            when wh.wh_id = '30' then 'South Elgin'
            when wh.wh_id = '31' then 'Cranbury 2'
            when wh.wh_id = '32' then 'Hammersbach'
            when wh.wh_id = '33' then 'Lich'
            when wh.wh_id = '35' then 'Aberdeen'
            when wh.wh_id = '42' then 'Romeoville'
            when wh.wh_id = '45' then 'Richmond'
            else wh.name
        end as wh,
        case 
            when wh.wh_id in ('05','10','15','21','22','30','42','45') then 'Central'
            when wh.wh_id in ('09','18','24','26') then 'West'
            when wh.wh_id in ('13','17','23','25','28','31','35') then 'East'
            when wh.wh_id in ('20','27','32','33') then 'EU'
            else null
        end as region,
        case 
            when wh.sty_id = 2 then datetime_diff(current_datetime("Europe/London"), current_datetime("America/New_York"), hour)
            when wh.sty_id = 3 then datetime_diff(current_datetime("Europe/Berlin"), current_datetime("America/New_York"), hour)
            else wh.hours_offset
        end as hours_offset, -- new hours_offset that adjust daylight time saving change
        wh.hours_offset as hours_offset_old,
        wh.city,
        wh.state,
        wh.zip,
        case
            when wh.wh_id = '05' then 1380
            when wh.wh_id = '09' then 1200
            when wh.wh_id = '10' then 1380
            when wh.wh_id = '13' then 1320
            when wh.wh_id = '17' then 1410
            when wh.wh_id = '18' then 1470
            when wh.wh_id = '21' then 1350
            when wh.wh_id = '22' then 1320
            when wh.wh_id = '23' then 990
            when wh.wh_id = '24' then 1380
            when wh.wh_id = '25' then 1020
            when wh.wh_id = '26' then 1200
            when wh.wh_id = '28' then 1050
            when wh.wh_id = '31' then 1320
            when wh.wh_id = '35' then 960
            else null  
        end as current_ground_time_in_minutes,
        su.WSSuID as warehouse_supplier_id,
        ifnull(dc.country_id, -1) as country_id,
        ifnull(su.WsIsBreakBulkFacility, false) as is_dedicated_break_bulk_facility,
        ifnull(st.IsActive, 0) = 1 as is_physical_retail_store_warehouse,
        su.WSIsActive as is_active, 
        su.WSIsFulfillmentcenter as is_fulfillment_center
    from {{ source('xyz-prod_curated','wh')}} as wh
    left join {{ source('xyz-prod_curated','lSi')}} as s
        on wh.wms_wh_id = s.WarehouseID
    left join {{ ref('tbl_dim_country') }} dc
        on wh.country_name = dc.country_long
    left join {{ source('xyz-prod_curated','l_wms_suppli')}} su
        on wh.wms_wh_id = su.wswhid
    left join {{ source('xyz-prod_curated','tbl_retail_store')}} st
        on wh.wms_wh_id = st.WarehouseID
    where wh.wms_wh_id not in (34,36,44,998,999) -- Excluding Test Warehouses
),
tmp_hash_record as (
    select
        w.wms_wh_id,
        wf-gcp-us-ae-gat-prod.ops_reporting_core.gat_hash_sha1(
                w.wh_id||'#'||
                cast(w.wms_wh_id as string)||'#'||
                ifnull(w.wh, 'N/A')||'#'||
                ifnull(w.region, 'N/A')||'#'||
                ifnull(cast(w.campusid as string), 'N/A')||'#'||
                ifnull(cast(w.hours_offset as string), 'N/A')||'#'||
                ifnull(w.city, 'N/A') ||'#'||
                ifnull(w.state, 'N/A') ||'#'||
                ifnull(w.zip, 'N/A')||'#'||
                ifnull(cast(w.warehouse_supplier_id as string), '-1')||'#'||
                cast(w.country_id as string)||'#'||
                cast(w.is_dedicated_break_bulk_facility as string)||'#'||
                cast(w.is_physical_retail_store_warehouse as string)||'#'||
                cast(w.is_active as string)||'#'||
                cast(w.is_fulfillment_center as string)
                ) as hash_record
    from cte_castlegate_warehouse w
)
  select 
        current_timestamp as dwh_creation_timestamp,
        current_timestamp as dwh_modified_timestamp,
        {{ source('xyz-prod_curated','wh')}} as record_source,
        h.hash_record,
        w.wms_wh_id as warehouse_id,
        w.wh_id as warehouse_string_id,
        ifnull(w.wh,'N/A') as warehouse_name,
        ifnull(w.region, 'N/A') as region,
        w.campusid as campus_id,
        w.hours_offset as time_zone_hours_offset, 
        ifnull(w.city, 'N/A') as warehouse_city,
        ifnull(w.state, 'N/A') as warehouse_state,
        ifnull(w.zip, 'N/A') as warehouse_zip,
        w.warehouse_supplier_id,
        w.country_id,
        w.is_dedicated_break_bulk_facility,
        w.is_physical_retail_store_warehouse,
        w.is_active,
        w.is_fulfillment_center
    from cte_castlegate_warehouse w
    left join tmp_hash_record h on w.wms_wh_id=h.wms_wh_id
    
{% if is_incremental() %}
    left join {{ this }} target on h.hash_record = target.hash_record
    where target.hash_record is null
{% endif %}
{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}