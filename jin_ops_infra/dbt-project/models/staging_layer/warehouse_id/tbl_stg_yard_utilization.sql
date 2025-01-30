{{ config(
  materialized='table'
) }}



--This staging table is based on the Gsheet provided by the NM team, since we don't have any reliable technical solution for now

WITH CTE_1 AS(
    
SELECT DISTINCT 
External_Reference as ReservationID,
WarehouseName,
CASE  
   when WarehouseName like '%Hebron%' then 5
   when WarehouseName like '%Perris%' then 9
   when WarehouseName like '%Erlanger%' then 10
   when WarehouseName like '%13%' then 13
   when WarehouseName like '%23%' then 23
   when WarehouseName like '%McDonough%' then 17
   when WarehouseName like '%Mississauga%' then 21
   when WarehouseName like '%Lancaster%' then 24
   when WarehouseName like '%Florence%' then 22
   when WarehouseName like '%Port Wentworth%' then 25
   when WarehouseName like '%Lathrop%' then 26
   when WarehouseName like '%Jacksonville%' then 28
   when WarehouseName like '%CranburySmall%' then 31
   when WarehouseName like '%Aberdeen%' then 35
   when WarehouseName like '%Romeoville%' then 42
   when WarehouseName like '%Richmond%' then 45
   when WarehouseName like '%Greensboro%' then 15
   when WarehouseName like '%South Elgin%' then 30
end as warehouse_id,
Zone,
Trailer_Type,
Trailer_Number,
Carrier,
C3_Subwarehouse,
Reservation_State,
Reservation_Type,
Arrival_Time,
Collection_Date,
Working_Date,
Next_Node,
On_Site_Timestamp,
Last_Action_Reservation_User_Name,
SPO,
Ready_For_Departure_Timestamp,
Classification,
Appointment_Date,
 Out_of_Order,
Storage_Type,
Trailer_Comment,
 Units_Reserved,
Must_Unload_By_Date,
Early_Drop,
Early_Drop_Date,
upload_date,
BusinessLineID,
BusinessLineSubgroupID,
Final_Flow,
Trailer_Full_or_Empty
FROM 
    {{source('nm-prod_inbound_management','Onsite_Report_Classified_Historicals')}}
WHERE UPLOAD_DATE IS NOT NULL
and upload_date = current_date()-1
),

CTE_2 AS (
    SELECT
    FCID, -- warehouse_id
    Yard_Capacity,
    Week,
    CONCAT(FCID, "-", Week) AS identifier
FROM
    {{source('xyz-prod_curated','spot_capacity')}}
WHERE EXTRACT(WEEK FROM Week) = EXTRACT(WEEK FROM CURRENT_DATE())
AND EXTRACT(YEAR FROM Week) = EXTRACT(YEAR FROM CURRENT_DATE())
),

CTE_3 AS (
    
SELECT *,
CASE  
   WHEN WarehouseName LIKE '%Hebron%' THEN 5
   WHEN WarehouseName LIKE '%Perris%' THEN 9
   WHEN WarehouseName LIKE '%Erlanger%' THEN 10
   WHEN WarehouseName LIKE '%13%' THEN 13
   WHEN WarehouseName LIKE '%23%' THEN 23
   WHEN WarehouseName LIKE '%McDonough%' THEN 17
   WHEN WarehouseName LIKE '%Mississauga%' THEN 21
   WHEN WarehouseName LIKE '%Lancaster%' THEN 24
   WHEN WarehouseName LIKE '%Florence%' THEN 22
   WHEN WarehouseName LIKE '%Port Wentworth%' THEN 25
   WHEN WarehouseName LIKE '%Lathrop%' THEN 26
   WHEN WarehouseName LIKE '%Jacksonville%' THEN 28
   WHEN WarehouseName LIKE '%CranburySmall%' THEN 31
   WHEN WarehouseName LIKE '%Aberdeen%' THEN 35
   WHEN WarehouseName LIKE '%Romeoville%' THEN 42
   WHEN WarehouseName LIKE '%Richmond%' THEN 45
   WHEN WarehouseName LIKE '%Greensboro%' THEN 15
   WHEN WarehouseName LIKE '%South Elgin%' THEN 30
end as warehouse_id_2 
FROM CTE_1 a
LEFT JOIN CTE_2 b on CASE  
   WHEN WarehouseName LIKE '%Hebron%' THEN 5
   WHEN WarehouseName LIKE '%Perris%' THEN 9
   WHEN WarehouseName LIKE '%Erlanger%' THEN 10
   WHEN WarehouseName LIKE '%13%' THEN 13
   WHEN WarehouseName LIKE '%23%' THEN 23
   WHEN WarehouseName LIKE '%McDonough%' THEN 17
   WHEN WarehouseName LIKE '%Mississauga%' THEN 21
   WHEN WarehouseName LIKE '%Lancaster%' THEN 24
   WHEN WarehouseName LIKE '%Florence%' THEN 22
   WHEN WarehouseName LIKE '%Port Wentworth%' THEN 25
   WHEN WarehouseName LIKE '%Lathrop%' THEN 26
   WHEN WarehouseName LIKE '%Jacksonville%' THEN 28
   WHEN WarehouseName LIKE '%CranburySmall%' THEN 31
   WHEN WarehouseName LIKE '%Aberdeen%' THEN 35
   WHEN WarehouseName LIKE '%Romeoville%' THEN 42
   WHEN WarehouseName LIKE '%Richmond%' THEN 45
   WHEN WarehouseName LIKE '%Greensboro%' THEN 15
   WHEN WarehouseName LIKE '%South Elgin%' THEN 30
end = b.FCID
)

Select
upload_date,
warehouse_id_2 as warehouse_id,
Count(distinct reservationid) amount_of_units,
Yard_Capacity as yard_capacity
from cte_3
where warehouse_id is not null
group by 1,2,4




{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}