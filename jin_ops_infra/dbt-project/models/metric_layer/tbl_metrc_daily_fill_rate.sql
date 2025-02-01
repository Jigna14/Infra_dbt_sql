{{ config(
         materialized = 'incremental',
         incremental_strategy = 'insert_overwrite',
         unique_key = ['report_date','warehouse_id','cadence','ship_class'],
         partition_by = {
            "field": "report_date",
            "data_type": "date",
            "granularity": "day" },
         cluster_by = ['warehouse_id']
         )
         }}

-- cte to include filter for middle mile and large parcel
WITH fill_rate_cte AS(

SELECT
  a.whid AS warehouse_id,
  a.shipclass AS ship_class, --same warehouse can have both LP and SP
  c.FacilityType AS is_middle_mile, --null is being left as is for now
  a.cptdatetime,
  a.ShippedOnTime
FROM {{ source ('xyz-prod_curated', 'tbl_dim_fc_weekly_review_fill_rate') }} a
LEFT JOIN {{ ref ('tbl_dim_castlegate_warehouse') }} b
  -- table a has less warehouses than table b. Left join is used now, there might be nulls of FacilityType in some warehouses
  ON a.whid=b.warehouse_string_id
LEFT JOIN{{ source ('xyz-prod_curated','tbl_dim_fc_fill_rate') }} c
  -- some warehouses are not in both tables, for example Port Wentworth is only in table c but not table b. Left join will exclude the warehouses only in table c
  ON b.warehouse_string_id=c.wh_id
where a.performanceexclusions = 0
AND a.OrderType_Performance = 'Standard'


)


SELECT
  CAST(d.warehouse_id AS INT) as warehouse_id,
  d.ship_class,
  d.is_middle_mile,
  DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY) AS report_date,
  COALESCE(SUM(d.ShippedOnTime),0)/COALESCE(NULLIF(COUNT(d.ShippedOnTime),0), 1) as fill_rate_mm, -- a small percentage (0.26%) of ShippedOnTime is null, the null handling here defaults the result to 0 if there's no record at all for the specific warehouse and report date
  CURRENT_DATETIME("America/New_York") AS refresh_date,
  "daily" AS cadence
FROM fill_rate_cte d
WHERE 1=1
  -- For daily FR (yesterday)
  AND DATE(d.cptdatetime) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
GROUP BY 1,2,3

UNION ALL

SELECT
  CAST(d.warehouse_id AS INT) as warehouse_id,
  d.ship_class,
  d.is_middle_mile,
  DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY) AS report_date,
  COALESCE(SUM(d.ShippedOnTime),0)/COALESCE(NULLIF(COUNT(d.ShippedOnTime),0), 1) as fill_rate_mm, -- a small percentage (0.26%) of ShippedOnTime is null, the null handling here defaults the result to 0 if there's no record at all for the specific warehouse and report date
  CURRENT_DATETIME("America/New_York") AS refresh_date,
  "weekly" AS cadence
FROM fill_rate_cte d
WHERE 1=1
  -- For weekly FR (from last Sunday to current day)
  AND DATE(d.cptdatetime) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK) and CURRENT_DATE()
GROUP BY 1,2,3


UNION ALL

SELECT

  CAST(d.warehouse_id AS INT) as warehouse_id,
  d.ship_class,
  d.is_middle_mile,
  DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY) AS report_date,
  COALESCE(SUM(d.ShippedOnTime),0)/COALESCE(NULLIF(COUNT(d.ShippedOnTime),0), 1) as fill_rate_mm, -- a small percentage (0.26%) of ShippedOnTime is null, the null handling here defaults the result to 0 if there's no record at all for the specific warehouse and report date
  CURRENT_DATETIME("America/New_York") AS refresh_date,
  "last3days" AS cadence -- this metric is used to caculate the average fillrate for the last 3 days
FROM fill_rate_cte d
WHERE 1=1
  AND DATE(d.cptdatetime) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND CURRENT_DATE()-1
GROUP BY 1,2,3


UNION ALL

SELECT

  CAST(d.warehouse_id AS INT) as warehouse_id,
  d.ship_class,
  d.is_middle_mile,
  DATE_SUB(CURRENT_DATE("America/New_York"), INTERVAL 1 DAY) AS report_date,
  COALESCE(SUM(d.ShippedOnTime),0)/COALESCE(NULLIF(COUNT(d.ShippedOnTime),0), 1) as fill_rate_mm, -- a small percentage (0.26%) of ShippedOnTime is null, the null handling here defaults the result to 0 if there's no record at all for the specific warehouse and report date
  CURRENT_DATETIME("America/New_York") AS refresh_date,
  "daybeforeyesterday" AS cadence -- this metric is used to calculate the day over day change in fillrate between yesterday and day before yesterday
FROM fill_rate_cte d
WHERE 1=1
  AND DATE(d.cptdatetime) = DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)
GROUP BY 1,2,3







{%- if var('ci_run') == true %}
LIMIT {{ var('ci_query_limit') | as_number }}
{%- endif %}