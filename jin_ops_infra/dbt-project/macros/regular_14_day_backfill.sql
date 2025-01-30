{% macro regular_14_day_backfill() -%}
  {% set current_hour = adapter.execute('select extract(HOUR from current_datetime("America/New_York"))') %}
  {% set current_day = adapter.execute('select extract(DAY from current_date)') %}
  {% set current_weekday = adapter.execute('select extract(DAYOFWEEK from current_date)') %}


  {%- if current_day == 1 and current_weekday == 0 -%}
    CURRENT_DATE() - 28
  {%- elif current_hour == 20 or current_hour == 21 or current_hour == 22 or current_hour == 23 -%}
    CURRENT_DATE() - 14
  {%- else -%}
    CURRENT_DATE() - 7
  {%- endif -%}
{%- endmacro %}