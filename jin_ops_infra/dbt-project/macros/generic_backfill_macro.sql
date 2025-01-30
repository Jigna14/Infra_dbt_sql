{% macro generic_backfill_macro(time_value,
regular_run_start,
regular_run_end,
regular_backfill_start,
regular_backfill_end,
backfill_condition) -%}

{%- if (var("manual_backfill",'') == True and var("backfill_start",'')!='' and var("backfill_end",'') != '') -%}
{{ time_value}} BETWEEN {{var("backfill_start")}} AND {{var("backfill_end")}}
{%- else -%}
{{ time_value }} BETWEEN IF({{backfill_condition}}, {{regular_run_start}}, {{regular_backfill_start}}) AND IF({{backfill_condition}}, {{regular_run_end}}, {{regular_backfill_end}})
{%- endif -%}
{%- endmacro -%}