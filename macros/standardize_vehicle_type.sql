{% macro standardize_vehicle_type(column_name) -%}
    {{ normalize_upper(column_name) }}
{%- endmacro %}