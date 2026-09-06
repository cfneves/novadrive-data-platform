{% macro audit_columns() -%}
    current_timestamp as _dbt_processed_at,
    '{{ invocation_id }}' as _dbt_invocation_id
{%- endmacro %}
