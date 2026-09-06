{# trim espaços e converte string vazia para null#}
{% macro normalize_text(column_name) -%}
    nullif(
        trim(
            regexp_replace(cast({{column_name}} as varchar), '\s+', ' ', 'g')
        ),
        ''
    )

{%- endmacro %}

{# trim simples #}
{% macro safe_trim(column_name) -%}
    nullif(trim(cast({{column_name}} as varchar)), '')
{%- endmacro %}

{# converte para maisculas #}
{% macro normalize_upper(column_name) -%}
    upper({{ normalize_text(column_name)}})
{%- endmacro %}
