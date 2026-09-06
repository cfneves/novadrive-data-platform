{% macro modification_timestamp_expr(inclusion_col='data_inclusao', update_col='data_atualizacao') -%}
    greatest(
        coalesce({{ inclusion_col }}, timestamp '1900-01-01 00:00:00'),
        coalesce({{ update_col }}, timestamp '1900-01-01 00:00:00')
    )
{%- endmacro %}

{% macro get_incremental_watermark(modification_ts_column, watermark_column=none) -%}
    {%- set watermark_column = watermark_column or modification_ts_column -%}
    {% if is_incremental() %}
    {#- modification_ts_column is normally an alias defined in this same
        select's column list (e.g. _source_modified_at) - aliases from the
        select list aren't visible inside that select's own WHERE clause in
        standard SQL (Postgres enforces this; DuckDB silently allowed it,
        which is how this stayed hidden through the whole DuckDB phase of
        this project). Re-derive the real expression for the source-side
        filter instead of referencing the alias; watermark_column still uses
        the alias name, which is valid there since {{ this }} is an already
        materialized table where that column really exists. #}
    and {{ modification_timestamp_expr() }} >= (
        select coalesce(max({{ watermark_column }}), timestamp '1900-01-01 00:00:00')
        from {{ this }}
    ) - interval '{{ var("incremental_safety_window_minutes", 5) }} minutes'
    {% endif %}
{%- endmacro %}