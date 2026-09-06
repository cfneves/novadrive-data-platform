{% macro create_run_log_table() %}
create table if not exists bronze._dbt_run_log (
    invocation_id varchar,
    event varchar,
    occurred_at timestamp,
    target_name varchar,
    dbt_version varchar,
    resultado_resumo varchar
)
{% endmacro %}

{% macro log_run_event(event) %}
insert into bronze._dbt_run_log (invocation_id, event, occurred_at, target_name, dbt_version, resultado_resumo)
values (
    '{{ invocation_id }}',
    '{{ event }}',
    current_timestamp,
    '{{ target.name }}',
    '{{ dbt_version }}',
    {% if results is defined %}
    '{{ results | length }} nos executados, {{ results | selectattr("status", "equalto", "error") | list | length }} erro(s)'
    {% else %}
    null
    {% endif %}
)
{% endmacro %}
