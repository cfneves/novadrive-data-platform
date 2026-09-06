{% test must_be_empty(model) %}
    select *
    from {{ model }}

{% endtest %}