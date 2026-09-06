{% test not_before_column(model, column_name, compare_column) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ compare_column }} is not null
      and {{ column_name }} < {{ compare_column }}

{% endtest %}
