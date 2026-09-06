{% test not_far_future_date(model, column_name, max_days_ahead=1) %}

    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} > current_date + interval '{{ max_days_ahead }} days'

{% endtest %}