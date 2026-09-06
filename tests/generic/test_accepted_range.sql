
{% test accepted_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}

    with validation as (
        select {{ column_name }} as value_field
        from {{ model }}
        where {{ column_name }} is not null
    )

    select *
    from validation
    where
        {% if min_value is not none %}
            {% if inclusive %} value_field < {{ min_value }}
            {% else %} value_field <= {{ min_value }}
            {% endif %}
        {% else %} 1 = 0
        {% endif %}
        or
        {% if max_value is not none %}
            {% if inclusive %} value_field > {{ max_value }}
            {% else %} value_field >= {{ max_value }}
            {% endif %}
        {% else %} 1 = 0
        {% endif %}

{% endtest %}
