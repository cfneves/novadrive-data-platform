{% macro classify_discount(valor_tabela, valor_pago) -%}
    case
        when {{ valor_tabela }} is null then null
        when {{ valor_pago }} > {{ valor_tabela }} then 'com_acrescimo'
        when {{ valor_pago }} < {{ valor_tabela }} then 'com_desconto'
        else 'sem_desconto'
    end
{%- endmacro %}