with base as (

    select
        f.id_vendas,
        f.ano_mes_venda as ano_mes,
        f.segmento_veiculo,
        f.tipo_padronizado,
        f.valor_pago
    from {{ ref('fct_vendas') }} as f

),

agregado as (

    select
        ano_mes,
        segmento_veiculo,
        tipo_padronizado,
        count(id_vendas) as quantidade_vendas,
        sum(valor_pago) as receita_total
    from base
    group by ano_mes, segmento_veiculo, tipo_padronizado

),

com_participacao as (

    select
        agregado.*,
        {{ safe_divide('receita_total', 'quantidade_vendas') }} as ticket_medio,
        {{ safe_divide('receita_total', 'sum(receita_total) over (partition by ano_mes)') }} as participacao_receita
    from agregado

)

select
    ano_mes,
    segmento_veiculo,
    tipo_padronizado,
    quantidade_vendas,
    receita_total,
    ticket_medio,
    participacao_receita,
    {{ audit_columns() }}
from com_participacao
