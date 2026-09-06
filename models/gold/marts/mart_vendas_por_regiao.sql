with base as (

    select
        f.id_vendas,
        f.ano_mes_venda as ano_mes,
        f.id_concessionarias,
        f.id_vendedores,
        f.id_clientes,
        f.valor_pago,
        f.valor_desconto,
        dc.regiao
    from {{ ref('fct_vendas') }} as f
    left join {{ ref('dim_concessionaria') }} as dc
        on f.id_concessionarias = dc.id_concessionarias

),

agregado as (

    select
        ano_mes,
        regiao,
        count(id_vendas) as quantidade_vendas,
        sum(valor_pago) as receita_total,
        avg(valor_desconto) as desconto_medio,
        count(distinct id_concessionarias) as concessionarias_ativas,
        count(distinct id_vendedores) as vendedores_ativos,
        count(distinct id_clientes) as clientes_unicos
    from base
    group by ano_mes, regiao

),

com_janelas as (

    select
        agregado.*,
        {{ safe_divide('receita_total', 'quantidade_vendas') }} as ticket_medio,
        {{ safe_divide('receita_total', 'sum(receita_total) over (partition by ano_mes)') }} as participacao_receita,
        rank() over (partition by ano_mes order by receita_total desc) as ranking_regiao,
        lag(receita_total) over (partition by regiao order by ano_mes) as receita_mes_anterior
    from agregado

)

select
    ano_mes,
    regiao,
    quantidade_vendas,
    receita_total,
    ticket_medio,
    desconto_medio,
    concessionarias_ativas,
    vendedores_ativos,
    clientes_unicos,
    participacao_receita,
    ranking_regiao,
    {{ percentage_change('receita_total', 'receita_mes_anterior') }} as crescimento_mensal,
    {{ audit_columns() }}
from com_janelas
