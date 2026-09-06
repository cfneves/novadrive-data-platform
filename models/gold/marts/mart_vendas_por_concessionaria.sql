with base as (

    select
        f.id_vendas,
        f.ano_mes_venda as ano_mes,
        f.id_concessionarias,
        c.concessionaria,
        c.cidade,
        c.estado,
        c.sigla,
        c.regiao,
        f.id_clientes,
        f.id_vendedores,
        f.valor_pago,
        f.valor_desconto
    from {{ ref('fct_vendas') }} as f
    left join {{ ref('dim_concessionaria') }} as c
        on f.id_concessionarias = c.id_concessionarias

),

agregado as (

    select
        ano_mes,
        id_concessionarias,
        concessionaria,
        cidade,
        estado,
        sigla,
        regiao,
        count(id_vendas) as quantidade_vendas,
        sum(valor_pago) as receita_total,
        avg(valor_desconto) as desconto_medio,
        count(distinct id_clientes) as clientes_unicos,
        count(distinct id_vendedores) as vendedores_ativos
    from base
    group by ano_mes, id_concessionarias, concessionaria, cidade, estado, sigla, regiao

),

com_janelas as (

    select
        agregado.*,
        {{ safe_divide('receita_total', 'quantidade_vendas') }} as ticket_medio,
        {{ safe_divide('receita_total', 'sum(receita_total) over (partition by ano_mes)') }} as participacao_receita,
        rank() over (partition by ano_mes order by receita_total desc) as ranking_concessionaria,
        lag(receita_total) over (partition by id_concessionarias order by ano_mes) as receita_mes_anterior
    from agregado

)

select
    ano_mes,
    id_concessionarias,
    concessionaria,
    cidade,
    estado,
    sigla,
    regiao,
    quantidade_vendas,
    receita_total,
    ticket_medio,
    desconto_medio,
    clientes_unicos,
    vendedores_ativos,
    participacao_receita,
    ranking_concessionaria,
    {{ percentage_change('receita_total', 'receita_mes_anterior') }} as crescimento_mensal,
    {{ audit_columns() }}
from com_janelas