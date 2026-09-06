with base as (

    select
        f.id_vendas,
        f.ano_mes_venda as ano_mes,
        f.id_vendedores,
        v.nome as vendedor,
        v.concessionaria,
        dc.estado,
        dc.sigla,
        f.id_clientes,
        f.valor_pago,
        f.valor_desconto
    from {{ ref('fct_vendas') }} as f
    left join {{ ref('dim_vendedor') }} as v
        on f.id_vendedores = v.id_vendedores
    left join {{ ref('dim_concessionaria') }} as dc
        on v.id_concessionarias = dc.id_concessionarias

),

agregado as (

    select
        ano_mes,
        id_vendedores,
        vendedor,
        concessionaria,
        estado,
        sigla,
        count(id_vendas) as quantidade_vendas,
        sum(valor_pago) as receita_total,
        avg(valor_desconto) as desconto_medio,
        count(distinct id_clientes) as clientes_unicos
    from base
    group by ano_mes, id_vendedores, vendedor, concessionaria, estado, sigla

),

com_janelas as (

    select
        agregado.*,
        {{ safe_divide('receita_total', 'quantidade_vendas') }} as ticket_medio,
        rank() over (partition by ano_mes order by receita_total desc) as ranking_vendedor,
        lag(receita_total) over (partition by id_vendedores order by ano_mes) as receita_mes_anterior
    from agregado

)

select
    ano_mes,
    id_vendedores,
    vendedor,
    concessionaria,
    estado,
    sigla,
    quantidade_vendas,
    receita_total,
    ticket_medio,
    desconto_medio,
    clientes_unicos,
    ranking_vendedor,
    {{ percentage_change('receita_total', 'receita_mes_anterior') }} as crescimento_mensal,
    {{ audit_columns() }}
from com_janelas
