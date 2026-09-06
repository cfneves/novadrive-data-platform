with base as (

    select
        id_vendas,
        ano_mes_venda as ano_mes,
        id_vendedores,
        id_concessionarias,
        id_veiculos,
        valor_pago,
        valor_desconto
    from {{ ref('fct_vendas') }}

),

agregado as (

    select
        ano_mes,
        count(id_vendas) as quantidade_vendas,
        sum(valor_pago) as receita_total,
        {{ safe_divide('sum(valor_pago)', 'count(id_vendas)') }} as ticket_medio,
        count(distinct id_vendedores) as vendedores_ativos,
        count(distinct id_concessionarias) as concessionarias_ativas,
        avg(valor_desconto) as desconto_medio
    from base
    group by ano_mes

),

com_crescimento as (

    select
        agregado.*,
        lag(receita_total) over (order by ano_mes) as receita_mes_anterior
    from agregado

),

veiculo_top as (

    select ano_mes, veiculo as veiculo_mais_vendido
    from (
        select
            b.ano_mes,
            v.nome as veiculo,
            count(*) as quantidade,
            row_number() over (
                partition by b.ano_mes order by count(*) desc, v.nome asc
            ) as rn
        from base as b
        left join {{ ref('dim_veiculo') }} as v
            on b.id_veiculos = v.id_veiculos
        group by b.ano_mes, v.nome
    ) as ranked
    where rn = 1

),

vendedor_top as (

    select ano_mes, vendedor as vendedor_maior_receita
    from (
        select
            b.ano_mes,
            v.nome as vendedor,
            sum(b.valor_pago) as receita,
            row_number() over (
                partition by b.ano_mes order by sum(b.valor_pago) desc, v.nome asc
            ) as rn
        from base as b
        left join {{ ref('dim_vendedor') }} as v
            on b.id_vendedores = v.id_vendedores
        group by b.ano_mes, v.nome
    ) as ranked
    where rn = 1

)

select
    c.ano_mes,
    c.receita_total,
    c.quantidade_vendas,
    c.ticket_medio,
    c.vendedores_ativos,
    c.concessionarias_ativas,
    c.desconto_medio,
    {{ percentage_change('c.receita_total', 'c.receita_mes_anterior') }} as crescimento_mensal_receita,
    vt.veiculo_mais_vendido,
    vd.vendedor_maior_receita,
    {{ audit_columns() }}
from com_crescimento as c
left join veiculo_top as vt on c.ano_mes = vt.ano_mes
left join vendedor_top as vd on c.ano_mes = vd.ano_mes
