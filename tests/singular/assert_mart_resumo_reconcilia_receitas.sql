with fato as (

    select
        ano_mes_venda as ano_mes,
        sum(valor_pago) as receita_fato
    from {{ ref('fct_vendas') }}
    group by ano_mes_venda

),

mart as (

    select
        ano_mes,
        receita_total as receita_mart
    from {{ ref('mart_resumo_mensal') }}

)

select
    f.ano_mes,
    f.receita_fato,
    m.receita_mart
from fato as f
inner join mart as m
    on f.ano_mes = m.ano_mes
where abs(f.receita_fato - m.receita_mart) > {{ var('reconciliacao_tolerancia') }}