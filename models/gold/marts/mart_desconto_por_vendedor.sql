with base as (

    select
        f.id_vendas,
        f.ano_mes_venda as ano_mes,
        f.id_vendedores,
        v.nome as vendedor,
        v.concessionaria,
        f.segmento_veiculo,
        f.valor_tabela,
        f.valor_pago,
        f.valor_desconto,
        f.classificacao_desconto
    from {{ ref('fct_vendas') }} as f
    left join {{ ref('dim_vendedor') }} as v
        on f.id_vendedores = v.id_vendedores

),

agregado as (

    select
        ano_mes,
        id_vendedores,
        vendedor,
        concessionaria,
        segmento_veiculo,
        count(id_vendas) as quantidade_vendas,
        sum(valor_tabela) as valor_tabela_total,
        sum(valor_pago) as receita_total,
        sum(valor_desconto) as desconto_total,
        {{ safe_divide('sum(valor_desconto)', 'sum(valor_tabela)') }} as percentual_desconto_medio,
        count(*) filter (where classificacao_desconto = 'com_desconto') as quantidade_com_desconto,
        count(*) filter (where classificacao_desconto = 'sem_desconto') as quantidade_sem_desconto,
        count(*) filter (where classificacao_desconto = 'com_acrescimo') as quantidade_com_acrescimo
    from base
    group by ano_mes, id_vendedores, vendedor, concessionaria, segmento_veiculo

),

com_ranking as (

    select
        agregado.*,
        rank() over (
            partition by ano_mes
            order by percentual_desconto_medio desc
        ) as ranking_desconto
    from agregado

)

select
    ano_mes,
    id_vendedores,
    vendedor,
    concessionaria,
    segmento_veiculo,
    quantidade_vendas,
    valor_tabela_total,
    receita_total,
    desconto_total,
    percentual_desconto_medio,
    quantidade_com_desconto,
    quantidade_sem_desconto,
    quantidade_com_acrescimo,
    ranking_desconto,
    {{ audit_columns() }}
from com_ranking
