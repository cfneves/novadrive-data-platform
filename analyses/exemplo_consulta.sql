with ultimo_mes as (
    select max(ano_mes) as ano_mes from {{ ref('mart_vendas_por_vendedor') }}
)

select
    m.ano_mes,
    m.vendedor,
    m.estado,
    m.receita_total,
    m.ranking_vendedor
from {{ ref('mart_vendas_por_vendedor') }} as m
inner join ultimo_mes as u on m.ano_mes = u.ano_mes
where m.ranking_vendedor <= 3
order by m.ranking_vendedor