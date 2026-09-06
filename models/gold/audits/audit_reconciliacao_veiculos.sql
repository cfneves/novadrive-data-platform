with maior_valor_pago as (

    select v.nome, max(vd.valor_pago) as valor_maximo_real
    from {{ ref('silver_veiculos') }} as v
    left join {{ ref('silver_vendas') }} as vd
        on v.id_veiculos = vd.id_veiculos
    group by v.nome

)

select
    m.nome,
    m.valor_maximo_real,
    vm."valor_máximo" as valor_reportado,
    abs(m.valor_maximo_real - vm."valor_máximo") as diferenca_absoluta
from maior_valor_pago as m
left join {{ source('bronze_decoy', 'veiculos_mais_caros') }} as vm
    on m.nome = vm."nome_veículo"
where vm."nome_veículo" is null
   or abs(m.valor_maximo_real - vm."valor_máximo") > {{ var('reconciliacao_tolerancia') }}
