with canonico as (

    select count(*) as qtd, sum(valor_pago) as total
    from {{ ref('silver_vendas') }}

),

stg_vendas as (

    select count(*) as qtd, sum(valor_pago) as total
    from {{ source('bronze_decoy', 'stg_vendas') }}

),

lista_veiculos as (

    select count(*) as qtd, sum(valor_pago) as total
    from {{ source('bronze_decoy', 'lista_veiculos_compra_clientes') }}

)

select
    'stg_vendas' as tabela_comparada,
    canonico.qtd as qtd_canonico,
    stg_vendas.qtd as qtd_comparado,
    canonico.total as total_canonico,
    stg_vendas.total as total_comparado,
    abs(canonico.total - stg_vendas.total) as diferenca_absoluta
from canonico cross join stg_vendas
where canonico.qtd != stg_vendas.qtd
   or abs(canonico.total - stg_vendas.total) > {{ var('reconciliacao_tolerancia') }}

union all

select
    'lista_veiculos_compra_clientes',
    canonico.qtd,
    lista_veiculos.qtd,
    canonico.total,
    lista_veiculos.total,
    abs(canonico.total - lista_veiculos.total)
from canonico cross join lista_veiculos
where canonico.qtd != lista_veiculos.qtd
   or abs(canonico.total - lista_veiculos.total) > {{ var('reconciliacao_tolerancia') }}
