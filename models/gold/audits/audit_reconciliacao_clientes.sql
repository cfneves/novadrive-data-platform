with canonico as (

    select count(*) as qtd
    from {{ ref('silver_clientes') }}

),

costumer as (

    select count(*) as qtd
    from {{ source('bronze_decoy', 'costumer') }}

),

stg_costumer as (

    select count(*) as qtd
    from {{ source('bronze_decoy', 'stg_costumer') }}

),

concessionaria_clientes as (

    select count(distinct concessionaria || '|' || cliente) as qtd
    from {{ source('bronze_decoy', 'concessionaria_clientes') }}

)

select
    'costumer' as tabela_comparada,
    canonico.qtd as qtd_canonico,
    costumer.qtd as qtd_comparado,
    abs(canonico.qtd - costumer.qtd) as diferenca_absoluta
from canonico cross join costumer
where canonico.qtd != costumer.qtd

union all

select
    'stg_costumer',
    canonico.qtd,
    stg_costumer.qtd,
    abs(canonico.qtd - stg_costumer.qtd)
from canonico cross join stg_costumer
where canonico.qtd != stg_costumer.qtd

union all

select
    'concessionaria_clientes',
    canonico.qtd,
    concessionaria_clientes.qtd,
    abs(canonico.qtd - concessionaria_clientes.qtd)
from canonico cross join concessionaria_clientes
where canonico.qtd != concessionaria_clientes.qtd
