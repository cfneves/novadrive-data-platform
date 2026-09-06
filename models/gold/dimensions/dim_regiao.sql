with regioes as (

    select distinct
        ordem_regiao as id_regiao,
        regiao
    from {{ ref('seed_regioes_estados') }}

)

select
    {{ generate_surrogate_key(['id_regiao']) }} as sk_regiao,
    id_regiao,
    regiao,
    {{ audit_columns() }}
from regioes
