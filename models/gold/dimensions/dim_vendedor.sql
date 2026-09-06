select
    {{ generate_surrogate_key(['v.id_vendedores']) }} as sk_vendedor,
    v.id_vendedores,
    v.nome,
    v.id_concessionarias,
    co.concessionaria,
    {{ audit_columns() }}
from {{ ref('silver_vendedores') }} as v
left join {{ ref('silver_concessionarias') }} as co
    on v.id_concessionarias = co.id_concessionarias
