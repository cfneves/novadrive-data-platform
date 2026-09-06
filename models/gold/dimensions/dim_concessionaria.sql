select
    {{ generate_surrogate_key(['co.id_concessionarias']) }} as sk_concessionaria,
    co.id_concessionarias,
    co.concessionaria,
    ci.cidade,
    e.estado,
    e.sigla,
    e.regiao,
    {{ audit_columns() }}
from {{ ref('silver_concessionarias') }} as co
left join {{ ref('silver_cidades') }} as ci
    on co.id_cidades = ci.id_cidades
left join {{ ref('silver_estados') }} as e
    on ci.id_estados = e.id_estados
