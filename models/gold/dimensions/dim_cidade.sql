SELECT
    {{generate_surrogate_key(['c.id_cidades'])}} as sk_cidade,
    c.id_cidades,
    c.cidade,
    e.estado,
    e.sigla,
    e.regiao,
    {{audit_columns()}}
from {{ref('silver_cidades')}} as c
left join {{ ref('silver_estados')}} as e
    on c.id_estados = e.id_estados