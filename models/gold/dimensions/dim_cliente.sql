select
    {{ generate_surrogate_key(['cl.id_clientes']) }} as sk_cliente,
    cl.id_clientes,
    cl.cliente,
    cl.endereco,
    co.concessionaria,
    {{ audit_columns() }}
from {{ ref('silver_clientes') }} as cl
left join {{ ref('silver_concessionarias') }} as co
    on cl.id_concessionarias = co.id_concessionarias
