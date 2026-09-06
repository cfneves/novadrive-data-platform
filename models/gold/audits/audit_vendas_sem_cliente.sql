select
    v.id_vendas,
    v.id_clientes,
    v.data_venda
from {{ ref('silver_vendas') }} as v
left join {{ ref('silver_clientes') }} as c
    on v.id_clientes = c.id_clientes
where c.id_clientes is null