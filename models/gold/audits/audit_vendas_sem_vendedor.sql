select
    v.id_vendas,
    v.id_vendedores,
    v.data_venda
from {{ ref('silver_vendas') }} as v
left join {{ ref('silver_vendedores') }} as ve
    on v.id_vendedores = ve.id_vendedores
where ve.id_vendedores is null
