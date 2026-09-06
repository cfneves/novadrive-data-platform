select
    v.id_vendas,
    v.id_veiculos,
    v.data_venda
from {{ ref('silver_vendas') }} as v
left join {{ ref('silver_veiculos') }} as ve
    on v.id_veiculos = ve.id_veiculos
where ve.id_veiculos is null