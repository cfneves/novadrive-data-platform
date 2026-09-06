select
    v.id_vendas,
    v.id_concessionarias,
    v.data_venda
from {{ ref('silver_vendas') }} as v
left join {{ ref('silver_concessionarias') }} as c
    on v.id_concessionarias = c.id_concessionarias
where c.id_concessionarias is null