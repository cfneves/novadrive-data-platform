with gasto_real as (

    select id_clientes, sum(valor_pago) as total_real
    from {{ ref('silver_vendas') }}
    group by id_clientes

),

gasto_reportado as (

    select id_clientes, total as total_reportado
    from {{ source('bronze_decoy', 'costumer') }}

)

select
    coalesce(gr.id_clientes, gp.id_clientes) as id_clientes,
    gr.total_real,
    gp.total_reportado,
    abs(coalesce(gr.total_real, 0) - coalesce(gp.total_reportado, 0)) as diferenca_absoluta
from gasto_real as gr
full outer join gasto_reportado as gp
    on gr.id_clientes = gp.id_clientes
where abs(coalesce(gr.total_real, 0) - coalesce(gp.total_reportado, 0)) > {{ var('reconciliacao_tolerancia') }}
