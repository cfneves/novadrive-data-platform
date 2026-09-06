{{ config(unique_key='id_vendas') }}

with source as (

    select
        id_vendas,
        id_veiculos,
        id_concessionarias,
        id_vendedores,
        id_clientes,
        cast(valor_pago as decimal(10, 2)) as valor_pago,
        data_venda,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr() }} as _source_modified_at
    from {{ source('bronze', 'vendas') }}
    where id_vendas is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

ranked as (

    select
        source.*,
        row_number() over (
            partition by id_vendas
            order by _source_modified_at desc
        ) as _rn
    from source

),

deduplicated as (

    select
        id_vendas,
        id_veiculos,
        id_concessionarias,
        id_vendedores,
        id_clientes,
        valor_pago,
        data_venda,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

),

com_partes_de_data as (

    select
        id_vendas,
        id_veiculos,
        id_concessionarias,
        id_vendedores,
        id_clientes,
        valor_pago,
        data_venda,
        to_char(data_venda, 'YYYY-MM') as ano_mes_venda,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from deduplicated

)

select
    id_vendas,
    id_veiculos,
    id_concessionarias,
    id_vendedores,
    id_clientes,
    valor_pago,
    data_venda,
    ano_mes_venda,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from com_partes_de_data
