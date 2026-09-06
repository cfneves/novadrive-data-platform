{{ config(unique_key='id_clientes') }}

with source as (

    select
        id_clientes,
        {{ normalize_text('cliente') }} as cliente,
        {{ safe_trim('endereco') }} as endereco,
        id_concessionarias,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr() }} as _source_modified_at
    from {{ source('bronze', 'clientes') }}
    where id_clientes is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

ranked as (

    select
        source.*,
        row_number() over (
            partition by id_clientes
            order by _source_modified_at desc
        ) as _rn
    from source

),

deduplicated as (

    select
        id_clientes,
        cliente,
        endereco,
        id_concessionarias,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

)

select
    id_clientes,
    cliente,
    endereco,
    id_concessionarias,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from deduplicated
