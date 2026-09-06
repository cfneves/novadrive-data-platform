{{ config(unique_key='id_vendedores') }}

with source as (

    select
        id_vendedores,
        {{ normalize_text('nome') }} as nome,
        id_concessionarias,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr() }} as _source_modified_at
    from {{ source('bronze', 'vendedores') }}
    where id_vendedores is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

ranked as (

    select
        source.*,
        row_number() over (
            partition by id_vendedores
            order by _source_modified_at desc
        ) as _rn
    from source

),

deduplicated as (

    select
        id_vendedores,
        nome,
        id_concessionarias,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

)

select
    id_vendedores,
    nome,
    id_concessionarias,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from deduplicated
