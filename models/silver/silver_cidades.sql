{{ config(unique_key='id_cidades') }}

with source as (

    select
        id_cidades,
        {{ normalize_text('cidade') }} as cidade,
        id_estados,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr() }} as _source_modified_at
    from {{ source('bronze', 'cidades') }}
    where id_cidades is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

ranked as (

    select
        source.*,
        row_number() over (
            partition by id_cidades
            order by _source_modified_at desc
        ) as _rn
    from source

),

deduplicated as (

    select
        id_cidades,
        cidade,
        id_estados,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

)

select
    id_cidades,
    cidade,
    id_estados,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from deduplicated
