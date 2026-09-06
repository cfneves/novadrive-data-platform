{{ config(unique_key='id_concessionarias') }}

with source as (

    select
        id_concessionarias,
        {{ normalize_text('concessionaria') }} as concessionaria,
        id_cidades,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr() }} as _source_modified_at
    from {{ source('bronze', 'concessionarias') }}
    where id_concessionarias is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

ranked as (

    select
        source.*,
        row_number() over (
            partition by id_concessionarias
            order by _source_modified_at desc
        ) as _rn
    from source

),

deduplicated as (

    select
        id_concessionarias,
        concessionaria,
        id_cidades,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

)

select
    id_concessionarias,
    concessionaria,
    id_cidades,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from deduplicated
