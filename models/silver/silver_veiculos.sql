{{ config(unique_key='id_veiculos') }}

with source as (

    select
        id_veiculos,
        {{ normalize_text('nome') }} as nome,
        {{ standardize_vehicle_type('tipo') }} as tipo,
        cast(valor as decimal(10, 2)) as valor,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr() }} as _source_modified_at
    from {{ source('bronze', 'veiculos') }}
    where id_veiculos is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

ranked as (

    select
        source.*,
        row_number() over (
            partition by id_veiculos
            order by _source_modified_at desc
        ) as _rn
    from source

),

deduplicated as (

    select
        id_veiculos,
        nome,
        tipo,
        valor,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

)

select
    id_veiculos,
    nome,
    tipo,
    valor,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from deduplicated
