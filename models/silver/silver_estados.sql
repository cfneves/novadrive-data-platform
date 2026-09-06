{{config(unique_key='id_estados')}}

with source as (
    select 
        id_estados,
        {{normalize_text('estado')}} as estado,
        {{normalize_upper('sigla')}} as sigla,
        data_inclusao,
        data_atualizacao,
        {{ modification_timestamp_expr()}} as _source_modified_at

    from {{ source('bronze', 'estados') }}
    where id_estados is not null
    {{ get_incremental_watermark('_source_modified_at')}}


),
ranked as (
    select
     source.*,
     row_number() over (
        partition by id_estados
        order by _source_modified_at desc
    ) as _rn
    from source

), 

deduplicated as (
    select
        id_estados,
        estado,
        sigla,
        data_inclusao,
        data_atualizacao,
        _source_modified_at
    from ranked
    where _rn = 1

),

enriquecido as (
        select 
            d.id_estados,
            d.estado,
            d.sigla,
            r.regiao,
            r.ordem_regiao,
            d.data_atualizacao,
            d.data_inclusao,
            d._source_modified_at
        from deduplicated d
        left join {{ ref('seed_regioes_estados') }} as r
         on d.sigla = r.sigla

)

select 
    id_estados,
    estado,
    sigla,
    regiao,
    ordem_regiao,
    data_atualizacao,
    data_inclusao,
    _source_modified_at,
    {{ audit_columns() }}

 from enriquecido