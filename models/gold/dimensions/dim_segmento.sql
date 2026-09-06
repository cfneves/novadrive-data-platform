with segmentos as (

    select distinct
        ordem_segmento as id_seguimento,
        segmento
    from {{ ref('seed_segmentos_veiculos') }}

)

select
    {{ generate_surrogate_key(['id_seguimento']) }} as sk_segmento,
    id_seguimento,
    segmento,
    {{ audit_columns() }}
from segmentos
