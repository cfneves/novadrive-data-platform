with veiculo as (

    select
        id_veiculos,
        nome,
        tipo,
        valor
    from {{ ref('silver_veiculos') }}

),

tipo_enriquecido as (

    select
        v.id_veiculos,
        v.nome,
        v.tipo,
        coalesce(t.tipo_padronizado, v.tipo) as tipo_padronizado,
        coalesce(t.categoria, 'Outros') as categoria_veiculo,
        v.valor
    from veiculo as v
    left join {{ ref('seed_tipos_veiculos') }} as t
        on v.tipo = t.tipo_origem

),

segmento_enriquecido as (

    select
        te.id_veiculos,
        te.nome,
        te.tipo,
        te.tipo_padronizado,
        te.categoria_veiculo,
        te.valor,
        s.segmento as segmento_veiculo,
        s.ordem_segmento
    from tipo_enriquecido as te
    left join {{ ref('seed_segmentos_veiculos') }} as s
        on
            te.valor >= s.valor_minimo
            and (s.valor_maximo is null or te.valor <= s.valor_maximo)

)

select
    {{ generate_surrogate_key(['id_veiculos']) }} as sk_veiculo,
    id_veiculos,
    nome,
    tipo,
    tipo_padronizado,
    categoria_veiculo,
    valor as valor_tabela,
    segmento_veiculo,
    ordem_segmento,
    {{ audit_columns() }}
from segmento_enriquecido
