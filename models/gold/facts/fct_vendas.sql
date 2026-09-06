{{ config(unique_key='id_vendas') }}


with vendas as (

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
        _source_modified_at
    from {{ ref('silver_vendas') }}
    where id_vendas is not null

    {{ get_incremental_watermark('_source_modified_at') }}

),

veiculo as (

    select
        id_veiculos,
        valor_tabela,
        segmento_veiculo,
        tipo_padronizado
    from {{ ref('dim_veiculo') }}

),

concessionaria_geo as (

    select
        co.id_concessionarias,
        co.id_cidades,
        ci.id_estados,
        e.regiao
    from {{ ref('silver_concessionarias') }} as co
    left join {{ ref('silver_cidades') }} as ci
        on co.id_cidades = ci.id_cidades
    left join {{ ref('silver_estados') }} as e
        on ci.id_estados = e.id_estados

),

regiao as (

    select
        id_regiao,
        regiao
    from {{ ref('dim_regiao') }}

),

segmento as (

    select
        id_seguimento,
        segmento
    from {{ ref('dim_segmento') }}

),

enriquecido as (

    select
        v.id_vendas,
        v.id_veiculos,
        v.id_vendedores,
        v.id_clientes,
        v.id_concessionarias,
        cg.id_cidades,
        cg.id_estados,
        r.id_regiao,
        cg.regiao,
        1 as quantidade_venda,
        ve.valor_tabela,
        v.valor_pago,
        (ve.valor_tabela - v.valor_pago) as valor_desconto,
        {{ safe_divide('(ve.valor_tabela - v.valor_pago)', 've.valor_tabela') }} as percentual_desconto,
        {{ classify_discount('ve.valor_tabela', 'v.valor_pago') }} as classificacao_desconto,
        s.id_seguimento,
        ve.segmento_veiculo,
        ve.tipo_padronizado,
        v.data_venda,
        v.ano_mes_venda,
        v.data_inclusao,
        v.data_atualizacao,
        v._source_modified_at
    from vendas as v
    left join veiculo as ve
        on v.id_veiculos = ve.id_veiculos
    left join concessionaria_geo as cg
        on v.id_concessionarias = cg.id_concessionarias
    left join regiao as r
        on cg.regiao = r.regiao
    left join segmento as s
        on ve.segmento_veiculo = s.segmento

)

select
    id_vendas,
    id_veiculos,
    id_vendedores,
    id_clientes,
    id_concessionarias,
    id_cidades,
    id_estados,
    id_regiao,
    regiao,
    quantidade_venda,
    valor_tabela,
    valor_pago,
    valor_desconto,
    percentual_desconto,
    classificacao_desconto,
    id_seguimento,
    segmento_veiculo,
    tipo_padronizado,
    data_venda,
    ano_mes_venda,
    data_inclusao,
    data_atualizacao,
    _source_modified_at,
    {{ audit_columns() }}
from enriquecido
