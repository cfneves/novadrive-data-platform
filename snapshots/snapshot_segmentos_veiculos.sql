{% snapshot snapshot_segmentos_veiculos %}

{{
    config(
        target_schema='snapshots',
        unique_key='segmento',
        strategy='check',
        check_cols=['valor_minimo', 'valor_maximo']
    )
}}

select * from {{ ref('seed_segmentos_veiculos') }}

{% endsnapshot %}