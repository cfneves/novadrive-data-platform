select
    {{ generate_surrogate_key(['id_estados']) }} as sk_estado,
    id_estados,
    estado,
    sigla,
    regiao,
    ordem_regiao,
    {{ audit_columns() }}
from {{ ref('silver_estados') }}
