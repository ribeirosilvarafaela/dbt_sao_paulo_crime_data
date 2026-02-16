
{{ config(
    materialized = 'view',
    schema = 'bronze'
) }}

select
  *,
  current_timestamp() as load_ts
from 
  {{ ref('spdadoscriminais_populacao_municipios') }}