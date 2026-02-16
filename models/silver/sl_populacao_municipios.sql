{{ config(
  materialized='table',
  schema='silver'
  ) }}

select
    ano,
    {{ norm_txt('nome_municipio') }} as municipio,
    populacao
from
 {{ ref('br_populacao_municipios') }}
