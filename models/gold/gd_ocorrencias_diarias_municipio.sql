{{ config(
  materialized='table',
  alias='gd_ocorrencias_diarias_municipio',
  schema='gold'
  ) }}

select
  municipio,
  dt_ocorrencia,
 count(*) total
from {{ ref('sl_ocorrencias_historico') }}
where dt_ocorrencia is not null
group by 1,2
