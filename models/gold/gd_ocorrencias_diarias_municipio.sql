{{ config(
  materialized='incremental',
  alias='gd_ocorrencias_diarias_municipio',
  partition_by={"field": "dt_ocorrencia", "data_type": "date", "granularity": "month"},
  schema='gold'
  ) }}

select
  municipio,
  dt_ocorrencia,
 count(*) total
from {{ ref('sl_ocorrencias_historico') }}
where dt_ocorrencia is not null
group by 1,2
