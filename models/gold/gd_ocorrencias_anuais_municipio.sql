{{
    config(materialized='table', schema='gold') 
}}


with ocorrencias as (
select 
  municipio,
  ano_referencia,
  count(*) as ocorrencias
 from {{ ref('sl_ocorrencias_historico') }}
group by 1,2
)
select 
  a.municipio,
  a.ano,
  b.ocorrencias,
  a.populacao
from
  {{ ref('sl_populacao_municipios') }} as a
left join ocorrencias b on a.municipio = b.municipio and a.ano = b.ano_referencia