{{ config(
  materialized='incremental',
  alias='gd_ocorrencias_diarias_municipio',
  partition_by={"field": "dt_ocorrencia", "data_type": "date", "granularity": "month"},
  schema='gold'
  ) }}

with populacao_municipio as (
  select
    municipio,
    ano,
    populacao
  from {{ ref('sl_populacao_municipios') }}
)
select
  a.municipio,
  dt_ocorrencia,
    case
    when populacao < 50000 then 'Menos de 50k'
    when populacao >= 50000 and populacao < 100000 then 'Entre 50k e 100k'
    when populacao >= 100000 and populacao < 500000 then 'Entre 100k e 500k'
    when populacao >= 500000 and populacao < 1000000 then 'Entre 500k e 1M'
    else 'Mais de 1M'
  end as cluster_populacao,
  avg(populacao) as populacao_ano,
  count(*) total
from {{ ref('sl_ocorrencias_historico') }} a
left join populacao_municipio p on a.municipio = p.municipio and a.ano_referencia = p.ano
where dt_ocorrencia >= '2022-01-01'
group by 1,2,3
