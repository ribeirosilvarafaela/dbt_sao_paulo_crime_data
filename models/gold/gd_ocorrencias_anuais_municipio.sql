{{
  config(
    materialized='incremental',
    schema='gold',
    partition_by={"field": "periodo_ano", "data_type": "date", "granularity": "year"},
    cluster_by=["municipio"]
  )
}}


with ocorrencias as (
select 
  municipio,
  ano_referencia,
  count(*) as ocorrencias
 from {{ ref('sl_ocorrencias_historico') }}
group by 1,2
)
, populacao_ano as (
select 
  a.municipio,
  a.ano,
  coalesce(b.ocorrencias, 0) as ocorrencias,
  a.populacao
from
  {{ ref('sl_populacao_municipios') }} as a
left join ocorrencias b on a.municipio = b.municipio and a.ano = b.ano_referencia
)
, clustered as (
select 
  *,
  case 
  when populacao < 50000 then 'Menos de 50k'
  when populacao >= 50000 and populacao < 100000 then 'Entre 50k e 100k'
  when populacao >= 100000 and populacao < 500000 then 'Entre 100k e 500k'
  when populacao >= 500000 and populacao < 1000000 then 'Entre 500k e 1M'
  else 'Mais de 1M' end as cluster_populacao
from populacao_ano
)
, estadual as (
select
  ano, 
  sum(ocorrencias) as ocorrencias_estaduais,
  sum(populacao) as populacao_estadual, 
  (sum(ocorrencias) * 100000.0) / sum(populacao) as taxa_estadual_100k
from
populacao_ano
group by 1
)
, media_cluster as (
select 
  cluster_populacao,
  ano,
  sum(ocorrencias) as ocorrencias_cluster,
  sum(populacao) as populacao_cluster,
  (sum(ocorrencias) * 100000.0) / sum(populacao) as taxa_cluster_100k
from clustered
group by 1,2
)
, municipio_cluster as (
select
  municipio,
  ano,
  cluster_populacao,
  (sum(ocorrencias) * 100000.0) / sum(populacao) as taxa_municipio_100k
from clustered
group by 1,2,3
)
, final as (
select 
  a.municipio,
  a.ano,
  date(a.ano, 1, 1) as periodo_ano,
  a.ocorrencias,
  a.populacao,
  a.cluster_populacao,
  b.ocorrencias_estaduais,
  b.populacao_estadual,
  round(b.taxa_estadual_100k, 2) as taxa_estadual_100k,
  c.ocorrencias_cluster,
  c.populacao_cluster,
  round(c.taxa_cluster_100k, 2) as taxa_cluster_100k,
  round(d.taxa_municipio_100k, 2) as taxa_municipio_100k,
  round(((d.taxa_municipio_100k / nullif(b.taxa_estadual_100k, 0) -1) * 100), 2) as desvio_vs_estado_pct,
  round(((d.taxa_municipio_100k / nullif(c.taxa_cluster_100k, 0) -1) * 100), 2) as desvio_vs_cluster_pct,
  round(lag(d.taxa_municipio_100k) over (partition by a.municipio order by a.ano), 2) as taxa_municipio_ano_anterior,
  round(lag(b.taxa_estadual_100k) over (partition by a.municipio order by a.ano), 2) as taxa_estado_ano_anterior
from clustered a
left join estadual b on a.ano = b.ano
left join media_cluster c on a.cluster_populacao = c.cluster_populacao and a.ano = c.ano
left join municipio_cluster d on a.municipio = d.municipio and a.ano = d.ano
)
select * from final