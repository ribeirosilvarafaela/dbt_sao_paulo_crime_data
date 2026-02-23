{{
  config(
    materialized='incremental',
    schema='gold',
    partition_by={"field": "mes", "data_type": "date", "granularity": "month"},
    cluster_by=["municipio"]
  )
}}

with meses as (
select distinct
  ano_referencia as ano,
  mes_referencia as mes_referencia,
  date(ano_referencia, mes_referencia, 1) as mes
from {{ ref('sl_ocorrencias_historico') }}
),
ocorrencias as (
select
  municipio,
  ano_referencia as ano,
  mes_referencia,
  date(ano_referencia, mes_referencia, 1) as mes,
  count(*) as ocorrencias
from {{ ref('sl_ocorrencias_historico') }}
group by 1,2,3,4
),
populacao_mes as (
select
  p.municipio,
  p.ano,
  m.mes_referencia,
  m.mes,
  coalesce(o.ocorrencias, 0) as ocorrencias,
  p.populacao
from {{ ref('sl_populacao_municipios') }} as p
inner join meses m on p.ano = m.ano
left join ocorrencias o
  on p.municipio = o.municipio
 and p.ano = o.ano
 and m.mes_referencia = o.mes_referencia
),
clustered as (
select
  *,
  case
    when populacao < 50000 then 'Menos de 50k'
    when populacao >= 50000 and populacao < 100000 then 'Entre 50k e 100k'
    when populacao >= 100000 and populacao < 500000 then 'Entre 100k e 500k'
    when populacao >= 500000 and populacao < 1000000 then 'Entre 500k e 1M'
    else 'Mais de 1M'
  end as cluster_populacao
from populacao_mes
),
estadual as (
select
  ano,
  mes_referencia,
  mes,
  sum(ocorrencias) as ocorrencias_estaduais,
  sum(populacao) as populacao_estadual,
  (sum(ocorrencias) * 100000.0) / nullif(sum(populacao), 0) as taxa_estadual_100k
from populacao_mes
group by 1,2,3
),
media_cluster as (
select
  cluster_populacao,
  ano,
  mes_referencia,
  mes,
  sum(ocorrencias) as ocorrencias_cluster,
  sum(populacao) as populacao_cluster,
  (sum(ocorrencias) * 100000.0) / nullif(sum(populacao), 0) as taxa_cluster_100k
from clustered
group by 1,2,3,4
),
municipio_cluster as (
select
  municipio,
  ano,
  mes_referencia,
  mes,
  cluster_populacao,
  (sum(ocorrencias) * 100000.0) / nullif(sum(populacao), 0) as taxa_municipio_100k
from clustered
group by 1,2,3,4,5
),
final as (
select
  a.municipio,
  a.ano,
  a.mes_referencia,
  a.mes,
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
  round(((d.taxa_municipio_100k / nullif(b.taxa_estadual_100k, 0) - 1) * 100), 2) as desvio_vs_estado_pct,
  round(((d.taxa_municipio_100k / nullif(c.taxa_cluster_100k, 0) - 1) * 100), 2) as desvio_vs_cluster_pct,
  round(lag(d.taxa_municipio_100k) over (partition by a.municipio order by a.mes), 2) as taxa_municipio_mes_anterior,
  round(lag(b.taxa_estadual_100k) over (partition by a.municipio order by a.mes), 2) as taxa_estado_mes_anterior
from clustered a
left join estadual b
  on a.ano = b.ano
 and a.mes_referencia = b.mes_referencia
left join media_cluster c
  on a.cluster_populacao = c.cluster_populacao
 and a.ano = c.ano
 and a.mes_referencia = c.mes_referencia
left join municipio_cluster d
  on a.municipio = d.municipio
 and a.ano = d.ano
 and a.mes_referencia = d.mes_referencia
)
select * from final