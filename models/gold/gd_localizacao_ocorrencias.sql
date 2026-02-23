{{ config(
  materialized='incremental',
  schema='gold',
  partition_by={"field": "dt_ocorrencia", "data_type": "date", "granularity": "month"},
  cluster_by=["municipio"]
  ) }}

Select
  'Brasil' as pais,
  'SP' as estado,
  municipio,
  dt_ocorrencia,
  bairro,
  natureza_apurada,
  latitude,
  longitude,
  concat(cast(latitude as string),',',cast(longitude as string)) as coordenadas
From {{ ref('sl_ocorrencias_historico') }}
Where latitude is not null
and longitude is not null
and dt_ocorrencia between '2022-01-01' and '2025-12-31'  