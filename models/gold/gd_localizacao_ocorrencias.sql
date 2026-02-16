{{ config(
  materialized='table',
  schema='gold'
  ) }}

Select
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