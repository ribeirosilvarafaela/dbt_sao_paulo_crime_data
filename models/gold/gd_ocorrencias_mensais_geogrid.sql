{{ config(
  materialized='incremental',
  schema='gold',
    partition_by={"field": "mes", "data_type": "date", "granularity": "month"},
  cluster_by=["municipio"]
  ) }}


SELECT 
  estado,
  municipio,
  DATE_TRUNC(dt_ocorrencia, MONTH) AS mes,
  ROUND(latitude, 2)  AS lat_grid,
  ROUND(longitude, 2) AS lon_grid,
  CONCAT(cast(ROUND(latitude, 2) as string),',',cast(ROUND(longitude, 2) as string)) AS geogrid,
  COUNT(*) AS total_ocorrencias
FROM `sao-paulo-crime-data.sp_crime_analytics_gold.gd_localizacao_ocorrencias`
WHERE dt_ocorrencia >= DATE '2025-01-01'
GROUP BY 1,2,3,4,5,6