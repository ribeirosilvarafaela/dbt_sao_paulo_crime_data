{{
  config(
    materialized = 'table',
    schema = 'gold'
  )
}}

SELECT 
  municipio,
  date_trunc(dt_ocorrencia, month) as mes,
  natureza_apurada,
  bairro,
  count(*) total
FROM {{ ref('sl_ocorrencias_historico') }}
where dt_ocorrencia is not null
group by 1,2,3, 4
