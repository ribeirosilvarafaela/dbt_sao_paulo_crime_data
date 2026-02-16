{{ config(
    materialized = 'view',
    schema = 'bronze'
) }}

select
  NOME_DEPARTAMENTO,
  NOME_SECCIONAL,
  NOME_DELEGACIA,
  NOME_MUNICIPIO,
  NUM_BO,
  ANO_BO,
  {{ normalize_null_and_cast('DATA_REGISTRO', 'timestamp') }} as DATA_REGISTRO,
  {{ normalize_null_and_cast('DATA_OCORRENCIA_BO', 'timestamp') }} as DATA_OCORRENCIA_BO,
  HORA_OCORRENCIA_BO,
  DESC_PERIODO,
  DESCR_SUBTIPOLOCAL,
  BAIRRO,
  LOGRADOURO,
  {{ normalize_null_and_cast('NUMERO_LOGRADOURO', 'int64') }} as NUMERO_LOGRADOURO,
  {{ normalize_null_and_cast('LATITUDE', 'float64') }} as LATITUDE,
  {{ normalize_null_and_cast('LONGITUDE', 'float64') }} as LONGITUDE,
  NOME_DELEGACIA_CIRCUNSCRICAO,
  NOME_DEPARTAMENTO_CIRCUNSCRICAO,
  NOME_SECCIONAL_CIRCUNSCRICAO,
  NOME_MUNICIPIO_CIRCUNSCRICAO,
  RUBRICA,
  DESCR_CONDUTA,
  NATUREZA_APURADA,
  {{ normalize_null_and_cast('MES_ESTATISTICA', 'int64') }} as MES_ESTATISTICA,
  {{ normalize_null_and_cast('ANO_ESTATISTICA', 'int64') }} as ANO_ESTATISTICA,
  '2023_jan_jun' as periodo_ref,
  'spdadoscriminais_2023__jan_jun_2023.csv' as source_file,
  current_timestamp() as load_ts
from 
  {{ ref('spdadoscriminais_2023__jan_jun_2023') }}
