{{ config(
  materialized='view',
  schema='silver'
  ) }}

with base as (

  select
    {{ norm_txt('CIDADE') }} as municipio,
    date(DATA_COMUNICACAO_BO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESCR_PERIODO)) as periodo,
    upper(trim(DESCR_TIPOLOCAL)) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2022) as ano_referencia,
    EXTRACT(MONTH FROM coalesce(DATA_OCORRENCIA_BO, DATA_COMUNICACAO_BO)) as mes_referencia,
    COALESCE(DATA_OCORRENCIA_BO, DATA_COMUNICACAO_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2022_jan_jun') }}

  union all

  select
    {{ norm_txt('CIDADE') }} as municipio,
    date(DATA_COMUNICACAO_BO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESCR_PERIODO)) as periodo,
    upper(trim(DESCR_TIPOLOCAL)) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2022) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_OCORRENCIA_BO, DATA_COMUNICACAO_BO)) as mes_referencia,
    COALESCE(DATA_OCORRENCIA_BO, DATA_COMUNICACAO_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2022_jul_dez') }}

  union all

  select
    {{ norm_txt('NOME_MUNICIPIO') }} as municipio,
    date(DATA_REGISTRO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESC_PERIODO)) as periodo,
    upper(trim(DESCR_SUBTIPOLOCAL)) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2023) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO)) as mes_referencia,
    COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2023_jan_jun') }}

  union all

  select
    {{ norm_txt('NOME_MUNICIPIO') }} as municipio,
    date(DATA_REGISTRO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESC_PERIODO)) as periodo,
    upper(trim(DESCR_SUBTIPOLOCAL)) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2023) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO)) as mes_referencia,
    COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2023_jul_dez') }}

  union all

  select
    {{ norm_txt('NOME_MUNICIPIO') }} as municipio,
    date(DATA_REGISTRO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESC_PERIODO)) as periodo,
    upper(trim(DESCR_SUBTIPOLOCAL)) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2024) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO)) as mes_referencia,
    COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2024_jan_jun') }}

  union all

  select
    {{ norm_txt('NOME_MUNICIPIO') }} as municipio,
    date(DATA_REGISTRO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESC_PERIODO)) as periodo,
    upper(trim(DESCR_SUBTIPOLOCAL)) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2024) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO)) as mes_referencia,
    COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2024_jul_dez') }}

  union all

  select
    {{ norm_txt('NOME_MUNICIPIO') }} as municipio,
    date(DATA_REGISTRO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESC_PERIODO)) as periodo,
    upper(trim(coalesce(DESCR_SUBTIPOLOCAL, DESCR_TIPOLOCAL))) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2025) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO)) as mes_referencia,
    COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2025_jan_jun') }}

  union all

  select
    {{ norm_txt('NOME_MUNICIPIO') }} as municipio,
    date(DATA_REGISTRO) as dt_registro,
    date(DATA_OCORRENCIA_BO) as dt_ocorrencia,
    upper(trim(DESC_PERIODO)) as periodo,
    upper(trim(coalesce(DESCR_SUBTIPOLOCAL, DESCR_TIPOLOCAL))) as descr_tipo_local,
    RUBRICA as rubrica,
    DESCR_CONDUTA as descr_conduta,
    NATUREZA_APURADA as natureza_apurada,
    {{ norm_txt('BAIRRO') }} as bairro,
    {{ norm_txt('LOGRADOURO') }} as logradouro,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    COALESCE(ANO_ESTATISTICA, 2025) as ano_referencia,
    EXTRACT(MONTH FROM COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO)) as mes_referencia,
    COALESCE(DATA_REGISTRO, DATA_OCORRENCIA_BO) as data_referencia,
    periodo_ref,
    source_file,
    load_ts as load_ts_bronze
  from {{ ref('br_crime_2025_jul_dez') }}
)

select * from base
