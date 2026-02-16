{{ config(
  materialized='table',
  schema='silver',
  partition_by= {"field": "dt_ocorrencia", "data_type": "date", "granularity": "month"},
  cluster_by=["municipio"]
  ) }}

select
  municipio,
  dt_registro,
  dt_ocorrencia,
  periodo,
  descr_tipo_local,
  rubrica,
  descr_conduta,
  natureza_apurada,
  bairro,
  logradouro,
  latitude,
  longitude,
  periodo_ref,
  source_file,
  current_timestamp() as load_ts,
  ano_referencia,
  mes_referencia
from {{ ref('int_ocorrencia_hist_unificado') }}