# sp_crime_dbt

Projeto dbt para modelagem de dados de criminalidade do estado de Sao Paulo, com base em arquivos CSV versionados em `seeds/`.

## Objetivo

Este projeto tem como objetivo transformar dados brutos de criminalidade do estado de Sao Paulo em uma base historica confiavel, padronizada e pronta para analise.

Na pratica, o pipeline em dbt foi estruturado para:

- consolidar diferentes arquivos e periodos em um historico unico de ocorrencias (2022 a 2025)
- padronizar nomes de colunas, tipos de dados e valores nulos para garantir consistencia entre fontes
- organizar os dados por camadas (`bronze`, `silver` e `gold`) para separar ingestao, tratamento e consumo analitico
- facilitar analises temporais e geograficas (municipio, latitude/longitude, natureza da ocorrencia), apoiando monitoramento, investigacao e tomada de decisao orientada por dados
- disponibilizar uma base reutilizavel para dashboards, estudos exploratorios e indicadores de seguranca publica

- `bronze`: padronizacao inicial por periodo de origem
- `silver`: consolidacao historica e normalizacao de colunas
- `gold`: camada analitica final (estrutura pronta, sem modelos ativos no momento)

## Estrutura do projeto

- `models/bronze/`: modelos de ingestao a partir de seeds (`view`)
- `models/silver/`: modelos de consolidacao historica (`view` e `table`)
- `models/gold/`: reservado para marts analiticos
- `seeds/`: CSVs de entrada e `seeds_schema.yml`
- `macros/`: macros utilitarias (`normalize_null_and_cast`, `generate_schema_name`)
- `tests/`: pasta para testes SQL customizados
- `snapshots/` e `analyses/`: estrutura padrao dbt

## Materializacao e schemas

Configuracao em `dbt_project.yml`:

- `bronze` -> `view`
- `silver` -> `table`
- `gold` -> `table`

Nos objetos criados, o schema final segue a macro `generate_schema_name`:

- schema base do target + sufixo da camada
- exemplo: se `target.schema = sp_crime`, uma tabela de `silver` sera criada em `sp_crime_silver`

## Principais modelos atuais

Em `silver`:

- `int_crime_hist_unified`: unifica periodos 2022-2025 da camada bronze
- `sl_crime_hist`: tabela historica consolidada para consumo analitico

Em `bronze`:

- modelos por semestre/ano (`br_crime_2022_*` ate `br_crime_2025_*`)
- `br_populacao_municipios`

## Pre-requisitos

- Python 3.10+
- `dbt-core` e adapter do seu warehouse (ex.: `dbt-bigquery`)
- profile `sp_crime_dbt` configurado em `profiles.yml`

## Como executar

1. Validar conexao:

```bash
dbt debug
```

2. Carregar seeds:

```bash
dbt seed
```

3. Executar modelos:

```bash
dbt run
```

4. Executar testes:

```bash
dbt test
```

5. Pipeline completa:

```bash
dbt build
```

## Execucao por camada

```bash
dbt run --select models/bronze
dbt run --select models/silver
dbt run --select models/gold
```

## Documentacao

```bash
dbt docs generate
dbt docs serve
```

## Limpeza de artefatos

Limpa artefatos padrao configurados no projeto (`target/` e `dbt_packages/`):

```bash
dbt clean
```

Para remover tambem logs locais de execucao:

```bash
rm -rf logs
```

No Windows (PowerShell):

```powershell
if (Test-Path logs) { cmd /c "rd /s /q logs" }
```

## Referencias

- dbt docs: https://docs.getdbt.com/docs/introduction
