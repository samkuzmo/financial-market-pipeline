# Financial Market Pipeline

Pipeline de Engenharia de Dados para ingestão, processamento e análise de dados financeiros utilizando uma arquitetura moderna de Data Lake baseada em camadas Bronze, Silver e Gold.

O projeto implementa um pipeline completo de dados financeiros utilizando dados históricos da Yahoo Finance API, armazenamento em AWS S3, processamento analítico com DuckDB, orquestração com Apache Airflow e consumo analítico através de ferramentas de BI.

---

# Arquitetura

O pipeline segue uma arquitetura de Data Lake em camadas utilizando o conceito de Medallion Architecture.

Fluxo geral:

![Arquitetura do Pipeline](https://github.com/samkuzmo/financial-market-pipeline/blob/main/images/Pipeline_architecture.png)

---

# Stack do Projeto

## Linguagens e Processamento

* Python
* SQL
* DuckDB

## Engenharia de Dados

* Apache Airflow
* Docker
* AWS S3
* Apache Athena
* Parquet

## Fonte de Dados

* Yahoo Finance API utilizando yFinance

## Visualização

* Power BI

---

# Camadas do Data Lake

## Bronze Layer

Responsável pela ingestão dos dados brutos provenientes da Yahoo Finance API.

Características:

* Dados históricos OHLCV dos ativos financeiros
* Persistência em formato Parquet
* Armazenamento particionado no AWS S3
* Inclusão de metadados de ingestão
* Validação automática da qualidade dos dados
* Geração de relatórios de validação em JSON

Estrutura:

```text
bronze/
└── ingestion_date=YYYY-MM-DD/
    ├── market_data_raw_TIMESTAMP.parquet
    └── ingestion_report_TIMESTAMP.json
```

O relatório de validação contém informações como:

* quantidade de registros ingeridos
* ativos esperados e ativos ausentes
* valores nulos encontrados
* registros duplicados
* schema dos dados
* intervalo de datas processado
* tempo de execução

![Exemplo JSON Bronze](https://github.com/samkuzmo/financial-market-pipeline/blob/main/images/Exemplo_JSON_bronze.png)

---

## Silver Layer

Responsável pelo processamento analítico e transformação dos dados brutos.

Principais operações:

* padronização do schema
* tratamento de dados
* cálculo de retornos diários
* médias móveis
* indicadores de volatilidade
* criação de features analíticas

A camada Silver utiliza dados históricos disponíveis para realizar os cálculos corretamente, garantindo que métricas como médias móveis considerem todo o histórico necessário.

Após o processamento, apenas o snapshot correspondente à execução mais recente é armazenado.

Estrutura:

```text
silver/
└── ingestion_date=YYYY-MM-DD/
    └── market_data_features.parquet
```

IMAGE PLACEHOLDER (SILVER TRANSFORM)

---

## Gold Layer

Camada final orientada ao consumo analítico e regras de negócio.

Responsável pela criação de datasets prontos para análise.

Principais produtos:

### Asset Ranking

Ranking de ativos baseado em indicadores como:

* retorno acumulado
* tendência de preço
* volatilidade
* score final

### Market Alerts

Identificação de situações relevantes:

* alta volatilidade
* movimentos anormais
* sinais de mercado

Estrutura:

```text
gold/
└── ingestion_date=YYYY-MM-DD/
    ├── asset_ranking.parquet
    └── market_alerts.parquet
```

---

# Orquestração

O pipeline é executado utilizando Apache Airflow em containers Docker.

A DAG controla todo o fluxo:

```text
ingest_bronze
        |
        v
transform_silver
        |
        v
generate_gold
        |
        v
update_partitions
```

A execução é automatizada e responsável por:

1. Buscar novos dados da Yahoo Finance API
2. Armazenar dados brutos na Bronze
3. Validar qualidade dos dados ingeridos
4. Processar transformações analíticas
5. Gerar datasets Gold
6. Atualizar partições para consumo analítico

IMAGE PLACEHOLDER (AIRFLOW DAG)

---

# Armazenamento

Os dados são armazenados em AWS S3 seguindo uma estrutura de Data Lake particionada.

```text
financial-data-lake/

├── bronze/
│   └── ingestion_date=YYYY-MM-DD/
│       ├── market_data_raw.parquet
│       └── ingestion_report.json
│
├── silver/
│   └── ingestion_date=YYYY-MM-DD/
│       └── market_data_features.parquet
│
└── gold/
    └── ingestion_date=YYYY-MM-DD/
        ├── asset_ranking.parquet
        └── market_alerts.parquet
```
IMAGE PLACEHOLDER (ORGANIZAÇÃO DO BUCKET)

---

# Tecnologias e Decisões Arquiteturais

## DuckDB

Utilizado como engine analítica para processamento dos arquivos Parquet.

Principais vantagens:

* processamento SQL eficiente
* leitura direta de Parquet
* integração com armazenamento em S3
* baixo consumo de recursos
* adequado para workloads analíticos

---

## Parquet

Formato utilizado para armazenamento dos dados.

Benefícios:

* armazenamento colunar
* compressão eficiente
* leitura otimizada para análises
* compatibilidade com ferramentas modernas

---

## AWS S3

Utilizado como armazenamento principal do Data Lake.

Benefícios:

* separação entre processamento e armazenamento
* alta durabilidade
* escalabilidade
* arquitetura cloud-native

---

## Docker

Utilizado para:

* isolamento de ambientes
* gerenciamento de dependências
* execução consistente do Airflow
* reprodutibilidade do pipeline

---

# Consumo Analítico

Os dados da camada Gold são disponibilizados para análise utilizando AWS Athena e Power BI.

Consultas analíticas são realizadas diretamente sobre os arquivos Parquet armazenados no S3.

O dashboard apresenta:

* ranking de ativos
* indicadores de retorno
* volatilidade
* alertas de mercado
* análise individual dos ativos

IMAGE PLACEHOLDER (DASHBOARD)

---

# Estrutura do Projeto

```text
financial-pipeline/

├── dags/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker/
│
├── requirements.txt
│
├── docker-compose.yml
│
└── README.md
```

---

# Próximos Passos

* Implementação de Delta Lake
* Processamento distribuído utilizando Apache Spark
* Monitoramento avançado de qualidade dos dados
* Implementação de CI/CD
* Deploy cloud-native
* Expansão para múltiplas fontes financeiras
* Automação de testes de dados
