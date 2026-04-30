# Financial Market Pipeline

Pipeline de dados para ingestão, transformação e análise de dados financeiros utilizando arquitetura moderna de Data Lake.

## Stack do Projeto

- Python
- DuckDB
- Parquet
- Docker
- Apache Airflow
- AWS S3
- yFinance API

## Arquitetura

O pipeline segue a arquitetura de Data Lake em camadas:

```text
Yahoo Finance API
        ↓
Airflow DAGs
        ↓
DuckDB Transformations
        ↓
AWS S3 Data Lake
(Bronze / Silver / Gold)
```

---

# Camadas do Data Lake

## Bronze

Ingestão de dados financeiros via Yahoo Finance API utilizando `yfinance`.

Características:
- dados crus
- persistência em Parquet
- armazenamento no AWS S3
- adição de timestamp de ingestão

Exemplo:
- OHLCV
- símbolo do ativo
- data da ingestão

---

## Silver

Transformações analíticas utilizando DuckDB.

Principais transformações:
- cálculo de retornos diários
- médias móveis
- volatilidade
- padronização de schema
- tratamento analítico dos dados

Tecnologias:
- DuckDB SQL Engine
- Parquet
- S3 Object Storage

---

## Gold

Camada analítica orientada a negócio.

Métricas e agregações:
- ranking de ativos
- indicadores de volatilidade
- métricas consolidadas
- datasets prontos para consumo analítico

---

# Orquestração

O pipeline é orquestrado utilizando Apache Airflow executando em containers Docker.

A DAG executa automaticamente:

1. Ingestão da camada Bronze
2. Transformação da camada Silver
3. Geração da camada Gold

---

# Armazenamento

Os dados são armazenados em formato Parquet no AWS S3 seguindo estrutura de Data Lake:

```text
s3://bucket-name/
│
├── bronze/
├── silver/
└── gold/
```

---

# Tecnologias e Decisões Arquiteturais

## DuckDB

Utilizado como engine analítica SQL para processamento local de arquivos Parquet diretamente no S3.

Vantagens:
- alta performance analítica
- baixo consumo de recursos
- integração nativa com Parquet
- leitura direta de dados no S3
- excelente para pipelines analíticos pequenos e médios

---

## Parquet

Formato colunar utilizado para:
- compressão eficiente
- leitura analítica rápida
- integração com engines modernas de dados

---

## Docker

Utilizado para:
- padronização do ambiente
- isolamento de dependências
- execução consistente do Airflow
- reprodutibilidade do pipeline

---

## AWS S3

Utilizado como camada de armazenamento do Data Lake.

Vantagens:
- desacoplamento entre compute e storage
- armazenamento escalável
- persistência cloud-native
- arquitetura moderna de dados

---

# Estrutura do Projeto

```text
financial-pipeline/
│
├── dags/
├── src/
├── data/
├── docker/
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

# Próximos Passos

- Particionamento de datasets no S3
- Integração com AWS Athena
- Implementação de Delta Lake
- Processamento distribuído com Spark
- Dashboard analítico
- Monitoramento e observabilidade
- Deploy cloud-native
- Suporte para ingestão e processamento de múltiplos ativos financeiros