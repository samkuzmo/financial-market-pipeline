# Financial Market Pipeline

Pipeline de dados para ingestão, transformação e análise de dados financeiros utilizando:

- Python
- DuckDB
- Parquet
- Docker
- Apache Airflow (futuro)
- AWS S3 (futuro)
- Apache Spark (futuro)
- Docker (futuro)

## Arquitetura

### Bronze
Ingestão de dados financeiros via Yahoo Finance API (yfinance).

### Silver
Transformações analíticas:
- retornos
- médias móveis
- volatilidade

### Gold
Camada de negócio:
- ranking de ativos
- alertas de volatilidade

## Próximos passos

- Integração com S3
- Orquestração com Airflow + Docker
- Transformações distribuídas com Spark
- Dashboard analítico