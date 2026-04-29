import duckdb


BRONZE_PATH = "/opt/airflow/data/market_data_raw.parquet"
SILVER_PATH = "/opt/airflow/data/market_data_features.parquet"


def create_connection():
    return duckdb.connect()


def load_bronze_data(con):
    con.execute(f"""
        CREATE OR REPLACE TABLE market_data_raw AS
        SELECT *
        FROM read_parquet('{BRONZE_PATH}')
    """)


def create_base_returns(con):
    con.execute("""
        CREATE OR REPLACE TABLE base_returns AS

        SELECT
            symbol,

            CAST(Date AS DATE) AS date,

            Close AS close,

            Volume AS volume,

            (
                Close / LAG(Close)
                OVER (
                    PARTITION BY symbol
                    ORDER BY Date
                )
            ) - 1 AS return_1d

        FROM market_data_raw
    """)


def create_market_features(con):
    con.execute("""
        CREATE OR REPLACE TABLE market_data_features AS

        SELECT
            symbol,
            date,
            close,
            volume,
            return_1d,

            -----------------------------------
            -- Média móvel 7 dias
            -----------------------------------
            AVG(close)
            OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS sma_7,

            -----------------------------------
            -- Média móvel 30 dias
            -----------------------------------
            AVG(close)
            OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS sma_30,

            -----------------------------------
            -- Volatilidade rolling 7 dias
            -----------------------------------
            STDDEV(return_1d)
            OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS volatility_7d

        FROM base_returns
    """)


def export_silver_data(con):
    con.execute(f"""
        COPY market_data_features
        TO '{SILVER_PATH}'
        (FORMAT PARQUET)
    """)


def validate_data(con):
    result = con.execute("""
        SELECT
            symbol,
            COUNT(*) AS total_rows,
            MIN(date) AS min_date,
            MAX(date) AS max_date
        FROM market_data_features
        GROUP BY symbol
    """).fetchdf()

    print("\n=== VALIDAÇÃO SILVER ===")
    print(result)


def run_silver_transformation():
    con = create_connection()

    print("Carregando Bronze...")
    load_bronze_data(con)

    print("Criando retornos base...")
    create_base_returns(con)

    print("Criando features analíticas...")
    create_market_features(con)

    print("Exportando parquet Silver...")
    export_silver_data(con)

    print("Validando dados...")
    validate_data(con)

    print("\nTransformação Silver concluída com sucesso")


if __name__ == "__main__":
    run_silver_transformation()