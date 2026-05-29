import duckdb

BRONZE_PATH = "s3://samuel-financial-data-lake/bronze/market_data_raw.parquet"
SILVER_PATH = "s3://samuel-financial-data-lake/silver/market_data_features.parquet"


def create_connection():

    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute("""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE S3,
            PROVIDER credential_chain
        );
    """)

    return con


def load_bronze_data(con):

    con.execute(f"""
        CREATE OR REPLACE TABLE market_data_raw AS

        SELECT *
        FROM read_parquet('{BRONZE_PATH}')
    """)


def create_base_returns(con):

    con.execute("""
        CREATE OR REPLACE TABLE base_returns AS

        WITH normalized AS (

            SELECT
                symbol,

                CAST(Date AS DATE) AS trade_date,

                Close AS close,

                Volume AS volume

            FROM market_data_raw
        )

        SELECT
            symbol,
            trade_date,
            close,
            volume,

            (
                close / LAG(close)
                OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                )
            ) - 1 AS return_1d

        FROM normalized
    """)


def create_market_features(con):

    con.execute("""
        CREATE OR REPLACE TABLE market_data_features AS

        SELECT
            symbol,
            trade_date,
            close,
            volume,
            return_1d,

            -----------------------------------
            -- Média móvel 7 dias
            -----------------------------------
            AVG(close)
            OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS sma_7,

            -----------------------------------
            -- Média móvel 30 dias
            -----------------------------------
            AVG(close)
            OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS sma_30,

            -----------------------------------
            -- Volatilidade rolling 7 dias
            -----------------------------------
            STDDEV(return_1d)
            OVER (
                PARTITION BY symbol
                ORDER BY trade_date
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
            MIN(trade_date) AS min_date,
            MAX(trade_date) AS max_date

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