import duckdb


SILVER_PATH = "s3://samuel-financial-data-lake/silver/market_data_features.parquet"
GOLD_RANKING_PATH = "s3://samuel-financial-data-lake/gold/asset_ranking.parquet"
GOLD_ALERTS_PATH = "s3://samuel-financial-data-lake/gold/market_alerts.parquet"

def create_connection():
    
    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE S3,
            PROVIDER credential_chain,
            CHAIN 'instance'
        );
    """)

    return con

def load_silver_data(con):

    con.execute(f"""
        CREATE OR REPLACE TABLE market_features AS

        SELECT *
        FROM read_parquet('{SILVER_PATH}')
    """)


def create_asset_ranking(con):

    con.execute("""
        CREATE OR REPLACE TABLE asset_ranking AS

        WITH latest_data AS (

            SELECT
                *,

                -----------------------------------
                -- Retorno acumulado 30 dias
                -----------------------------------
                (
                    close /
                    LAG(close, 30)
                    OVER (
                        PARTITION BY symbol
                        ORDER BY date
                    )
                ) - 1 AS return_30d,

                -----------------------------------
                -- Sinal de tendência
                -----------------------------------
                CASE
                    WHEN sma_7 > sma_30 THEN 1
                    ELSE 0
                END AS trend_signal

            FROM market_features
        ),

        scored AS (

            SELECT
                symbol,
                date,

                close,

                return_1d,
                return_30d,

                volatility_7d,

                sma_7,
                sma_30,

                trend_signal,

                -----------------------------------
                -- Score final
                -----------------------------------
                (
                    COALESCE(return_30d, 0)
                    - COALESCE(volatility_7d, 0)
                    + trend_signal
                ) AS score_final

            FROM latest_data
        )

        SELECT
            *,

            RANK()
            OVER (
                ORDER BY score_final DESC
            ) AS rank_position

        FROM scored
    """)


def create_market_alerts(con):

    con.execute("""
        CREATE OR REPLACE TABLE market_alerts AS

        SELECT
            symbol,

            date,

            volatility_7d,

            'HIGH_VOLATILITY' AS alert_type,

            'Volatilidade acima de 5%' AS alert_description,

            CASE
                WHEN volatility_7d > 0.10 THEN 'HIGH'
                WHEN volatility_7d > 0.05 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS severity

        FROM market_features

        WHERE volatility_7d > 0.05
    """)


def export_gold_data(con):

    con.execute(f"""
        COPY asset_ranking
        TO '{GOLD_RANKING_PATH}'
        (FORMAT PARQUET)
    """)

    con.execute(f"""
        COPY market_alerts
        TO '{GOLD_ALERTS_PATH}'
        (FORMAT PARQUET)
    """)


def validate_gold(con):

    ranking = con.execute("""
        SELECT *
        FROM asset_ranking
        ORDER BY rank_position
        LIMIT 10
    """).fetchdf()

    alerts = con.execute("""
        SELECT *
        FROM market_alerts
        LIMIT 10
    """).fetchdf()

    print("\n=== TOP RANKING ===")
    print(ranking)

    print("\n=== ALERTAS ===")
    print(alerts)


def run_gold_transformation():

    con = create_connection()

    print("Carregando dados Silver...")
    load_silver_data(con)

    print("Criando ranking de ativos...")
    create_asset_ranking(con)

    print("Criando alertas de mercado...")
    create_market_alerts(con)

    print("Exportando datasets Gold...")
    export_gold_data(con)

    print("Validando resultados...")
    validate_gold(con)

    print("\nTransformação Gold concluída com sucesso")


if __name__ == "__main__":
    run_gold_transformation()