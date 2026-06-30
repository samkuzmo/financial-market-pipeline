import logging
from datetime import UTC, datetime

import duckdb


SILVER_PATH = (
    "s3://samuel-financial-data-lake/"
    "silver/**/*.parquet"
)

GOLD_BUCKET = (
    "s3://samuel-financial-data-lake/gold"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


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


def load_silver_data(con):

    logger.info(
        "Loading Silver files..."
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE market_features AS

        SELECT DISTINCT *
        FROM read_parquet(
            '{SILVER_PATH}',
            union_by_name = true
        )
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM market_features
    """).fetchone()[0]

    logger.info(
        f"Loaded {rows} Silver rows"
    )


def create_asset_ranking(con):

    logger.info(
        "Creating asset ranking..."
    )

    con.execute("""
        CREATE OR REPLACE TABLE asset_ranking AS

        WITH latest_data AS (

            SELECT
                *,

                (
                    close /
                    LAG(close, 30)
                    OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date
                    )
                ) - 1 AS return_30d,

                CASE
                    WHEN sma_7 > sma_30
                    THEN 1
                    ELSE 0
                END AS trend_signal

            FROM market_features
        ),

        scored AS (

            SELECT
                *,
                (
                    COALESCE(return_30d, 0)
                    - COALESCE(volatility_7d, 0)
                    + trend_signal
                ) AS score_final

            FROM latest_data
        ),

        latest_snapshot AS (

            SELECT *
            FROM scored

            WHERE trade_date = (
                SELECT MAX(trade_date)
                FROM scored
            )
        )

        SELECT
            *,
            RANK()
            OVER (
                ORDER BY score_final DESC
            ) AS rank_position

        FROM latest_snapshot
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
    """).fetchone()[0]

    logger.info(
        f"Created ranking for {rows} assets"
    )


def create_market_alerts(con):

    logger.info(
        "Creating market alerts..."
    )

    con.execute("""
        CREATE OR REPLACE TABLE market_alerts AS

        SELECT
            symbol,
            trade_date,
            volatility_7d,

            'HIGH_VOLATILITY'
                AS alert_type,

            'Volatilidade acima de 5%'
                AS alert_description,

            CASE
                WHEN volatility_7d > 0.10
                    THEN 'HIGH'

                WHEN volatility_7d > 0.05
                    THEN 'MEDIUM'

                ELSE 'LOW'
            END AS severity

        FROM market_features

        WHERE volatility_7d > 0.05
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
    """).fetchone()[0]

    logger.info(
        f"Created {rows} alerts"
    )


def validate_gold(con):

    ranking_count = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
    """).fetchone()[0]

    alerts_count = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
    """).fetchone()[0]

    logger.info(
        f"Ranking rows: {ranking_count}"
    )

    logger.info(
        f"Alert rows: {alerts_count}"
    )

    top_assets = con.execute("""
        SELECT
            symbol,
            score_final,
            rank_position

        FROM asset_ranking

        ORDER BY rank_position

        LIMIT 5
    """).fetchdf()

    logger.info(
        f"\nTop Assets:\n{top_assets}"
    )


def export_gold_data(con):

    logger.info(
        "Exporting Gold datasets..."
    )

    execution_time = datetime.now(UTC)

    partition = execution_time.strftime(
        "%Y-%m-%d"
    )

    filename = execution_time.strftime(
        "%Y%m%d_%H%M%S"
    )

    ranking_path = (
        f"{GOLD_BUCKET}/asset_ranking/"
        f"ingestion_date={partition}/"
        f"asset_ranking_{filename}.parquet"
    )

    alerts_path = (
        f"{GOLD_BUCKET}/market_alerts/"
        f"ingestion_date={partition}/"
        f"market_alerts_{filename}.parquet"
    )

    con.execute(f"""
        COPY (
            SELECT *
            FROM asset_ranking
        )
        TO '{ranking_path}'
        (
            FORMAT PARQUET
        )
    """)

    con.execute(f"""
        COPY (
            SELECT *
            FROM market_alerts
        )
        TO '{alerts_path}'
        (
            FORMAT PARQUET
        )
    """)

    logger.info(
        f"Asset ranking exported to "
        f"{ranking_path}"
    )

    logger.info(
        f"Market alerts exported to "
        f"{alerts_path}"
    )


def run_gold_transformation():

    logger.info(
        "Starting Gold transformation..."
    )

    con = create_connection()

    load_silver_data(con)

    create_asset_ranking(con)

    create_market_alerts(con)

    validate_gold(con)

    export_gold_data(con)

    logger.info(
        "Gold transformation completed "
        "successfully"
    )


if __name__ == "__main__":
    run_gold_transformation()