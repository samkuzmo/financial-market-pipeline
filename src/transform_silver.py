import logging
from datetime import UTC, datetime

import duckdb

BRONZE_PATH = (
    "s3://samuel-financial-data-lake/"
    "bronze/**/*.parquet"
)

SILVER_PATH = (
    "s3://samuel-financial-data-lake/"
    "silver/market_data_features.parquet"
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


def load_bronze_data(con):

    logger.info(
        "Loading Bronze files..."
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE market_data_raw AS

        SELECT *
        FROM read_parquet(
            '{BRONZE_PATH}',
            union_by_name = true
        )
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM market_data_raw
    """).fetchone()[0]

    logger.info(
        f"Loaded {rows} Bronze rows"
    )

    print(
    con.execute("""
        DESCRIBE market_data_raw
    """).fetchdf()
)


def create_base_returns(con):

    logger.info(
        "Creating base returns..."
    )

    con.execute("""
        CREATE OR REPLACE TABLE base_returns AS

        WITH normalized AS (

            SELECT
                symbol,

                CAST(Date AS DATE) AS trade_date,

                Close AS close,

                Volume AS volume,

                ingestion_timestamp,

                ROW_NUMBER()
                OVER (
                    PARTITION BY
                        symbol,
                        CAST(Date AS DATE)
                    ORDER BY
                        ingestion_timestamp DESC
                ) AS rn

            FROM market_data_raw
        ),

        deduplicated AS (

            SELECT
                symbol,
                trade_date,
                close,
                volume

            FROM normalized

            WHERE rn = 1
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

        FROM deduplicated

        ORDER BY
            symbol,
            trade_date
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM base_returns
    """).fetchone()[0]

    logger.info(
        f"Base returns created with {rows} rows"
    )


def create_market_features(con):

    logger.info(
        "Creating market features..."
    )

    con.execute("""
        CREATE OR REPLACE TABLE market_data_features AS

        SELECT
            symbol,
            trade_date,
            close,
            volume,
            return_1d,

            AVG(close)
            OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
            ) AS sma_7,

            AVG(close)
            OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
            ) AS sma_30,

            STDDEV(return_1d)
            OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
            ) AS volatility_7d

        FROM base_returns

        ORDER BY
            symbol,
            trade_date
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
    """).fetchone()[0]

    logger.info(
        f"Market features created with {rows} rows"
    )


def export_silver_data(con):

    logger.info(
        "Exporting Silver dataset..."
    )

    execution_time = datetime.now(UTC)

    partition = execution_time.strftime(
        "%Y-%m-%d"
    )

    filename = execution_time.strftime(
        "%Y%m%d_%H%M%S"
    )

    output_path = (
        "s3://samuel-financial-data-lake/"
        f"silver/ingestion_date={partition}/"
        f"market_data_features_{filename}.parquet"
    )

    row_count = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
    """).fetchone()[0]

    con.execute(f"""
        COPY (
            SELECT *
            FROM market_data_features
        )
        TO '{output_path}'
        (
            FORMAT PARQUET
        )
    """)

    logger.info(
        f"Exported {row_count} rows "
        f"to {output_path}"
    )

def validate_data(con):

    logger.info(
        "Running validations..."
    )

    summary = con.execute("""
        SELECT
            symbol,
            COUNT(*) AS total_rows,
            MIN(trade_date) AS min_date,
            MAX(trade_date) AS max_date

        FROM market_data_features

        GROUP BY symbol

        ORDER BY symbol
    """).fetchdf()

    duplicates = con.execute("""
        SELECT COUNT(*)
        FROM (

            SELECT
                symbol,
                trade_date,
                COUNT(*) AS total

            FROM market_data_features

            GROUP BY
                symbol,
                trade_date

            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    null_symbols = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
        WHERE symbol IS NULL
    """).fetchone()[0]

    logger.info(
        f"Duplicate rows: {duplicates}"
    )

    logger.info(
        f"Null symbols: {null_symbols}"
    )

    logger.info(
        "\n=== SILVER VALIDATION ===\n"
        f"{summary}"
    )


def run_silver_transformation():

    logger.info(
        "Starting Silver transformation..."
    )

    con = create_connection()

    load_bronze_data(con)

    create_base_returns(con)

    create_market_features(con)

    validate_data(con)

    export_silver_data(con)

    logger.info(
        "Silver transformation completed successfully"
    )


if __name__ == "__main__":
    run_silver_transformation()