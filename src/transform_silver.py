import logging
from datetime import UTC, datetime
import json
import boto3

import duckdb

BUCKET_NAME = (
    "samuel-financial-data-lake"
) 

BRONZE_PATH = (
    f"s3://{BUCKET_NAME}/"
    f"bronze/data/**/*.parquet"
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


def deduplicate_bronze_data(con):

    logger.info(
        "Deduplicating Bronze data..."
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE deduplicated_market_data AS

        WITH normalized AS (

            SELECT
                symbol,
                CAST(date AS DATE) AS trade_date,
                close,
                volume,
                ingestion_timestamp

            FROM market_data_raw
        
        ),

        ranked AS (

            SELECT
                symbol,
                trade_date,
                close,
                volume,
                ingestion_timestamp,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        symbol,
                        trade_date

                    ORDER BY

                        CASE
                            WHEN close IS NOT NULL
                             AND volume IS NOT NULL
                            THEN 1
                            ELSE 0
                        END DESC,

                        ingestion_timestamp DESC
                ) AS rn

            FROM normalized
        )

        SELECT
            symbol,
            trade_date,
            close,
            volume,
            ingestion_timestamp

        FROM ranked

        WHERE rn = 1
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM deduplicated_market_data
    """).fetchone()[0]

    logger.info(
        f"Deduplicated Bronze data contains "
        f"{rows} rows"
    )


def create_market_features(con):

    logger.info(
        "Creating market features..."
    )

    con.execute("""
        CREATE OR REPLACE TABLE market_data_features AS

        WITH base_returns AS (

            SELECT
                symbol,
                trade_date,
                close,
                volume,

                (
                    close
                    / LAG(close)
                    OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date
                    )
                ) - 1 AS return_1d

            FROM deduplicated_market_data
        )

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
        f"s3://{BUCKET_NAME}/"
        f"silver/data/ingestion_date={partition}/"
        f"market_data_features_{filename}.parquet"
    )

    row_count = con.execute("""
        WITH latest_market_snapshot AS (

            SELECT
                symbol,
                trade_date,
                close,
                volume,
                return_1d,
                sma_7,
                sma_30,
                volatility_7d

            FROM (
                SELECT
                    symbol,
                    trade_date,
                    close,
                    volume,
                    return_1d,
                    sma_7,
                    sma_30,
                    volatility_7d,

                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date DESC
                    ) AS row_num

                FROM market_data_features
            )

            WHERE row_num = 1
        )

        SELECT COUNT(*)
        FROM latest_market_snapshot
    """).fetchone()[0]

    con.execute(f"""
        COPY (
            WITH latest_market_snapshot AS (

                SELECT
                    *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol
                            ORDER BY trade_date DESC
                        ) AS row_num

                    FROM market_data_features
                )
                WHERE row_num = 1
            )

            SELECT
                symbol,
                trade_date,
                close,
                volume,
                return_1d,
                sma_7,
                sma_30,
                volatility_7d

            FROM latest_market_snapshot
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

def create_validation_report(
        con,
        execution_time: datetime,
        start_time: datetime,
        end_time: datetime,
        current_step: str,
        pipeline_status: str,
) -> dict:

    logger.info(
        "Creating Validation Report"
    )

    if pipeline_status == "FAILED":
        return {
            "pipeline": "financial-market-pipeline",
            "layer": "silver",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "pipeline_status": "FAILED",
            "status": "FAILED",
            "current_step": current_step,
        }

    expected_columns = [
        "symbol",
        "trade_date",
        "close",
        "volume",
        "return_1d",
        "sma_7",
        "sma_30",
        "volatility_7d",
    ]

    actual_columns = [
        row[0]
        for row in con.execute("""
            DESCRIBE market_data_features
        """).fetchall()
    ]

    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    if missing_columns:
        return {
            "pipeline": "financial-market-pipeline",
            "layer": "silver",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "pipeline_status": pipeline_status,
            "status": "FAILED",
            "current_step": current_step,
            "missing_columns": missing_columns,
        }

    rows_processed = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
    """).fetchone()[0]

    expected_assets = con.execute("""
        SELECT COUNT(DISTINCT symbol)
        FROM market_data_raw
        """).fetchone()[0]

    expected_symbols = [
    row[0]
    for row in con.execute("""
        SELECT DISTINCT symbol
        FROM market_data_raw
    """).fetchall()
]

    assets_processed = con.execute("""
        SELECT COUNT(DISTINCT symbol)
        FROM market_data_features
    """).fetchone()[0]

    processed_assets = [
        row[0]
        for row in con.execute("""
            SELECT DISTINCT symbol
            FROM market_data_features
        """).fetchall()
    ]

    missing_assets = sorted(
        set(expected_symbols)
        - set(processed_assets)
    )

    null_symbol = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
        WHERE symbol IS NULL
    """).fetchone()[0]

    null_trade_date = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
        WHERE trade_date IS NULL
    """).fetchone()[0]

    null_close = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
        WHERE close IS NULL
    """).fetchone()[0]

    null_volume = con.execute("""
        SELECT COUNT(*)
        FROM market_data_features
        WHERE volume IS NULL
    """).fetchone()[0]

    duplicated_trade_records = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                symbol,
                trade_date
            FROM market_data_features
            GROUP BY
                symbol,
                trade_date
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    snapshot_rows = con.execute("""
        WITH latest_market_snapshot AS (

            SELECT
                symbol,
                trade_date,

                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date DESC
                ) AS row_num

            FROM market_data_features
        )

        SELECT COUNT(*)
        FROM latest_market_snapshot

        WHERE row_num = 1
    """).fetchone()[0]

    min_trade_date, max_trade_date = con.execute("""
        SELECT
            MIN(trade_date),
            MAX(trade_date)
        FROM market_data_features
    """).fetchone()

    if pipeline_status == "FAILED":
        status = "FAILED"
    else:
        status = (
            "SUCCESS"
            if (
                len(missing_columns) == 0
                and len(missing_assets) == 0
                and null_symbol == 0
                and null_trade_date == 0
                and null_close == 0
                and null_volume == 0
                and duplicated_trade_records == 0
                and snapshot_rows == assets_processed
            )
            else "WARNING"
        )

    report = {
        "pipeline": "financial-market-pipeline",
        "layer": "silver",

        "execution_timestamp": (
            execution_time.isoformat()
        ),

        "current_step": current_step,

        "pipeline_status":  pipeline_status,

        "status": status,

        "schema": {
            "columns": actual_columns,
        },

        "metrics": {
            "rows_processed": int(
                rows_processed
            ),
            "expected_assets": (
                expected_assets
            ),
            "assets_processed": int(
                assets_processed
            ),
            "missing_assets": (
                missing_assets
            ),
            "snapshot_rows": int(
                snapshot_rows
            ),
            "null_symbol": int(
                null_symbol
            ),
            "null_trade_date": int(
                null_trade_date
            ),
            "null_close": int(
                null_close
            ),
            "null_volume": int(
                null_volume
            ),
            "duplicated_trade_records": int(
                duplicated_trade_records
            ),
            "ingestion_duration_seconds": (
                end_time - start_time
            ).total_seconds()
        },

        "date_range": {
            "min_trade_date": (
                str(min_trade_date)
            ),
            "max_trade_date": (
                str(max_trade_date)
            ),
        },
    }

    logger.info(
        "Validation Report created"
    )

    return report

def save_validation_report(
    report: dict,
    bucket_name: str,
    execution_time: datetime,
) -> None:

    partition = execution_time.strftime(
        "%Y-%m-%d"
    )

    filename = execution_time.strftime(
        "%Y%m%d_%H%M%S"
    )

    key = (
        "silver/validation/"
        f"ingestion_date={partition}/"
        f"ingestion_report_{filename}.json"
    )

    logger.info(
        f"Saving validation report to s3://{bucket_name}/{key}"
    )

    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(
            report,
            indent=4
        ),
        ContentType="application/json",
    )

    logger.info(
        "Validation Report saved successfully"
    )

def run_silver_transformation():

    current_step = "DuckDB connection creation"
    pipeline_status = "FAILED"
    execution_time = datetime.now(UTC)
    start_time = execution_time
    con = None

    try:

        logger.info(
            "Starting Silver transformation..."
        )

        con = create_connection()

        current_step = "Bronze data loading"
        load_bronze_data(con)

        current_step = "Bronze data deduplication"
        deduplicate_bronze_data(con)

        current_step = "Market_features creation"
        create_market_features(con)

        current_step = "Silver data exportation"
        export_silver_data(con)

        pipeline_status = "SUCCESS"

        logger.info(
            "Silver transformation completed successfully"
        )

    except Exception as e:

        pipeline_status = "FAILED"

        logger.exception(
            f"Silver transformation failed: {e}"
        )

    finally:
        if con is not None:
            end_time = datetime.now(UTC)

            try:
                report = create_validation_report(
                    con, 
                    execution_time, 
                    start_time, 
                    end_time, 
                    current_step,
                    pipeline_status
                )
                
                save_validation_report(
                    report,
                    BUCKET_NAME,
                    execution_time
                    )

            finally:
                con.close()



if __name__ == "__main__":
    run_silver_transformation()