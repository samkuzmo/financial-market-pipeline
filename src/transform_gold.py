import logging
from datetime import UTC, datetime
import json
import boto3

import duckdb

BUCKET_NAME = "samuel-financial-data-lake"

SILVER_PATH = (
    f"s3://{BUCKET_NAME}/"
    f"silver/data/**/*.parquet"
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

        SELECT
            symbol,
            trade_date,
            close,
            volume,
            return_1d,
            return_30d,
            sma_7,
            sma_30,
            volatility_7d
        
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

        WITH calculated_features AS (

            SELECT
                *,

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

            FROM calculated_features
        )

        SELECT
            *,

            ROW_NUMBER()
            OVER (
                ORDER BY
                    score_final DESC,
                    return_30d DESC,
                    volatility_7d ASC,
                    symbol ASC
            ) AS rank_position

        FROM scored
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

        -- High volatility alerts
        SELECT
            symbol,
            trade_date,
            volatility_7d,
            return_30d,

            'HIGH_VOLATILITY'
                AS alert_type,

            CONCAT(
                'Volatilidade de 7 dias em ',
                ROUND(volatility_7d * 100, 2),
                '%, com retorno de 30 dias de ',
                ROUND(return_30d * 100, 2),
                '%'
            ) AS alert_description,

            CASE
                WHEN volatility_7d > 0.10
                    THEN 'HIGH'

                WHEN volatility_7d > 0.07
                    THEN 'MEDIUM'

                ELSE 'LOW'
            END AS severity

        FROM market_features

        WHERE volatility_7d > 0.03


        UNION ALL


        -- Strong negative return alerts
        SELECT
            symbol,
            trade_date,
            volatility_7d,
            return_30d,

            'STRONG_NEGATIVE_RETURN'
                AS alert_type,

            CONCAT(
                'Retorno de 30 dias de ',
                ROUND(return_30d * 100, 2),
                '%, indicando queda relevante'
            ) AS alert_description,

            CASE
                WHEN return_30d <= -0.20
                    THEN 'HIGH'

                ELSE 'MEDIUM'
            END AS severity

        FROM market_features

        WHERE return_30d < -0.10


        UNION ALL


        -- Strong positive return alerts
        SELECT
            symbol,
            trade_date,
            volatility_7d,
            return_30d,

            'STRONG_POSITIVE_RETURN'
                AS alert_type,

            CONCAT(
                'Retorno de 30 dias de ',
                ROUND(return_30d * 100, 2),
                '%, indicando alta relevante'
            ) AS alert_description,

            CASE
                WHEN return_30d >= 0.20
                    THEN 'HIGH'

                ELSE 'MEDIUM'
            END AS severity

        FROM market_features

        WHERE return_30d > 0.10
    """)

    rows = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
    """).fetchone()[0]

    logger.info(
        f"Created {rows} market alerts"
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
            "layer": "gold",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "pipeline_status": "FAILED",
            "status": "FAILED",
            "current_step": current_step,
        }

    expected_ranking_columns = [
        "symbol",
        "trade_date",
        "close",
        "volume",
        "return_1d",
        "return_30d",
        "sma_7",
        "sma_30",
        "volatility_7d",
        "trend_signal",
        "score_final",
        "rank_position",
    ]

    expected_alert_columns = [
        "symbol",
        "trade_date",
        "volatility_7d",
        "return_30d",
        "alert_type",
        "alert_description",
        "severity",
    ]

    actual_ranking_columns = [
        row[0]
        for row in con.execute("""
            DESCRIBE asset_ranking
        """).fetchall()
    ]

    actual_alert_columns = [
        row[0]
        for row in con.execute("""
            DESCRIBE market_alerts
        """).fetchall()
    ]

    missing_ranking_columns = [
        column
        for column in expected_ranking_columns
        if column not in actual_ranking_columns
    ]

    missing_alert_columns = [
        column
        for column in expected_alert_columns
        if column not in actual_alert_columns
    ]

    if (
        missing_ranking_columns
        or missing_alert_columns
    ):
        return {
            "pipeline": "financial-market-pipeline",
            "layer": "gold",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "pipeline_status": pipeline_status,
            "status": "FAILED",
            "current_step": current_step,
            "missing_columns": {
                "asset_ranking": (
                    missing_ranking_columns
                ),
                "market_alerts": (
                    missing_alert_columns
                ),
            },
        }

    ranking_rows = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
    """).fetchone()[0]

    alert_rows = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
    """).fetchone()[0]

    ranking_assets = con.execute("""
        SELECT COUNT(DISTINCT symbol)
        FROM asset_ranking
    """).fetchone()[0]

    alert_symbols = con.execute("""
        SELECT DISTINCT symbol
        FROM market_alerts
    """).fetchall()

    alert_symbols = {
        row[0]
        for row in alert_symbols
    }

    null_ranking_symbol = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
        WHERE symbol IS NULL
    """).fetchone()[0]

    null_ranking_trade_date = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
        WHERE trade_date IS NULL
    """).fetchone()[0]

    null_ranking_score = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
        WHERE score_final IS NULL
    """).fetchone()[0]

    null_ranking_position = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
        WHERE rank_position IS NULL
    """).fetchone()[0]

    duplicated_ranks = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                rank_position
            FROM asset_ranking
            GROUP BY rank_position
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    invalid_rank_positions = con.execute("""
        SELECT COUNT(*)
        FROM asset_ranking
        WHERE rank_position < 1
    """).fetchone()[0]

    null_alert_symbol = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
        WHERE symbol IS NULL
    """).fetchone()[0]

    null_alert_trade_date = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
        WHERE trade_date IS NULL
    """).fetchone()[0]

    null_alert_type = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
        WHERE alert_type IS NULL
    """).fetchone()[0]

    null_alert_severity = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
        WHERE severity IS NULL
    """).fetchone()[0]

    invalid_alert_types = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
        WHERE alert_type NOT IN (
            'HIGH_VOLATILITY',
            'STRONG_NEGATIVE_RETURN',
            'STRONG_POSITIVE_RETURN'
        )
    """).fetchone()[0]

    invalid_severity = con.execute("""
        SELECT COUNT(*)
        FROM market_alerts
        WHERE severity NOT IN (
            'HIGH',
            'MEDIUM',
            'LOW'
        )
    """).fetchone()[0]

    duplicated_alerts = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                symbol,
                trade_date,
                alert_type
            FROM market_alerts
            GROUP BY
                symbol,
                trade_date,
                alert_type
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    min_trade_date, max_trade_date = con.execute("""
        SELECT
            MIN(trade_date),
            MAX(trade_date)
        FROM asset_ranking
    """).fetchone()

    validation_failed = (
        len(missing_ranking_columns) > 0
        or len(missing_alert_columns) > 0
        or null_ranking_symbol > 0
        or null_ranking_trade_date > 0
        or null_ranking_score > 0
        or null_ranking_position > 0
        or duplicated_ranks > 0
        or invalid_rank_positions > 0
        or null_alert_symbol > 0
        or null_alert_trade_date > 0
        or null_alert_type > 0
        or null_alert_severity > 0
        or invalid_alert_types > 0
        or invalid_severity > 0
        or duplicated_alerts > 0
    )

    status = (
        "WARNING"
        if validation_failed
        else "SUCCESS"
    )

    report = {
        "pipeline": "financial-market-pipeline",
        "layer": "gold",

        "execution_timestamp": (
            execution_time.isoformat()
        ),

        "current_step": current_step,

        "pipeline_status": pipeline_status,

        "status": status,

        "schema": {
            "asset_ranking": {
                "columns": actual_ranking_columns,
            },
            "market_alerts": {
                "columns": actual_alert_columns,
            },
        },

        "metrics": {
            "ranking_rows": int(
                ranking_rows
            ),
            "ranking_assets": int(
                ranking_assets
            ),
            "alert_rows": int(
                alert_rows
            ),
            "assets_with_alert": int(
                len(alert_symbols)
            ),
            "null_ranking_symbol": int(
                null_ranking_symbol
            ),
            "null_ranking_trade_date": int(
                null_ranking_trade_date
            ),
            "null_ranking_score": int(
                null_ranking_score
            ),
            "null_ranking_position": int(
                null_ranking_position
            ),
            "duplicated_ranks": int(
                duplicated_ranks
            ),
            "invalid_rank_positions": int(
                invalid_rank_positions
            ),
            "null_alert_symbol": int(
                null_alert_symbol
            ),
            "null_alert_trade_date": int(
                null_alert_trade_date
            ),
            "null_alert_type": int(
                null_alert_type
            ),
            "null_alert_severity": int(
                null_alert_severity
            ),
            "invalid_alert_types": int(
                invalid_alert_types
            ),
            "invalid_severity": int(
                invalid_severity
            ),
            "duplicated_alerts": int(
                duplicated_alerts
            ),
            "ingestion_duration_seconds": (
                end_time - start_time
            ).total_seconds(),
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
        "gold/validation/"
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
        f"s3://{BUCKET_NAME}/gold/data/asset_ranking/"
        f"ingestion_date={partition}/"
        f"asset_ranking_{filename}.parquet"
    )

    alerts_path = (
        f"s3://{BUCKET_NAME}/gold/data/market_alerts/"
        f"ingestion_date={partition}/"
        f"market_alerts_{filename}.parquet"
    )

    con.execute(f"""
        COPY (
            SELECT 
                symbol,
                trade_date,
                close,
                volume,
                return_1d,
                sma_7,
                sma_30,
                volatility_7d,
                return_30d,
                trend_signal,
                score_final,
                rank_position
            FROM asset_ranking
        )
        TO '{ranking_path}'
        (
            FORMAT PARQUET
        )
    """)

    con.execute(f"""
        COPY (
            SELECT 
                symbol,
                trade_date,
                volatility_7d,
                return_30d,
                alert_type,
                alert_description,
                severity
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

    execution_time = datetime.now(UTC)
    start_time = execution_time
    pipeline_status = "FAILED"
    con = None

    current_step = "DuckDB connection creation"

    try:

        con = create_connection()

        current_step = "Silver data loading"
        load_silver_data(con)

        current_step = "Asset Ranking creation"
        create_asset_ranking(con)

        current_step = "Market Alerts creation"
        create_market_alerts(con)

        current_step = "Gold data exportation"
        export_gold_data(con)

        pipeline_status = "SUCCESS"

        logger.info(
            f"Gold transformation completed "
            f"successfully"
        )

    except Exception as e: 

        pipeline_status = "FAILED"

        logger.exception(
            f"Gold transformation failed: {e}"
        )

    finally:
        if con is not None:
            try:

                end_time = datetime.now(UTC)

                report = create_validation_report(
                    con,
                    execution_time,
                    start_time,
                    end_time,
                    current_step,
                    pipeline_status,
                )

                save_validation_report(
                    report,
                    BUCKET_NAME,
                    execution_time,
                )

            finally:
                con.close()




if __name__ == "__main__":
    run_gold_transformation()