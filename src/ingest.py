import json
import logging
from datetime import UTC, datetime

import boto3
import pandas as pd
import yfinance as yf


BUCKET_NAME = (
    "samuel-financial-data-lake"
)


BRAZIL_SYMBOLS = [
    "PETR4.SA",
    "VALE3.SA",
    "ITUB4.SA",
    "WEGE3.SA",
]

US_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
]

SYMBOLS = (
    BRAZIL_SYMBOLS
    + US_SYMBOLS
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def fetch_market_data(
    symbols: list[str],
    period: str = "6mo",
) -> pd.DataFrame:
    """
    Fetch historical market data from Yahoo Finance.

    Parameters
    ----------
    symbols : list[str]
        List of ticker symbols.

    period : str
        Historical period accepted by yfinance.

    Returns
    -------
    pd.DataFrame
        Consolidated market data for all successfully
        retrieved symbols.
    """

    logger.info(
        f"Fetching market data for {len(symbols)} assets..."
    )

    all_data = []

    for symbol in symbols:

        try:

            logger.info(
                f"Fetching data for {symbol}"
            )

            df = yf.download(
                symbol,
                period=period,
                auto_adjust=False,
                progress=False,
            )

            if df.empty:

                logger.warning(
                    f"No data returned for {symbol}"
                )

                continue

            if isinstance(
                df.columns,
                pd.MultiIndex,
            ):

                df.columns = (
                    df.columns.get_level_values(0)
                )

            df.reset_index(
                inplace=True
            )

            df.columns = [
                str(col)
                .strip()
                .lower()
                .replace(" ", "_")
                for col in df.columns
            ]

            df["symbol"] = symbol

            all_data.append(df)

            logger.info(
                f"Successfully fetched "
                f"{len(df)} rows for {symbol}"
            )

        except Exception as e:

            logger.exception(
                f"Error fetching data for "
                f"{symbol}: {e}"
            )

    if not all_data:

        raise ValueError(
            "No market data could be retrieved."
        )

    final_df = pd.concat(
        all_data,
        ignore_index=True,
    )

    final_df["date"] = (
        pd.to_datetime(
            final_df["date"]
        ).dt.date
    )

    logger.info(
        f"Total rows fetched: "
        f"{len(final_df)}"
    )

    return final_df


def create_validation_report(
    df: pd.DataFrame | None,
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
            "layer": "bronze",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "pipeline_status": "FAILED",
            "status": "FAILED",
            "current_step": current_step,
        }

    expected_columns = [
        "date",
        "close",
        "volume",
        "symbol",
        "ingestion_timestamp",
    ]

    actual_columns = list(
        df.columns
    )

    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    if missing_columns:

        return {
            "pipeline": "financial-market-pipeline",
            "layer": "bronze",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "pipeline_status": pipeline_status,
            "status": "FAILED",
            "current_step": current_step,
            "missing_columns": missing_columns,
        }

    expected_assets = len(
        SYMBOLS
    )

    processed_symbols = set(
        df["symbol"]
        .dropna()
        .unique()
    )

    expected_symbols = set(
        SYMBOLS
    )

    missing_assets = sorted(
        expected_symbols
        - processed_symbols
    )

    assets_processed = int(
        df["symbol"].nunique()
    )

    null_symbol = int(
        df["symbol"].isna().sum()
    )

    null_trade_date = int(
        df["date"].isna().sum()
    )

    null_close = int(
        df["close"].isna().sum()
    )

    null_volume = int(
        df["volume"].isna().sum()
    )

    duplicated_trade_records = int(
        df.duplicated(
            subset=[
                "symbol",
                "date",
            ]
        ).sum()
    )

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
        )
        else "WARNING"
    )

    report = {
        "pipeline": "financial-market-pipeline",
        "layer": "bronze",

        "execution_timestamp": (
            execution_time.isoformat()
        ),

        "current_step": current_step,

        "pipeline_status": pipeline_status,

        "status": status,

        "schema": {
            "columns": actual_columns,

            "dtypes": {
                column: str(dtype)
                for column, dtype
                in df.dtypes.items()
            },
        },

        "metrics": {
            "rows_ingested": int(
                len(df)
            ),

            "expected_assets": (
                expected_assets
            ),

            "assets_processed": (
                assets_processed
            ),

            "missing_assets": (
                missing_assets
            ),

            "null_symbol": (
                null_symbol
            ),

            "null_trade_date": (
                null_trade_date
            ),

            "null_close": (
                null_close
            ),

            "null_volume": (
                null_volume
            ),

            "duplicated_trade_records": (
                duplicated_trade_records
            ),

            "ingestion_duration_seconds": (
                end_time - start_time
            ).total_seconds(),

            "missing_columns": (
                missing_columns
            ),
        },

        "date_range": {
            "min_trade_date": (
                str(df["date"].min())
            ),

            "max_trade_date": (
                str(df["date"].max())
            ),
        },
    }

    logger.info(
        "Validation Report created"
    )

    return report


def save_raw_data(
    df: pd.DataFrame,
    bucket_name: str,
    execution_time: datetime,
) -> None:

    logger.info(
        "Exporting Bronze dataset..."
    )

    partition = execution_time.strftime(
        "%Y-%m-%d"
    )

    filename = execution_time.strftime(
        "%Y%m%d_%H%M%S"
    )

    output_path = (
        f"s3://{bucket_name}/"
        f"bronze/data/"
        f"ingestion_date={partition}/"
        f"market_data_raw_{filename}.parquet"
    )

    logger.info(
        f"Saving Bronze data to "
        f"{output_path}"
    )

    df.to_parquet(
        output_path,
        storage_options={
            "anon": False
        },
        index=False,
    )

    logger.info(
        f"Successfully saved "
        f"{len(df)} rows"
    )


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
        "bronze/validation/"
        f"ingestion_date={partition}/"
        f"ingestion_report_{filename}.json"
    )

    logger.info(
        f"Saving validation report to "
        f"s3://{bucket_name}/{key}"
    )

    s3 = boto3.client(
        "s3"
    )

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(
            report,
            indent=4,
        ),
        ContentType=(
            "application/json"
        ),
    )

    logger.info(
        "Validation Report saved successfully"
    )


def run_bronze_ingestion():

    logger.info(
        "Starting Bronze ingestion..."
    )

    execution_time = datetime.now(
        UTC
    )

    start_time = execution_time

    pipeline_status = "FAILED"

    current_step = (
        "Market data fetching"
    )

    df = None

    try:

        df = fetch_market_data(
            SYMBOLS
        )

        df["ingestion_timestamp"] = (
            execution_time
        )

        current_step = (
            "Bronze data exportation"
        )

        save_raw_data(
            df,
            BUCKET_NAME,
            execution_time,
        )

        pipeline_status = "SUCCESS"

        logger.info(
            "Bronze ingestion completed "
            "successfully"
        )

    except Exception as e:

        pipeline_status = "FAILED"

        logger.exception(
            f"Bronze ingestion failed: {e}"
        )

    finally:

        end_time = datetime.now(
            UTC
        )

        report = create_validation_report(
            df,
            execution_time,
            start_time,
            end_time,
            current_step,
            pipeline_status,
        )

        try:

            save_validation_report(
                report,
                BUCKET_NAME,
                execution_time,
            )

        except Exception as e:

            logger.exception(
                f"Failed to save validation "
                f"report: {e}"
            )

    if pipeline_status == "FAILED":

        raise RuntimeError(
            "Bronze ingestion failed."
        )


if __name__ == "__main__":
    run_bronze_ingestion()
