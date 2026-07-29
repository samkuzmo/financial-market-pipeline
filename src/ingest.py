import logging
from datetime import UTC, datetime

import pandas as pd
import yfinance as yf
import json
import boto3

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

SYMBOLS = BRAZIL_SYMBOLS + US_SYMBOLS


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
    """

    all_data = []

    for symbol in symbols:
        try:
            logger.info(f"Fetching data for {symbol}")

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

            df.reset_index(inplace=True)

            df.columns = [
                str(col).strip().lower()
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

    logger.info(
        f"Total rows fetched: "
        f"{len(final_df)}"
    )

    return final_df

def create_validation_report(
        df: pd.DataFrame,
        execution_time: datetime,
        start_time: datetime,
        end_time: datetime,
) -> dict:
    logger.info("Creating Validation Report")

    expected_columns = [
    "date",
    "close",
    "volume",
    "symbol",
    "ingestion_timestamp",
    ]

    missing_columns = [
    col
    for col in expected_columns
    if col not in df.columns
    ]

    if missing_columns:
        return {
            "pipeline": "financial-market-pipeline",
            "layer": "bronze",
            "execution_timestamp": (
                execution_time.isoformat()
            ),
            "status": "FAILED",
            "missing_columns": missing_columns,
        }

    expected_assets = int (
        len(SYMBOLS)
    )

    missing_assets = list(
        set(SYMBOLS)
        - set(df["symbol"].unique())
    )

    null_close = int (
        df['close'].isna().sum()
    )

    null_volume = int (
        df['volume'].isna().sum()
    )

    null_symbol = int (
        df['symbol'].isna().sum()
    )

    duplicated_trade_records = int (
        df.duplicated(
            subset=[
                'symbol',
                'date'
            ]
        ).sum()
    )
    
    report = {
        "pipeline": "financial-market-pipeline",
        "layer": "bronze",

        "execution_timestamp": (
            execution_time.isoformat()
        ),

        "status": (
            "SUCCESS"
            if (
                null_symbol == 0
                and null_close == 0
                and null_volume == 0
                and len(missing_assets) == 0
            )

            else "WARNING"
        ),

        "schema": {
            "columns": list(df.columns),
            "dtypes": {
                col: str(dtype)
                for col, dtype in df.dtypes.items()
            },
        },

        "metrics": {
            "rows_ingested": len(df),
            "expected_assets": (
                expected_assets
            ),
            "assets_processed": (
                df["symbol"].nunique()
            ),
            "missing_assets": (
                missing_assets
            ),
            "null_close": null_close,
            "null_volume": null_volume,
            "null_symbol": null_symbol,
            "duplicated_trade_records": (
                duplicated_trade_records
            ),
            "ingestion_duration_seconds": (
                end_time - start_time
            ).total_seconds(),
            "missing_columns": missing_columns,
        },

        "date_range": {
            "min_trade_date": (
                str(df["date"].min().date())
            ),
            "max_trade_date": (
                str(df["date"].max().date())
            ),
        },
    }

    logger.info("Validation Report created")

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
        "bronze/"
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

def save_raw_data(
    df: pd.DataFrame,
    bucket_name: str,
    execution_time: datetime,
) -> None:

    partition = execution_time.strftime(
        "%Y-%m-%d"
    )

    filename = execution_time.strftime(
        "%Y%m%d_%H%M%S"
    )

    path = (
        f"s3://{bucket_name}/bronze/"
        f"ingestion_date={partition}/"
        f"market_data_raw_{filename}.parquet"
    )

    logger.info(
        f"Saving Bronze data to {path}"
    )

    df.to_parquet(
        path,
        storage_options={"anon": False},
        index=False,
    )

    logger.info(
        f"Successfully saved {len(df)} rows"
    )


if __name__ == "__main__":

    BUCKET_NAME = (
        "samuel-financial-data-lake"
    )

    try:

        logger.info(
            "Starting ingestion process"
        )

        execution_time = datetime.now(UTC)
        start_time = execution_time

        df = fetch_market_data(
            SYMBOLS
        )

        df["ingestion_timestamp"] = (
            execution_time
        )

        end_time = datetime.now(UTC)

        report = create_validation_report(
            df,
            execution_time,
            start_time,
            end_time,
        )

        save_raw_data(
            df,
            BUCKET_NAME,
            execution_time,
        )

        save_validation_report(
            report,
            BUCKET_NAME,
            execution_time,
        )

        logger.info(
            "Ingestion process completed successfully"
        )

    except Exception as e:
        logger.exception(
            f"Ingestion failed: {e}"
        )
        raise