import logging
from datetime import UTC, datetime

import pandas as pd
import yfinance as yf

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
                str(col).strip()
                for col in df.columns
            ]

            df["symbol"] = symbol
            df["ingestion_timestamp"] = (
                datetime.now(UTC)
            )

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


def save_raw_data(
    df: pd.DataFrame,
    bucket_name: str,
) -> None:
    """
    Save raw data to Bronze layer.

    Parameters
    ----------
    df : pd.DataFrame

    bucket_name : str
    """

    execution_time = datetime.now(UTC)

    partition = execution_time.strftime("%Y-%m-%d")
    filename = execution_time.strftime("%Y%m%d_%H%M%S")

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

    BUCKET_NAME = "samuel-financial-data-lake"

    try:
        logger.info(
            "Starting ingestion process"
        )

        df = fetch_market_data(
            SYMBOLS
        )

        save_raw_data(
            df,
            BUCKET_NAME,
        )

        logger.info(
            "Ingestion process completed successfully"
        )

    except Exception as e:
        logger.exception(
            f"Ingestion failed: {e}"
        )
        raise