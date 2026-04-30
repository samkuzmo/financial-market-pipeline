import yfinance as yf
import pandas as pd
from datetime import datetime, UTC

def fetch_market_data(symbol="PETR4.SA", period="6mo"):
    df = yf.download(
        symbol, 
        period=period,
        auto_adjust=False,
        progress=False        
        )
    
    if isinstance(df.columns,pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.reset_index(inplace=True)
    
    df["symbol"] = symbol
    df["ingestion_timestamp"] = datetime.now(UTC)

    return df

def save_raw_data(df, bucket_name):

    path = f"s3://{bucket_name}/bronze/market_data_raw.parquet"

    df.to_parquet(
        path,
        storage_options = {"anon": False},
        index = False
    )

    print(f"Dados salvos em: {path}")

if __name__ == "__main__":
    
    BUCKET_NAME = "samuel-financial-data-lake"

    df = fetch_market_data()

    save_raw_data(df, BUCKET_NAME)