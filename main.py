import pandas as pd
import os
import requests
from datetime import datetime, timedelta

# === Configuration ===
OUTPUT_FILE = "/workspaces/SPY_data/spy.csv"
USE_MOCK_DATA = False  # Change to False to fetch from Alpha Vantage
API_KEY = "71234567890123456789012345678901"  # Required if USE_MOCK_DATA is False
SYMBOL = "SPY"
INTERVAL = "1min"
YEAR = "2010"


def fetch_data(url):
    if USE_MOCK_DATA:
        return mock_response
    print("Fetching from:", url)
    response = requests.get(url)
    return response.json()


def parse_time_series(data):
    # Extract time series block
    key = next((k for k in data.keys() if "Time Series" in k), None)
    if not key:
        raise ValueError("Time series key not found.")
    ts = data[key]

    # Build DataFrame
    df = pd.DataFrame.from_dict(ts, orient='index')
    df.index = pd.to_datetime(df.index)
    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })
    df = df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int
    })
    df = df[["open", "high", "low", "close", "volume"]]
    df.sort_index(inplace=True)
    return df

def load_existing_data(filepath):
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        return pd.read_csv(filepath, index_col=0, parse_dates=True)
    # Return an empty dataframe with correct structure
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def merge_data(existing_df, new_df):
    combined = pd.concat([existing_df, new_df])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    return combined


def save_data(df, filepath):
    df.to_csv(filepath)
    print(f"✅ Data saved to {filepath}")


# === Execution ===
if __name__ == "__main__":
    for i in range(12):
        month = ""
        
        if i < 9:
            month = f"0{i+1}"
        else:
            month = f"{i+1}"
        date = f"{YEAR}-{month}"
        print(date)
        
        url = (
            f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval={INTERVAL}&apikey={API_KEY}&month={date}&outputsize=full"
        )
        existing_df = load_existing_data(OUTPUT_FILE)
        new_raw_data = fetch_data(url)
        new_df = parse_time_series(new_raw_data)
        updated_df = merge_data(existing_df, new_df)
        save_data(updated_df, OUTPUT_FILE)
        