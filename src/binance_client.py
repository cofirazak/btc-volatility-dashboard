# src/binance_client.py
"""
This module provides a client for interacting with the Binance API to fetch cryptocurrency market data.
It includes functions for fetching historical and latest k-line (candlestick) data.
"""

from datetime import datetime as dt, timedelta
import time
import pandas as pd
import requests

# --- Constants ---
API_SCHEMA = 'https'
API_HOST = 'data-api.binance.vision'
API_PATH = 'api/v3/klines'
BASE_URL = f"{API_SCHEMA}://{API_HOST}/{API_PATH}"

SYMBOL = 'BTCUSDT'
INTERVAL = '1h'
KLINES_LIMIT = 1000

# The interval in milliseconds. For '1h' it's 60 * 60 * 1000 = 3,600,000 ms.
INTERVAL_MS = int(timedelta(hours=1).total_seconds() * 1000)

CANDLE_COLUMNS = [
    'open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume', 'unused_field_ignore'
]

# --- Functions ---

def fetch_klines(start_dt: dt, end_dt: dt) -> pd.DataFrame:
    """
    Fetches k-line (candlestick) data from Binance in a given date range.

    This function handles pagination by repeatedly calling the Binance API
    to fetch data in chunks until the entire requested period is covered.

    Args:
        start_dt: The start datetime of the period to fetch (inclusive).
        end_dt: The end datetime of the period to fetch (inclusive).

    Returns:
        A pandas DataFrame containing the k-line data, sorted by open time.
    """
    all_candles = []
    start_time_ms = int(start_dt.timestamp() * 1000)
    end_time_ms = int(end_dt.timestamp() * 1000)

    payload_params = {
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'limit': KLINES_LIMIT
    }

    print(f"Fetching data for {SYMBOL} from {start_dt.strftime('%Y-%m-%d %H:%M:%S')} ({int(start_dt.timestamp())}) to {end_dt.strftime('%Y-%m-%d %H:%M:%S')} ({int(start_dt.timestamp())})")

    while start_time_ms <= end_time_ms:
        payload_params['startTime'] = start_time_ms
        # The API endpoint is inclusive for both startTime and endTime.
        payload_params['endTime'] = end_time_ms

        print(f"Fetching chunk starting from: {dt.fromtimestamp(start_time_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')} ({int(start_time_ms / 1000)})")

        try:
            response = requests.get(BASE_URL, params=payload_params)
            response.raise_for_status()
            data = response.json()

            if data:
                all_candles.extend(data)
                # The open_time of the last candle is at index 0
                last_candle_open_time = data[-1][0]
                start_time_ms = last_candle_open_time + INTERVAL_MS
            else:
                # If no data is returned, we should advance the time to avoid an infinite loop.
                # We advance by the maximum possible time range for one request (limit * interval).
                start_time_ms += KLINES_LIMIT * INTERVAL_MS

            # Pause to respect API rate limits
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    if not all_candles:
        return pd.DataFrame(columns=[col for col in CANDLE_COLUMNS if col != 'unused_field_ignore'])

    df_candles = pd.DataFrame(all_candles, columns=CANDLE_COLUMNS)
    df_candles.drop(columns=['unused_field_ignore'], inplace=True)
    
    # Convert columns to numeric types, preserving original dtype if conversion fails.
    # This explicit loop replaces the deprecated `errors='ignore'` pattern.
    for col in df_candles.columns:
        try:
            df_candles[col] = pd.to_numeric(df_candles[col])
        except ValueError:
            # If conversion fails, log a warning and keep the original data type.
            print(f"Warning: Column '{col}' contains non-numeric data and could not be converted.")
    
    # Remove duplicates and sort
    df_candles.drop_duplicates(subset=['open_time'], inplace=True)
    df_candles.sort_values(by='open_time', inplace=True)
    df_candles['symbol'] = SYMBOL
    
    return df_candles
