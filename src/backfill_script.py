# src/backfill_script.py
"""
This script is used to backfill historical k-line data for a specified period
by fetching it from the Binance API via the binance_client module.
"""

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from binance_client import fetch_historical_klines
from data_analyzer import analyze_data_gaps


if __name__ == "__main__":
    # Goal: Fetch all fully formed hourly candles from the beginning of the previous year.
    # To get only completed candles, the end date must be the open time of the
    # last fully completed candle.
    current_hour_start = dt.now().replace(minute=0, second=0, microsecond=0)
    end_date = current_hour_start - relativedelta(hours=1)

    # The starting point is the beginning of the previous year.
    start_date = dt(year=end_date.year - 1, month=1, day=1)

    # Call our client with the calculated dates
    historical_data = fetch_historical_klines(start_dt=start_date, end_dt=end_date)

    if not historical_data.empty:
        print("\nFinal DataFrame:")
        print(historical_data.head())
        print("...")
        print(historical_data.tail())
        print(f"\nSuccessfully fetched {len(historical_data)} records.")

        # Perform the data gap analysis
        analyze_data_gaps(historical_data, start_date, end_date)

        # Save the DataFrame to a CSV file.
        # We set index=False because the DataFrame index is not meaningful for this data.
        csv_filename = 'btcusdt_1h_historical_data.csv'
        historical_data.to_csv(csv_filename, index=False)
        print(f"\nData has been saved to '{csv_filename}'")
    else:
        print("No data was fetched.")