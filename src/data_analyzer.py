"""
This module provides functions for analyzing and validating time-series data.
"""

from datetime import datetime as dt
from dateutil.tz import tzlocal
import pandas as pd


def analyze_data_gaps(historical_data: pd.DataFrame, start_date: dt, end_date: dt):
    """
    Analyzes the provided DataFrame to find any missing hourly candles within the expected date range.

    Args:
        historical_data: DataFrame containing the fetched k-line data. Must have an 'open_time' column.
        start_date: The expected start datetime of the data range.
        end_date: The expected end datetime of the data range.
    """
    print("\n--- Starting Data Gap Analysis ---")

    # 1. Convert Binance's UTC milliseconds to timezone-aware UTC datetime objects
    datetimes = pd.to_datetime(historical_data['open_time'], unit='ms', utc=True)

    # 2. Create the ideal, complete time range, correctly handling timezones.
    # The naive start/end dates from the backfill script imply the system's local timezone.
    # We first create a date range and tell pandas it's in the local timezone,
    # then we convert it to UTC to match the data from Binance for a correct comparison.
    print(f"Analyzing data gaps in the range (times imply local timezone):")
    print(f"From: {start_date}")
    print(f"To:   {end_date}\n")
    
    ideal_range_naive = pd.date_range(start=start_date, end=end_date, freq='h')
    ideal_range = ideal_range_naive.tz_localize(tzlocal()).tz_convert('UTC')

    # 3. Find the difference between the ideal range and your data
    # Convert to sets for a fast difference operation
    actual_timestamps_set = set(datetimes)
    ideal_timestamps_set = set(ideal_range)

    missing_timestamps = sorted(list(ideal_timestamps_set - actual_timestamps_set))

    # 4. Print the results
    if not missing_timestamps:
        print("🎉 No data gaps found!")
    else:
        print(f"❗ Found {len(missing_timestamps)} missing candles.\n")
        print("List of missing timestamps (UTC):")
        for ts in missing_timestamps:
            # Convert back to milliseconds for clarity
            missing_ms = int(ts.timestamp() * 1000)
            print(f"- {ts} (timestamp: {missing_ms})")
    print("--- Data Gap Analysis Finished ---")
