# src/session_aggregator.py
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from database_utils import upsert_to_mysql
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz


# --- Configuration and Connection ---
load_dotenv()
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
HOST = os.getenv('DB_HOST')
PORT = os.getenv('DB_PORT')
DATABASE = os.getenv('DB_NAME')

def run_session_aggregation():
    """
    Main function that runs the entire ETL process for session aggregation.
    It reads new hourly data, transforms it into daily sessions, calculates statistics,
    and upserts the result into the database.
    """
    db_connection_str = f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    engine = create_engine(db_connection_str)

    # --- Step 1: Determine the starting point for processing ---
    start_processing_from_dt = None
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT MAX(open_time) FROM daily_sessions;"))
            latest_session_time = result.scalar_one_or_none()

            if latest_session_time:
                # The datetime from the DB is "naive" (has no timezone info).
                # We must "localize" it to the Moscow timezone before we can compare it
                # with our timezone-aware DataFrame index.
                moscow_tz = gettz('Europe/Moscow')
                start_processing_from_dt = latest_session_time.replace(tzinfo=moscow_tz)
                print(f"✅ Found existing sessions. Will process new data starting from {start_processing_from_dt}.")
            else:
                print("Table 'daily_sessions' is empty. Processing all historical data.")

    except Exception as e:
        print(f"Could not check for latest session; will process all data. Error: {e}")

    # --- Step 2: Extract Required Hourly Data (with historical context) ---
    print("Reading required hourly k-lines from the database...")
    try:
        query = text("SELECT * FROM klines_hourly WHERE open_time >= :start_ts ORDER BY open_time")

        start_ts_ms = 0 # Default to fetching all data if the sessions table is empty
        if start_processing_from_dt:
            # Fetch data with a large margin (40 days) to ensure we have
            # enough historical context to correctly calculate the 30-day rolling statistics.
            start_ts_dt = start_processing_from_dt - relativedelta(days=40)
            start_ts_ms = int(start_ts_dt.timestamp() * 1000)

        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params={'start_ts': start_ts_ms})

        if df.empty:
            print("No new hourly data to process. Exiting.")
            return

        # Convert open_time into datetime format
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
        print(f"Successfully loaded {len(df)} hourly rows to process.")

    except Exception as e:
        print(f"An error occurred during data extraction: {e}")
        return # If we can't read the data, there's no point in continuing

    # --- Step 3: Transform Data and Calculate All Metrics ---
    # Perform all calculations on the single DataFrame that now contains both new data and historical context.
    df.set_index('open_time', inplace=True)
    df.index = df.index.tz_convert('Europe/Moscow')

    print("\nData converted to 'Europe/Moscow' timezone. Resampling into daily sessions...")
    # Dictionary with aggregation rules
    aggregation_rules = {
        'open_price': 'first',  # The session's open price is the open price of the FIRST hour
        'high_price': 'max',    # The session's high is the MAXIMUM of all hourly highs
        'low_price': 'min',     # The session's low is the MINIMUM of all hourly lows
        'close_price': 'last'   # The session's close price is the close price of the LAST hour
    }
    # Perform resampling and aggregation
    # '24h' - interval, '11h' - window start offset
    daily_sessions_df = df.resample('24h', offset='11h').agg(aggregation_rules)
    # Drop rows that might contain only NaNs (if a 24h period had no data)
    daily_sessions_df.dropna(inplace=True)

    print("Calculating base metrics (Volatility and Range)...")
    # Volatility (Vol) = abs(Open – Close) for the session.
    # #WHY: We use .abs() to get the absolute value, as volatility cannot be negative.
    daily_sessions_df['volatility'] = (daily_sessions_df['open_price'] - daily_sessions_df['close_price']).abs()
    # Range (Rng) = High – Low for the session.
    daily_sessions_df['range'] = daily_sessions_df['high_price'] - daily_sessions_df['low_price']

    print("Calculating rolling window statistics...")
    # The list of windows we need
    windows = [5, 7, 14, 30]
    # The list of metrics we need
    metrics = ['volatility', 'range']
    # Loop through metrics and windows to create new statistic columns
    for metric in metrics:
        for window in windows:
            # For Vol and Range, calculate:
            # mean, median, p10, p25, p75, p90
            # for windows: 5, 7, 14, 30 days

            # Calculate mean
            # Column name, e.g., 'vol_ma_14'
            col_name_mean = f"{metric[:3]}_ma_{window}"
            daily_sessions_df[col_name_mean] = daily_sessions_df[metric].rolling(window=window).mean()
            # Calculate median
            col_name_median = f"{metric[:3]}_median_{window}"
            daily_sessions_df[col_name_median] = daily_sessions_df[metric].rolling(window=window).median()
            # Calculate percentiles
            col_name_p10 = f"{metric[:3]}_p10_{window}"
            daily_sessions_df[col_name_p10] = daily_sessions_df[metric].rolling(window=window).quantile(0.1)

            col_name_p25 = f"{metric[:3]}_p25_{window}"
            daily_sessions_df[col_name_p25] = daily_sessions_df[metric].rolling(window=window).quantile(0.25)

            col_name_p75 = f"{metric[:3]}_p75_{window}"
            daily_sessions_df[col_name_p75] = daily_sessions_df[metric].rolling(window=window).quantile(0.75)

            col_name_p90 = f"{metric[:3]}_p90_{window}"
            daily_sessions_df[col_name_p90] = daily_sessions_df[metric].rolling(window=window).quantile(0.9)


    # --- Step 4: Isolate New/Updated Rows and Load to DB ---
    # We only want to save the rows that we actually need to update or insert.
    if start_processing_from_dt:
        final_df_to_load = daily_sessions_df[daily_sessions_df.index >= start_processing_from_dt].copy()
    else:
        # If it was the first run, all calculated sessions are new.
        final_df_to_load = daily_sessions_df.copy()

    final_df_to_load.reset_index(inplace=True)

    if final_df_to_load.empty:
        print("\nNo new session data to save.")
    else:
        print(f"\nSaving {len(final_df_to_load)} aggregated session records to the 'daily_sessions' table...")
        upsert_to_mysql(
            df=final_df_to_load,
            table_name='daily_sessions',
            engine=engine
        )
        print("✅ Aggregated session data successfully saved to the database.")

# --- Entry Point ---
if __name__ == "__main__":
    run_session_aggregation()
