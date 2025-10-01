# src/session_aggregator.py
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from database_utils import upsert_to_mysql
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
from src.metrics_calculator import calculate_session_metrics


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
    daily_sessions_df = calculate_session_metrics(df)


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
