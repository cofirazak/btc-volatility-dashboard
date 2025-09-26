# src/backfill_script.py
"""
This script is used to backfill historical k-line data for a specified period
by fetching it from the Binance API via the binance_client module.
"""

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from binance_client import fetch_klines
from data_analyzer import analyze_data_gaps
from sqlalchemy import create_engine, exc
from database_utils import upsert_to_mysql
import os
from dotenv import load_dotenv
from sqlalchemy import text

# Load and read the variables from .env
load_dotenv()
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
HOST = os.getenv('DB_HOST')
PORT = os.getenv('DB_PORT')
DATABASE = os.getenv('DB_NAME')

if __name__ == "__main__":
    # Goal: Fetch all missing hourly candles up to the last fully completed one.
    # The script automatically finds the last entry in the database and
    # starts fetching from there, ensuring data integrity.

    start_date = None
    end_date = dt.now().replace(minute=0, second=0, microsecond=0) - relativedelta(hours=1)

    print("--- Starting database integrity audit ---")
    # Creating sqlalchemy engine to connect to the database.
    db_connection_str = f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    engine = create_engine(db_connection_str)

    try:
        with engine.connect() as connection:
            # 1. Fetch all required metrics in a single query.
            query = text("SELECT MIN(open_time) as min_ts, MAX(open_time) as max_ts, COUNT(*) as total_rows FROM klines_hourly;")
            db_stats = connection.execute(query).fetchone()

            # 2. Analyze the result.
            if db_stats and db_stats.total_rows > 0:
                # If the table is not empty, conduct a full "check-up".
                min_dt = dt.fromtimestamp(db_stats.min_ts / 1000)
                max_dt = dt.fromtimestamp(db_stats.max_ts / 1000)

                # How many hours should there be between the minimum and maximum?
                expected_hours = (max_dt - min_dt).total_seconds() / 3600 + 1

                print(f"Statistics from DB: Rows={db_stats.total_rows}, Expected={int(expected_hours)}")

                if int(db_stats.total_rows) == int(expected_hours):
                    is_consistent = True
                    start_date = max_dt + relativedelta(hours=1)
                    print("✅ The data has been approved. Starting the delta upload.")
                else:
                    is_consistent = False
                    print("❗ Data inconsistency detected (missing values).")
            else:
                is_consistent = False
                print("The table in the database is empty.")

    except Exception as e:
        is_consistent = False
        print(f"Error during database audit. Consider the data inconsistent. Error: {e}")

    if not is_consistent:
        print("Launching the full historical download.")
        start_date = dt(year=end_date.year - 1, month=1, day=1)

    if start_date >= end_date:
        print("✅ Database is already up to date. No new data to fetch.")
        exit()

    # Call our client with the calculated dates
    data_to_load = fetch_klines(start_dt=start_date, end_dt=end_date)

    if not data_to_load.empty:
        print(f"\nGot {len(data_to_load)} new klines to save.")

        # Perform the data gap analysis
        analyze_data_gaps(data_to_load, start_date, end_date)

        # Write new data to the database.
        upsert_to_mysql(data_to_load, 'klines_hourly', engine)
    else:
        print("No new data to save.")