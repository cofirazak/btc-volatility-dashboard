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

    # Creating sqlalchemy engine to connect to the database.
    db_connection_str = f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    engine = create_engine(db_connection_str)

    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT MAX(open_time) FROM klines_hourly"))
            latest_timestamp = result.scalar_one_or_none()
    except Exception as e:
        print(f"Error while checking for the latest timestamp in the database: {e}")

    if latest_timestamp:
        start_date = dt.fromtimestamp(latest_timestamp / 1000) + relativedelta(hours=1)
        current_hour_start = dt.now().replace(minute=0, second=0, microsecond=0)
        end_date = current_hour_start - relativedelta(hours=1)
    else:
        current_hour_start = dt.now().replace(minute=0, second=0, microsecond=0)
        # The starting point is the beginning of the previous year.
        end_date = current_hour_start - relativedelta(hours=1)
        start_date = dt(year=end_date.year - 1, month=1, day=1)

    if start_date >= end_date:
        print("Database is already up to date. No new data to fetch.")
        exit()

    # Call our client with the calculated dates
    historical_data = fetch_klines(start_dt=start_date, end_dt=end_date)

    if not historical_data.empty:
        print("\nFinal DataFrame:")
        print(historical_data.head())
        print("...")
        print(historical_data.tail())
        print(f"\nSuccessfully fetched {len(historical_data)} records.")

        # Perform the data gap analysis
        analyze_data_gaps(historical_data, start_date, end_date)

        # Write new data to the database.
        upsert_to_mysql(historical_data, 'klines_hourly', engine)
    else:
        print("No data was fetched.")