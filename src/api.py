# src/api.py
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from src.metrics_calculator import calculate_session_metrics
from src.binance_client import fetch_klines
from datetime import datetime as dt
from dateutil.tz import gettz
from dateutil.relativedelta import relativedelta
import pandas as pd

# --- Configuration and DB Connection ---
load_dotenv()
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
HOST = os.getenv('DB_HOST')
PORT = os.getenv('DB_PORT')
DATABASE = os.getenv('DB_NAME')

db_connection_str = f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
engine = create_engine(db_connection_str)

# 1. Create a FastAPI "application" instance
app = FastAPI(
    title="BTC Volatility Dashboard API",
    description="An API to serve pre-calculated BTC session data and statistics.",
    version="1.0.0"
)

# --- Endpoints ---

@app.get("/")
def read_root():
    """
    Root endpoint to check if the API is running.
    """
    return {"status": "ok", "message": "Welcome to the BTC Volatility API!"}

@app.get("/statistics")
def get_statistics(
    days: int = Query(default=30, ge=1, le=365, description="Number of past days to retrieve statistics for (1-365).")
):
    """
    Endpoint to retrieve pre-calculated session statistics for a specified number of days.
    """
    print(f"Received request for /statistics with days={days}")
    try:
        with engine.connect() as connection:
            # #WHY: We must exclude the current, potentially incomplete session from historical statistics.
            # We only select sessions whose open_time is *before* the start of today's session.
            now_moscow = dt.now(gettz('Europe/Moscow'))
            if now_moscow.hour < 11:
                today_session_start = now_moscow.replace(hour=11, minute=0, second=0, microsecond=0) - relativedelta(days=1)
            else:
                today_session_start = now_moscow.replace(hour=11, minute=0, second=0, microsecond=0)

            query = text("SELECT * FROM daily_sessions WHERE open_time < :today_start ORDER BY open_time DESC LIMIT :limit;")
            result = connection.execute(query, {"limit": days, "today_start": today_session_start}).mappings().all()

        print(f"Successfully fetched {len(result)} records from the database.")
        return {"status": "ok", "count": len(result), "data": result}
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Could not fetch data from the database.")

@app.get("/today")
def get_today_stats():
    """
    Calculates and returns real-time statistics for the current, ongoing session
    by fetching all required data directly from the source API.
    """
    print("Received request for real-time /today stats")
    
    try:
        # --- Step 1: Determine the time window needed for calculation ---
        
        # #WHY: To calculate a 30-day rolling window for *today*, we need today's
        # hourly data PLUS the last 30 days of historical hourly data.
        # We fetch 40 days to be safe.
        end_dt = dt.now(gettz('UTC')) # Always use timezone-aware objects
        start_dt = (end_dt - relativedelta(days=40)).replace(hour=8,minute=0,second=0)

        # --- Step 2: Fetch ALL necessary data directly from Binance ---
        # #WHY: This simplifies the logic immensely and always uses the "source of truth".
        df_hourly = fetch_klines(start_dt=start_dt, end_dt=end_dt)

        if df_hourly.empty:
            raise HTTPException(status_code=404, detail="Could not fetch recent data from Binance.")
        # #WHY: The fetch_klines function returns raw numeric timestamps.
        # Before we can do any time-series calculations, we MUST convert
        # the 'open_time' column to proper, timezone-aware datetime objects.
        df_hourly_prepared = df_hourly.copy()
        df_hourly_prepared['open_time'] = pd.to_datetime(df_hourly_prepared['open_time'], unit='ms', utc=True)
        print(f"Loaded and prepared {len(df_hourly_prepared)} hourly klines for calculation.")
        # --- Step 3: Reuse our universal calculation logic ---
        all_calculated_sessions = calculate_session_metrics(df_hourly_prepared)

        # --- Step 4: Return only the very last row, which represents today's session ---
        if all_calculated_sessions.empty:
            return {"status": "ok", "message": "Not enough data to form a session."}
        # .iloc[-1] gets the last row of the DataFrame.
        # .to_dict() converts it to a dictionary, which is JSON-compatible.
        today_s_data = all_calculated_sessions.iloc[-1].to_dict()
        # The index (open_time) is not included in to_dict(), so we add it manually.
        today_s_data['open_time'] = all_calculated_sessions.index[-1].isoformat()

        return {"status": "ok", "data": today_s_data}

    except Exception as e:
        print(f"An error occurred while calculating today's statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error: Could not process today's statistics.")