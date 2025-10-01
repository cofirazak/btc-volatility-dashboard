# src/metrics_calculator.py
import pandas as pd


def calculate_session_metrics(df_hourly: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DataFrame of hourly k-lines and returns a DataFrame
    with aggregated daily sessions and all calculated statistics.
    """
    df = df_hourly.copy()
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

    return daily_sessions_df
