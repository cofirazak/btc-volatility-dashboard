# backfill_script.py

# This script is used to backfill the data for the BTCUSDT and 1H interval from the Binance API

from datetime import timedelta, datetime as dt
from dateutil.relativedelta import relativedelta
import requests
import time
import pandas as pd

param_symbol = 'BTCUSDT'
param_interval = '1h'
param_limit = 1000

# 4. Set the param_startTime variable to 1 year ago.
dt_now = dt.now()
dt_now_day_start = dt(dt_now.year,dt_now.month,dt_now.day,0,0,0)
dt_now_day_end = dt(dt_now.year,dt_now.month,dt_now.day,23,59,59)
dt_startTime = dt_now_day_start - relativedelta(years=1)
param_startTime = int(dt_startTime.timestamp()) * 1000

# 5. set the api_schema variable to 'https' String.
api_schema = 'https'
# 6. set the api_host variable to 'data-api.binance.vision' String.
api_host = 'data-api.binance.vision'
# 7. set the api_path variable to 'api/v3/klines' String.
api_path = 'api/v3/klines'
# 8. send the GET request to Binance API
payload_params = {'symbol':param_symbol,'interval':param_interval,'limit':param_limit}
# 9. Init a list for all candles
candles_list = []

# 10. while the param_startTime is less then datetime now
while param_startTime < int(dt_now.timestamp() * 1000):
    # 11. move the param_endTime value to 41 days after param_startTime
    dt_endTime = dt_startTime + relativedelta(days=40)
    dt_endTime_day_end = dt_endTime.replace(hour=23,minute=59,second=59)
    param_endTime = int(dt_endTime_day_end.timestamp() * 1000)
    # 12. update the dynamic parameters – startTime and endTime
    dynamic_params = {'startTime':param_startTime,'endTime':param_endTime}
    payload_params.update(dynamic_params)

    print(f'payload_params:\n{payload_params}')

    # 13. make a request to the Binance API to get a batch of candles
    response = requests.get(
        f'{api_schema}://{api_host}/{api_path}', 
        params=payload_params
        )
    # 14. If the response is not 200OK, then rais the error.
    response.raise_for_status()
    # 15. Ohterwise get, save and print the data
    data = response.json()
    if data:
        for d in data:
            candles_list.append(d)
    else:
        print("The API response data is empty.")
    dt_startTime = dt_startTime + relativedelta(days=41)
    param_startTime = int(dt_startTime.timestamp()) * 1000
    print("Pause for 1 second to avoid API request weight per minute Hard-Limits...")
    time.sleep(1)

# 16. create a DataFrame from all the saved candles
df_candles = pd.DataFrame(candles_list)
df_candles.columns = ['open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'unused_field_ignore']
df_candles.drop(columns=['unused_field_ignore'], inplace=True)
print("\nФинальный DataFrame:")
print(df_candles)