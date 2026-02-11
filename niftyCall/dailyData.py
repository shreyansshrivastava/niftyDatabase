import yfinance as yf
import pandas as pd
import pytz
from databaseFile.databaseConnection import conn
from growwapi import GrowwAPI
import time
from growwCall.getAccessToken import access_token
# Groww API Credentials (Replace with your actual credentials)
cur = conn.cursor()
def saveImages():
    API_AUTH_TOKEN = access_token

    # Initialize Groww API
    groww = GrowwAPI(API_AUTH_TOKEN)

    # you can give time programatically.
    end_time_in_millis = int(time.time() * 1000)  # epoch time in milliseconds
    start_time_in_millis = end_time_in_millis - (24 * 60 * 60 * 1000)  # last 24 hours

    # OR

    # you can give start time and end time in yyyy-MM-dd HH:mm:ss format.
    end_time = "2026-02-10 14:00:00"
    start_time = "2023-03-27 10:00:00"

    historical_data_response = groww.get_historical_candle_data(
        trading_symbol="BANKNIFTY",
        exchange=groww.EXCHANGE_NSE,
        segment=groww.SEGMENT_CASH,
        start_time=start_time,
        end_time=end_time,
        interval_in_minutes=1440  # Optional: Interval in minutes for the candle data
    )

    cur = conn.cursor()

    tz = pytz.timezone("Asia/Kolkata")

    # df = yf.download(
    #     "^NSEI",
    #     period="2y",
    #     interval="1d",
    #     auto_adjust=False,
    #     progress=False
    # )

    # df.dropna(inplace=True)
    # df.index = pd.to_datetime(df.index)
    # df.index = df.index.tz_localize("UTC").tz_convert(tz)
    data = []
    for candle in historical_data_response['candles']:

        candle_data = {
            "candle_date": time.strftime("%Y-%m-%d", time.localtime(candle[0])),
            "open": float(candle[1])if candle[1] is not None else 0.0,
            "high": float(candle[2])if candle[2] is not None else 0.0,
            "low": float(candle[3])if candle[3] is not None else 0.0,
            "close": float(candle[4])if candle[4] is not None else 0.0,
            "volume": int(candle[5])if candle[5] is not None else 0
        }
        data.append(candle_data)
    df = pd.DataFrame(data)

    # for idx, row in df.iterrows():
    #     candle_date = idx.date()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO nifty_day_prices
            (candle_date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (candle_date) DO NOTHING
        """, (
            row["candle_date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"]
        ))
    conn.commit()
