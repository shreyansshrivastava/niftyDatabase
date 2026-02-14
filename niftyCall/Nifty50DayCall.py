import config
from databaseFile import insertDataDb
import yfinance as yf
import pandas as pd
import mplfinance as mpf
from io import BytesIO
from config import symbol, days, interval15min
from growwapi import GrowwAPI
from growwCall import getAccessToken
import time
def saveImages():
    print("Inside image saved")
    symbol = "^NSEI"
    # df = yf.download(symbol, period=days, interval= interval15min, auto_adjust=False,
    #     progress=False)
    df = yf.download(
        symbol,
        start="2007-09-17",
        end="2026-02-14",
        interval="1d"
    )


    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

    df = df[[f'Open_{symbol}', f'High_{symbol}', f'Low_{symbol}', f'Close_{symbol}', f'Volume_{symbol}']]

    # 4. Rename to standard OHLCV
    df.rename(columns={
        f'Open_{symbol}': 'Open',
        f'High_{symbol}': 'High',
        f'Low_{symbol}': 'Low',
        f'Close_{symbol}': 'Close',
        f'Volume_{symbol}': 'Volume'
    }, inplace=True)

    df.dropna(inplace=True)
    print(df)
    # df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
    #
    # df = df.apply(pd.to_numeric, errors='coerce')

    # df.dropna(inplace=True)

    df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
    else:
        df.index = df.index.tz_convert('Asia/Kolkata')

    df['date'] = df.index.date
    grouped = df.groupby('date')
    interval = interval15min
    for date, day_df in df.groupby('date'):

        day_df = day_df.drop(columns=['date'])

        # if len(day_df) < 5:  # skip partial days
        #     continue

        # Save image to BytesIO (in memory)
        image_buffer = BytesIO()

        mpf.plot(
            day_df,
            type='candle',
            volume=True,
            style='yahoo',
            title=f"NIFTY 15-Min Candlestick ({date})",
            savefig=image_buffer
        )
        image_buffer.seek(0)  # go to start of buffer
        image_bytes = image_buffer.read()
        # Insert into DB
        print(f"Saved image for {date} to DB")
        close_bytes = day_df['Close'].values.tobytes()
        insertDataDb.insertNifty50DataDay(date, interval, close_bytes, image_bytes)
    print("Candlestick image saved")









