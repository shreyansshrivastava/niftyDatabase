import config
from databaseFile import insertDataDb
import yfinance as yf
import pandas as pd
import mplfinance as mpf
from io import BytesIO
from config import symbol, days, intervalDay
from growwapi import GrowwAPI
from growwCall import getAccessToken
import time
def saveImages():


    df = yf.download(symbol, period=days, interval= intervalDay, auto_adjust=False,
        progress=False)

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)


    df = df[['Open', 'High', 'Low', 'Close', 'Volume']]

    df = df.apply(pd.to_numeric, errors='coerce')

    df.dropna(inplace=True)

    df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_convert('Asia/Kolkata')
    df['date'] = df.index.date
    grouped = df.groupby('date')
    interval = config.intervalDay
    #checkDbTable.checkTable()
    for date, day_df in df.groupby('date'):
        day_df = day_df.drop(columns=['date'])

        if len(day_df) < 5:  # skip partial days
            continue

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
        insertDataDb.insertDataDay(date, interval, close_bytes, image_bytes)
    print("Candlestick image saved")









