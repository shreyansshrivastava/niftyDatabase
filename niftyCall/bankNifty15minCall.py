import numpy as np
import yfinance as yf
import pandas as pd
import mplfinance as mpf
from io import BytesIO
from datetime import datetime, timedelta
import pytz
from databaseFile.databaseConnection import conn
from config import symbol, days, interval15min
cur = conn.cursor()

SYMBOL = symbol
INTERVAL = interval15min

# NSE timezone
tz = pytz.timezone("Asia/Kolkata")

end_date = datetime.now(tz)
start_date = end_date - timedelta(days=days)


def download_nifty_15m():
    print("inside BankNIfty call")
    df = yf.download(
        SYMBOL,
        interval=INTERVAL,
        start=start_date,
        end=end_date,
        group_by=False,
        auto_adjust=False,
        progress=False,
        prepost=False
    )

    if df is None or df.empty:
        raise RuntimeError("❌ NSE 15m data unavailable (yfinance issue)")

    return df


def save_images():
    df = download_nifty_15m()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

    df = df[['^NSEBANK_Open', '^NSEBANK_High',
             '^NSEBANK_Low', '^NSEBANK_Close', '^NSEBANK_Volume']]

    df.rename(columns={
        '^NSEBANK_Open': 'Open',
        '^NSEBANK_High': 'High',
        '^NSEBANK_Low': 'Low',
        '^NSEBANK_Close': 'Close',
        '^NSEBANK_Volume': 'Volume'
    }, inplace=True)

    df.dropna(inplace=True)

    df.index = pd.to_datetime(df.index).tz_convert("Asia/Kolkata")
    df['trade_date'] = df.index.date

    for trade_date, day_df in df.groupby('trade_date'):
        day_df = day_df.drop(columns=['trade_date'])

        if len(day_df) < 10:
            continue

        print(f"\nProcessing {trade_date}")

        for i in range(len(day_df)):
            candle_df = day_df.iloc[: i + 1]
            candle_time = candle_df.index[-1]

            candle_count = len(candle_df)   # ✅ THIS IS THE KEY

            cur.execute("""
                SELECT 1 FROM nifty_15min_images
                WHERE trade_date=%s AND candle_time=%s
            """, (trade_date, candle_time))

            if cur.fetchone():
                continue

            close_prices = candle_df["Close"].astype(np.float64).values
            close_price_bytes = close_prices.tobytes()

            buffer = BytesIO()
            mpf.plot(
                candle_df,
                type="candle",
                volume=True,
                style="yahoo",
                tight_layout=True,
                savefig=dict(fname=buffer, dpi=90)
            )

            buffer.seek(0)
            img_bytes = buffer.read()

            cur.execute("""
                INSERT INTO nifty_15min_images
                (trade_date, candle_time, interval, candle_count, image, close_price)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                trade_date,
                candle_time,
                INTERVAL,
                candle_count,        # ✅ STORED HERE
                img_bytes,
                close_price_bytes
            ))

    print("\n✅ Done saving images")




