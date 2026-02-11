import numpy as np
import yfinance as yf
import pandas as pd
import mplfinance as mpf
from io import BytesIO
from datetime import datetime, timedelta
import pytz
from databaseFile.databaseConnection import conn
from config import symbol,days, intervalHr
cur = conn.cursor()
import tempfile

SYMBOL = symbol
INTERVAL = intervalHr

# NSE timezone
tz = pytz.timezone("Asia/Kolkata")

end_date = datetime.now(tz)
start_date = end_date - timedelta(days=days)

with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
    tmp_path = tmp.name
def download_nifty_2hr():
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
        raise RuntimeError("❌ NSE 1h data unavailable (yfinance issue)")

    return df

def save_images():
    df = download_nifty_2hr()

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
        day_df = day_df.sort_index()

        closes = day_df["Close"].astype(np.float64).values

        for n in range(1, len(closes) + 1):
            candle_df = day_df.iloc[:n]
            candle_time = candle_df.index[-1]
            candle_count = n

            cur.execute("""
                SELECT 1 FROM nifty_1hr_images
                WHERE trade_date=%s AND candle_count=%s
            """, (trade_date, candle_count))

            if cur.fetchone():
                continue

            close_bytes = closes[:n].tobytes()

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

            cur.execute("""
                INSERT INTO nifty_1hr_images
                (trade_date, candle_time, candle_count, interval, image, close_price)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                trade_date,
                candle_time,
                candle_count,
                "1h",
                buffer.read(),
                close_bytes
            ))

    print("\n✅ Done saving images")




