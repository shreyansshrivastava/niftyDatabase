import pandas as pd
from psycopg2 import Binary
import mplfinance as mpf
from io import BytesIO
from databaseFile.databaseConnection import conn
import numpy as np

def process_and_upload_nifty(excel_file):
    # 1️⃣ Load CSV with technical indicators
    df = pd.read_csv(excel_file)
    df['date'] = pd.to_datetime(df['date'])

    cur = conn.cursor()

    # 2️⃣ Group by trading date
    for trade_date, day_df in df.groupby(df['date'].dt.date):
        # Drop the 'date' column temporarily for plotting
        plot_df = day_df.drop(columns=['date']).copy()
        plot_df.index = day_df['date']  # mplfinance requires DatetimeIndex

        if len(plot_df) < 1:
            continue

        print(f"\nProcessing {trade_date}")

        # 3️⃣ Loop through progressive candles 1→N (to store 1–25)
        for i in range(len(plot_df)):
            candle_df = plot_df.iloc[: i + 1]  # progressive slice
            candle_time = candle_df.index[-1]  # last candle of current slice
            candle_count = len(candle_df)

            # Skip if already exists
            cur.execute("""
                SELECT 1 FROM nifty_15min_images
                WHERE trade_date=%s AND candle_time=%s
            """, (trade_date, candle_time))
            if cur.fetchone():
                continue

            # Generate candlestick image in memory
            buffer = BytesIO()
            mpf.plot(
                candle_df,
                type='candle',
                volume=True,
                style='charles',
                tight_layout=True,
                savefig=dict(fname=buffer, format='png', dpi=90)
            )
            buffer.seek(0)
            img_bytes = buffer.read()

            # Store close_price array as BYTEA (all closes in this window)
            close_prices = candle_df['close'].astype(np.float64).values
            close_bytes = close_prices.tobytes()

            # Insert into DB
            try:
                cur.execute("""
                    INSERT INTO nifty_15min_images 
                    (trade_date, candle_time, "interval", image, close_price, candle_count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, candle_time) DO NOTHING;
                """, (
                    trade_date,
                    candle_time,
                    "15m",
                    Binary(img_bytes),
                    Binary(close_bytes),
                    candle_count
                ))
            except Exception as e:
                print(f"Error inserting {trade_date} candle {candle_count}: {e}")
                conn.rollback()
                continue

        conn.commit()
        print(f"✅ Completed {trade_date}")

    cur.close()
    conn.close()
    print("\nAll processing and uploads complete!")

# Usage
# process_and_upload_nifty("data/nifty_15m.csv")
