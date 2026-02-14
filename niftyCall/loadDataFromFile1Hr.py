import pandas as pd
import psycopg2
import numpy as np
from databaseFile.databaseConnection import conn
def process_and_upload_nifty(csv_path):

    # ================= READ CSV =================
    df = pd.read_csv(csv_path)

    df['Datetime'] = pd.to_datetime(df['date'])
    df.set_index('Datetime', inplace=True)

    # ================= RESAMPLE TO 1H (NSE aligned 09:15) =================
    df_1h = df.resample(
        '60min',
        origin='start',
        offset='15min'
    ).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })

    df_1h.dropna(inplace=True)
    df_1h.reset_index(inplace=True)

    # ================= DB CONNECTION =================

    cur = conn.cursor()

    insert_query = """
        INSERT INTO nifty_1hr_images
        (trade_date, candle_time, "interval", image, close_price, candle_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_date, candle_time) DO NOTHING
    """

    # ================= INSERT WITH CANDLE COUNT RESET PER DAY =================
    for trade_date, day_data in df_1h.groupby(df_1h['Datetime'].dt.date):

        day_data = day_data.sort_values("Datetime")

        for idx, row in enumerate(day_data.itertuples(), start=1):

            close_array = np.array([row.close], dtype=np.float64)
            close_bytes = close_array.tobytes()

            cur.execute(insert_query, (
                trade_date,          # trade_date
                row.Datetime,        # candle_time
                "1h",                # interval
                None,                # image (optional)
                psycopg2.Binary(close_bytes),
                idx                  # candle_count 1–7
            ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ 1-hour data uploaded successfully.")
