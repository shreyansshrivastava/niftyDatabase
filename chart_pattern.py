import yfinance as yf
import numpy as np
import pandas as pd
from databaseConnection import conn
from config import symbol, interval, days
INTERVAL = interval

#=============== get_historical ========================
def get_historical(symbol=symbol, interval=interval, days=days):
    try:
        df = yf.download(
            symbol,
            interval=interval,
            period=days,
            progress=False,
            threads=False
        )

        if df is None or df.empty:
            print(f"⚠ No data returned for {symbol}")
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.dropna()
        df = df[~df.index.duplicated()]

        return df

    except Exception as e:
        print(f"❌ Download failed for {symbol}: {e}")
        return pd.DataFrame()


#================ Sliding window =======================
def sliding_windows(df, window=30, step=5):
    for i in range(0, len(df) - window - 5, step):
        yield df.iloc[i:i+window], df.iloc[i+window:i+window+5]


#================ Avoid junk insert ====================
def is_pattern_candidate(df):
    returns = df["Close"].pct_change().dropna()

    range_ratio = float(
        (df["High"].max() - df["Low"].min()) / df["Close"].mean()
    )
    volatility = float(returns.std())

    if np.isnan(range_ratio) or np.isnan(volatility):
        return False

    return (range_ratio < 0.06) and (volatility < 0.015)


#================ Extract Shape ========================
def extract_shape(df, points=20):
    close = pd.to_numeric(df["Close"], errors="coerce").dropna()

    if len(close) < 5:
        return None

    close = close.to_numpy()

    x_old = np.arange(len(close))
    x_new = np.linspace(0, len(close) - 1, points)

    shape = np.interp(x_new, x_old, close)

    denom = shape.max() - shape.min()
    if denom == 0:
        return None

    shape = (shape - shape.min()) / denom
    return shape.tolist()


#================ Extract Features =====================
def extract_features(df):
    returns = df["Close"].pct_change().dropna()

    slope = np.polyfit(range(len(df)), df["Close"], 1)[0]

    return {
        "volatility": float(returns.std()),
        "range_ratio": float(
            (df["High"].max() - df["Low"].min()) / df["Close"].mean()
        )
    }


#================ Label Outcomes =======================
def label_outcomes(pattern_df, future_df):

    if len(pattern_df) == 0 or len(future_df) < 5:
        return False, False, False, False

    last_close = float(pattern_df["Close"].values[-1])

    next_close = float(future_df["Close"].values[0])
    next5_mean = float(future_df["Close"].values[:5].mean())

    future_high_max = float(future_df["High"].values.max())
    pattern_high_max = float(pattern_df["High"].values.max())

    next_candle_up = next_close > last_close
    next_5_up = next5_mean > last_close
    breakout = future_high_max > pattern_high_max * 1.002

    completed = breakout or next_5_up

    return next_candle_up, next_5_up, breakout, completed


#================ DB Safe ==============================
def db_safe(val):
    import numpy as np
    import pandas as pd

    if isinstance(val, pd.Series):
        return float(val.iloc[0])

    if isinstance(val, np.ndarray):
        return val.tolist()

    if isinstance(val, np.generic):
        return val.item()

    return val


#================ Insert Pattern =======================
def insert_pattern(cur, data):

    required = [
        "symbol", "interval", "pattern_time",
        "shape_vector", "range_ratio", "volatility",
        "next_candle_up", "next_5_up", "breakout", "completed"
    ]

    for k in required:
        if k not in data:
            raise KeyError(f"Missing required field: {k}")

    safe = {k: db_safe(v) for k, v in data.items()}

    cur.execute("""
        INSERT INTO chart_patterns (
            symbol, interval, pattern_time,
            shape_vector, range_ratio, volatility,
            next_candle_up, next_5_up, breakout, completed
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        safe["symbol"],
        safe["interval"],
        safe["pattern_time"],
        safe["shape_vector"],
        safe["range_ratio"],
        safe["volatility"],
        safe["next_candle_up"],
        safe["next_5_up"],
        safe["breakout"],
        safe["completed"]
    ))


#================ Main Builder =========================
def build_pattern_db():

    df = get_historical(symbol, INTERVAL, days=days)
    if df.empty:
        print("No data — skipping run")
        return

    cur = conn.cursor()
    inserted = 0

    for pattern_df, future_df in sliding_windows(df):

        if not is_pattern_candidate(pattern_df):
            continue

        shape = extract_shape(pattern_df)
        if shape is None:
            continue

        feats = extract_features(pattern_df)
        labels = label_outcomes(pattern_df, future_df)

        pattern_time = pattern_df.index[-1].to_pydatetime()

        insert_pattern(cur, {
            "symbol": symbol,
            "interval": INTERVAL,
            "pattern_time": pattern_time,
            "shape_vector": shape,
            "range_ratio": feats["range_ratio"],
            "volatility": feats["volatility"],
            "next_candle_up": labels[0],
            "next_5_up": labels[1],
            "breakout": labels[2],
            "completed": labels[3]
        })

        inserted += 1

        if inserted % 500 == 0:
            conn.commit()
            print(f"Inserted {inserted} patterns")

    conn.commit()
    cur.close()
    print(f"✅ Done. Total inserted: {inserted}")



