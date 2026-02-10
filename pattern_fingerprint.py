import numpy as np
import pandas as pd
import yfinance as yf
from databaseConnection import conn
import pytz
from config import PERIOD, days, interval, symbol
# ================= CONFIG =================
SYMBOL = symbol
INTERVAL = interval


tz = pytz.timezone("Asia/Kolkata")

# ================= DB CONNECT =================
cur = conn.cursor()

# ================= HELPERS =================
def safe_float(x):
    if isinstance(x, (pd.Series, np.ndarray)):
        if len(x) == 0:
            return None
        x = x.iloc[-1] if isinstance(x, pd.Series) else x[-1]

    if pd.isna(x):
        return None
    return float(x)


def safe_bool(x):
    if isinstance(x, (pd.Series, np.ndarray)):
        if len(x) == 0:
            return None
        x = x.iloc[-1] if isinstance(x, pd.Series) else x[-1]

    if pd.isna(x):
        return None
    return bool(x)


def safe_int(x):
    if pd.isna(x):
        return None
    return int(x)


def safe_array(arr):
    if isinstance(arr, np.ndarray):
        return arr.tolist()
    if isinstance(arr, pd.Series):
        return arr.values.tolist()
    return arr


# ================= FEATURE BUILDER =================
def build_features(df):

    close = df["Close"]

    # Force Series
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]

    close = close.astype(float)

    returns = close.pct_change().dropna()

    return_total = float(close.iloc[-1] / close.iloc[0] - 1)
    volatility = float(returns.std())

    slope = float(np.polyfit(np.arange(len(close)), close.values, 1)[0])

    max_drawdown = float(
        ((close.cummax() - close) / close.cummax()).max()
    )

    range_ratio_val = (
                              df["High"].max() - df["Low"].min()
                      ) / df["Close"].mean()

    if isinstance(range_ratio_val, pd.Series):
        range_ratio_val = range_ratio_val.iloc[0]

    range_ratio = float(range_ratio_val)

    body_ratio_val = (
            abs(df["Close"] - df["Open"]).mean() /
            (df["High"] - df["Low"]).mean()
    )

    if isinstance(body_ratio_val, pd.Series):
        body_ratio_val = body_ratio_val.iloc[0]

    body_ratio = float(body_ratio_val)

    # ✅ SAFE scalar comparison
    trend_strength = abs(slope) / volatility if volatility > 0 else 0.0

    return {
        "return_total": return_total,
        "volatility": volatility,
        "slope": slope,
        "max_drawdown": max_drawdown,
        "range_ratio": range_ratio,
        "body_ratio": body_ratio,
        "prev_trend_slope": slope,
        "trend_strength": trend_strength,
        "vwap_distance": 0.0,           # placeholder
        "range_compression": 0.0,       # placeholder
        "volume_contraction": 0.0,      # placeholder
        "volume_expansion": 0.0,        # placeholder
        "pattern_length": int(len(df)),
        "swing_count": int(len(df) / 5),
        "symmetry_score": 1.0
    }


# ================= LABEL BUILDER =================
def build_labels(pattern_df, future_df):

    try:
        # ===== Flatten multi-index if exists =====
        if isinstance(pattern_df.columns, pd.MultiIndex):
            pattern_df.columns = pattern_df.columns.get_level_values(0)

        if isinstance(future_df.columns, pd.MultiIndex):
            future_df.columns = future_df.columns.get_level_values(0)

        pattern_df = pattern_df.dropna()
        future_df = future_df.dropna()

        if len(pattern_df) == 0 or len(future_df) < 5:
            return False, False, False, False

        # ===== FORCE SCALAR VALUES =====
        last_close = float(pattern_df["Close"].iloc[-1])

        next_close = float(future_df["Close"].iloc[0])
        next5_mean = float(future_df["Close"].iloc[:5].mean())

        future_high_max = float(future_df["High"].max())
        pattern_high_max = float(pattern_df["High"].max())

        # ===== BOOLEAN SCALARS ONLY =====
        next_candle_up = bool(next_close > last_close)
        next_5_up = bool(next5_mean > last_close)
        breakout = bool(future_high_max > pattern_high_max * 1.002)

        # SAFE BOOLEAN LOGIC
        completed = bool(breakout or next_5_up)

        return next_candle_up, next_5_up, breakout, completed

    except Exception as e:
        print("build_labels error:", e)
        return False, False, False, False



# ================= SHAPE VECTOR =================
def build_shape_vector(df, length=30):

    close = df["Close"]

    if len(close) < length:
        return None

    series = close.tail(length)
    norm = (series - series.min()) / (series.max() - series.min() + 1e-9)

    return norm.values


# ================= INSERT =================
def insert_pattern_fingerprint(cur, data):

    cur.execute("""
    INSERT INTO pattern_fingerprints (
        pattern_name, symbol, timeframe,
        shape_vector,
        return_total, volatility, slope, max_drawdown,
        range_ratio, body_ratio,
        prev_trend_slope, trend_strength, vwap_distance,
        range_compression, volume_contraction, volume_expansion,
        pattern_length, swing_count, symmetry_score,
        next_candle_up, next_5_up, breakout_happened, pattern_completed
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["pattern_name"],
        data["symbol"],
        data["timeframe"],
        safe_array(data["shape_vector"]),
        safe_float(data["return_total"]),
        safe_float(data["volatility"]),
        safe_float(data["slope"]),
        safe_float(data["max_drawdown"]),
        safe_float(data["range_ratio"]),
        safe_float(data["body_ratio"]),
        safe_float(data["prev_trend_slope"]),
        safe_float(data["trend_strength"]),
        safe_float(data["vwap_distance"]),
        safe_float(data["range_compression"]),
        safe_float(data["volume_contraction"]),
        safe_float(data["volume_expansion"]),
        safe_int(data["pattern_length"]),
        safe_int(data["swing_count"]),
        safe_float(data["symmetry_score"]),
        safe_bool(data["next_candle_up"]),
        safe_bool(data["next_5_up"]),
        safe_bool(data["breakout_happened"]),
        safe_bool(data["pattern_completed"])
    ))


# ================= MAIN =================
def run():

    print("Downloading data...")

    df = yf.download(
        SYMBOL,
        interval=INTERVAL,
        period=PERIOD,
        progress=False
    )

    if df is None or df.empty:
        print("⚠ No data returned")
        return

    df.dropna(inplace=True)

    window = 40          # pattern size
    future = 5           # label future candles
    step = 1             # slide speed (IMPORTANT)

    inserted = 0

    for i in range(0, len(df) - window - future, step):

        pattern_df = df.iloc[i:i+window]
        future_df = df.iloc[i+window:i+window+future]

        shape = build_shape_vector(pattern_df)

        if shape is None:
            continue

        feats = build_features(pattern_df)
        labels = build_labels(pattern_df, future_df)

        record = {
            "pattern_name": "UNLABELED",
            "symbol": SYMBOL,
            "timeframe": INTERVAL,
            "shape_vector": shape,
            **feats,
            "next_candle_up": labels[0],
            "next_5_up": labels[1],
            "breakout_happened": labels[2],
            "pattern_completed": labels[3]
        }

        insert_pattern_fingerprint(cur, record)
        inserted += 1

        if inserted % 200 == 0:
            conn.commit()
            print(f"Inserted {inserted}")

    conn.commit()
    print(f"✅ Done. Inserted {inserted} fingerprints")


# ================= RUN =================

