from growwapi import GrowwAPI
import time

# Groww API Credentials (Replace with your actual credentials)
API_AUTH_TOKEN = "your_token"

# Initialize Groww API
groww = GrowwAPI(API_AUTH_TOKEN)

# you can give time programatically.
end_time_in_millis = int(time.time() * 1000)  # epoch time in milliseconds
start_time_in_millis = end_time_in_millis - (24 * 60 * 60 * 1000)  # last 24 hours

# OR

# you can give start time and end time in yyyy-MM-dd HH:mm:ss format.
end_time = "2025-02-27 14:00:00"
start_time = "2025-02-27 10:00:00"

historical_data_response = groww.get_historical_candle_data(
    trading_symbol="RELIANCE",
    exchange=groww.EXCHANGE_NSE,
    segment=groww.SEGMENT_CASH,
    start_time=start_time,
    end_time=end_time,
    interval_in_minutes=5  # Optional: Interval in minutes for the candle data
)
print(historical_data_response)