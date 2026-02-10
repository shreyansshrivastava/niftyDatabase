import psycopg2

conn = psycopg2.connect(
    database = "postgres",
    user = "postgres",
    host = "localhost",
    port = 5432,
    password = "password"
)

# CREATE TABLE similarity_results (
#     trade_date DATE,
#     timeframe VARCHAR(10),
#     similarity FLOAT,
#     matched_date DATE,
#     next_day_return FLOAT
# );