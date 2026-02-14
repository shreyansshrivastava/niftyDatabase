import psycopg2
from databaseFile.databaseConnection import conn

cur = conn.cursor()


def insertDataDay(candle_date, interval,close_price, image):
    print("before insert")
    cur.execute(
        "INSERT INTO nifty_day_images (candle_date, interval,close_price, image) VALUES (%s, %s, %s,%s)",
        (candle_date, interval,close_price, psycopg2.Binary(image))
    )

    print("Inserted DB nifty_day_images")
    print("Candlestick image stored in database")
def insertNifty50DataDay(candle_date, interval,close_price, image):
    print("before insert")
    cur.execute(
        "INSERT INTO nifty_day_images (candle_date, interval,close_price, image) VALUES (%s, %s, %s,%s)",
        (candle_date, interval,close_price, psycopg2.Binary(image))
    )

    print("Inserted DB nifty_day_images")
    print("Candlestick image stored in database")
def insertData15min(candle_date, interval,close_price, image):
    cur.execute(
        "INSERT INTO nifty_15min_images (candle_date, interval,close_price, image) VALUES (%s, %s, %s,%s)",
        (candle_date, interval,close_price, psycopg2.Binary(image))
    )
    print("Inserted DB nifty_15min_images")
    print("Candlestick image stored in database")

def insertData2hr(candle_date, interval,close_price, image):
    cur.execute(
        "INSERT INTO nifty_1hr_images (candle_date, interval,close_price, image) VALUES (%s, %s, %s,%s)",
        (candle_date, interval,close_price, psycopg2.Binary(image))
    )
    print("Inserted DB nifty_1hr_images")
    print("Candlestick image stored in database")