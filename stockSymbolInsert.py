import psycopg2
import csv
from databaseFile.databaseConnection import conn
# 1. Connection settings

csv_file_path = r'C:\Users\shrey\OneDrive\Desktop\NSE_STOCK_Data.csv'

try:
    # 2. Connect to the database
    cur = conn.cursor()

    # 3. Open and read the CSV
    with open(csv_file_path, mode='r') as f:
        reader = csv.DictReader(f)

        # SQL statement with placeholders (%s) for safety
        insert_query = """
            INSERT INTO company_listings (
                id, symbol, company_name, series, date_of_listing, 
                paid_up_value, market_lot, isin_number, face_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for row in reader:
            # Prepare data tuple based on CSV headers
            data = (
                row['id'], row['symbol'], row['company_name'], row['series'],
                row['date_of_listing'], row['paid_up_value'], row['market_lot'],
                row['isin_number'], row['face_value']
            )

            # 4. Execute the insert
            cur.execute(insert_query, data)

    # 5. Commit all changes
    conn.commit()
    print("Data inserted successfully using INSERT statements.")

except Exception as e:
    print(f"Error: {e}")
    if conn: conn.rollback()

finally:
    if cur: cur.close()
    if conn: conn.close()
