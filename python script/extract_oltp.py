import psycopg2
import pandas as pd
import os

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "db_oltp"
DB_USER = "" 
DB_PASS = ""

BRONZE_PATH = os.path.join("data", "bronze")
os.makedirs(BRONZE_PATH, exist_ok=True)
OUTPUT_FILE = os.path.join(BRONZE_PATH, "bronze_bookings.csv")

def extract_oltp_data():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        print("Terhubung ke db_oltp...")

        sql_query = "SELECT * FROM Bookings;"

        df = pd.read_sql_query(sql_query, conn)

        df.to_csv(OUTPUT_FILE, index=False)

        print(f"BERHASIL: {len(df)} data diekstrak dari 'Bookings'.")
        print(f"Data mentah disimpan di: {OUTPUT_FILE}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error saat ekstraksi: {error}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    extract_oltp_data()