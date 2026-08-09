import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Database Configuration (loaded from .env)
# ---------------------------------------------------------------------------
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")
DB_NAME: str = os.getenv("DB_NAME_OLTP", os.getenv("DB_NAME", "db_oltp"))
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASS: str = os.getenv("DB_PASS", "your_postgres_password")

# Bronze output path
BRONZE_PATH = os.path.join("data", "bronze")
os.makedirs(BRONZE_PATH, exist_ok=True)
OUTPUT_FILE = os.path.join(BRONZE_PATH, "bronze_bookings.csv")


def extract_oltp_data() -> None:
    """
    Connects to the OLTP database (db_oltp), extracts all records from the
    Bookings table, and writes them as a raw CSV to the Bronze landing zone.

    The output CSV is saved at: data/bronze/bronze_bookings.csv
    This file serves as the Bronze-layer input for transform_and_load.py.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )
        print(f"Connected to OLTP database: {DB_HOST}:{DB_PORT}/{DB_NAME}")

        sql_query = "SELECT * FROM Bookings;"
        df = pd.read_sql_query(sql_query, conn)

        df.to_csv(OUTPUT_FILE, index=False)
        print(f"SUCCESS: {len(df)} records extracted from 'Bookings'.")
        print(f"Bronze output saved to: {OUTPUT_FILE}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR during OLTP extraction: {error}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    extract_oltp_data()
