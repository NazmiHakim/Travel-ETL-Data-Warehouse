import psycopg2
import random
import os
from faker import Faker
from datetime import datetime
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

fake = Faker()

# ---------------------------------------------------------------------------
# Known valid airline routes for synthetic booking generation.
# Tuple format: (carrier_code, origin_airport_id, dest_airport_id)
# ---------------------------------------------------------------------------
VALID_ROUTES = [
    ("DL", 11433, 13303),
    ("DL", 14869, 12478),
    ("DL", 14057, 14869),
    ("DL", 15016, 11433),
    ("DL", 11193, 12892),
    ("DL", 10397, 15016),
    ("DL", 12266, 10397),
    ("AA", 12892, 10397),
    ("UA", 10397, 12892),
    ("WN", 13303, 12478),
]


def generate_dummy_bookings(num_records: int) -> list[tuple]:
    """
    Generates a list of synthetic booking records.

    Each record contains a random booking date (between 2024-01-01 and 2025-11-01),
    a random user ID, a randomly selected route, passenger count, and total revenue.

    Args:
        num_records: Number of booking records to generate.

    Returns:
        A list of tuples ready for bulk INSERT into the Bookings table.
    """
    records = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 11, 1)

    for _ in range(num_records):
        carrier, origin, dest = random.choice(VALID_ROUTES)
        passengers = random.randint(1, 4)
        base_fare = random.uniform(150.0, 600.0)
        revenue = round(passengers * base_fare, 2)
        user_id = random.randint(1001, 5000)
        booking_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        records.append((booking_date, user_id, carrier, origin, dest, passengers, revenue))

    return records


def insert_to_db(records: list[tuple]) -> None:
    """
    Bulk-inserts generated booking records into the OLTP Bookings table.

    Rolls back and prints an error if the INSERT fails for any reason.
    Closes the connection in a finally block to prevent connection leaks.

    Args:
        records: List of booking tuples returned by generate_dummy_bookings().
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
        cur = conn.cursor()

        sql = """
        INSERT INTO Bookings
        (booking_date, user_id, flight_carrier_code, flight_origin_id, flight_dest_id, passengers, revenue)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(sql, records)
        conn.commit()
        print(f"SUCCESS: {len(records)} dummy booking records inserted into the Bookings table.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"ERROR: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    NUM_RECORDS_TO_GENERATE = 5000

    print(f"Generating {NUM_RECORDS_TO_GENERATE} synthetic booking records...")
    dummy_data = generate_dummy_bookings(NUM_RECORDS_TO_GENERATE)

    print(f"Inserting records into db_oltp ({DB_HOST}:{DB_PORT}/{DB_NAME})...")
    insert_to_db(dummy_data)