import psycopg2
import random
from faker import Faker
from datetime import datetime, timedelta

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "db_oltp"
DB_USER = "" 
DB_PASS = ""

fake = Faker()

valid_routes = [
    ('DL', 11433, 13303),
    ('DL', 14869, 12478),
    ('DL', 14057, 14869),
    ('DL', 15016, 11433),
    ('DL', 11193, 12892),
    ('DL', 10397, 15016),
    ('DL', 12266, 10397),
    ('AA', 12892, 10397),
    ('UA', 10397, 12892),
    ('WN', 13303, 12478)
]

def generate_dummy_bookings(num_records):
    records = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 11, 1)

    for _ in range(num_records):
        route = random.choice(valid_routes)
        carrier, origin, dest = route

        passengers = random.randint(1, 4)

        base_fare = random.uniform(150.0, 600.0)
        revenue = round(passengers * base_fare, 2)

        user_id = random.randint(1001, 5000)
        booking_date = fake.date_time_between(start_date=start_date, end_date=end_date)

        records.append((
            booking_date, user_id, carrier, origin, dest, passengers, revenue
        ))

    return records

def insert_to_db(records):
    conn = None
    try:

        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()


        sql = """
        INSERT INTO Bookings 
        (booking_date, user_id, flight_carrier_code, flight_origin_id, flight_dest_id, passengers, revenue)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cur.executemany(sql, records)

        conn.commit()

        print(f"BERHASIL: {len(records)} data dummy berhasil dimasukkan ke tabel Bookings.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        if conn:
            conn.rollback() 
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    NUM_RECORDS_TO_GENERATE = 5000  

    print(f"Mulai membuat {NUM_RECORDS_TO_GENERATE} data dummy...")
    dummy_data = generate_dummy_bookings(NUM_RECORDS_TO_GENERATE)

    print("Memasukkan data ke database db_oltp...")
    insert_to_db(dummy_data)