import os
import random
import pandas as pd
from datetime import datetime, timedelta

# Define base paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(DATA_DIR, exist_ok=True)

AIRPORTS_FILE = os.path.join(DATA_DIR, "airports.csv")
FLIGHTS_FILE = os.path.join(DATA_DIR, "flights.csv")

# 1. Airport reference dataset (matching airport_ids used in OLTP dummy generation)
AIRPORTS_DATA = [
    {"airport_id": 10397, "city": "Atlanta", "state": "GA", "name": "Hartsfield-Jackson Atlanta International Airport"},
    {"airport_id": 11433, "city": "Detroit", "state": "MI", "name": "Detroit Metropolitan Wayne County Airport"},
    {"airport_id": 13303, "city": "Nashville", "state": "TN", "name": "Nashville International Airport"},
    {"airport_id": 14869, "city": "Salt Lake City", "state": "UT", "name": "Salt Lake City International Airport"},
    {"airport_id": 12478, "city": "New York", "state": "NY", "name": "John F. Kennedy International Airport"},
    {"airport_id": 14057, "city": "Portland", "state": "OR", "name": "Portland International Airport"},
    {"airport_id": 15016, "city": "St. Louis", "state": "MO", "name": "St. Louis Lambert International Airport"},
    {"airport_id": 11193, "city": "Cincinnati", "state": "OH", "name": "Cincinnati/Northern Kentucky International Airport"},
    {"airport_id": 12892, "city": "Los Angeles", "state": "CA", "name": "Los Angeles International Airport"},
    {"airport_id": 12266, "city": "Houston", "state": "TX", "name": "George Bush Intercontinental Airport"},
    {"airport_id": 13930, "city": "Chicago", "state": "IL", "name": "Chicago O'Hare International Airport"},
    {"airport_id": 11298, "city": "Dallas", "state": "TX", "name": "Dallas/Fort Worth International Airport"},
    {"airport_id": 14771, "city": "San Francisco", "state": "CA", "name": "San Francisco International Airport"},
    {"airport_id": 13204, "city": "Orlando", "state": "FL", "name": "Orlando International Airport"},
    {"airport_id": 14107, "city": "Phoenix", "state": "AZ", "name": "Phoenix Sky Harbor International Airport"}
]

# 2. Carrier codes supported by the ETL pipeline
CARRIERS = ["DL", "AA", "UA", "WN", "AS", "B6", "F9", "NK"]

def generate_airports_csv():
    print(f"Generating {AIRPORTS_FILE}...")
    df_airports = pd.DataFrame(AIRPORTS_DATA)
    df_airports.to_csv(AIRPORTS_FILE, index=False)
    print(f"SUCCESS: Created {AIRPORTS_FILE} with {len(df_airports)} airport records.")

def generate_flights_csv(num_records=10000):
    print(f"Generating {FLIGHTS_FILE} with {num_records} records...")
    
    airport_ids = [a["airport_id"] for a in AIRPORTS_DATA]
    start_date = datetime(2024, 1, 1)
    
    records = []
    for _ in range(num_records):
        origin_id = random.choice(airport_ids)
        dest_id = random.choice([aid for aid in airport_ids if aid != origin_id])
        carrier = random.choice(CARRIERS)
        
        random_days = random.randint(0, 700)
        flight_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
        
        dep_delay = random.randint(-15, 120)
        arr_delay = dep_delay + random.randint(-10, 20)
        distance = random.randint(200, 3000)
        cancelled = 1 if random.random() < 0.02 else 0
        
        records.append({
            "FlightDate": flight_date,
            "Carrier": carrier,
            "OriginAirportID": origin_id,
            "DestAirportID": dest_id,
            "DepDelay": dep_delay,
            "ArrDelay": arr_delay,
            "Distance": distance,
            "Cancelled": cancelled
        })
        
    df_flights = pd.DataFrame(records)
    df_flights.to_csv(FLIGHTS_FILE, index=False)
    print(f"SUCCESS: Created {FLIGHTS_FILE} with {len(df_flights)} flight records.")

BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)
REVIEWS_FILE = os.path.join(BRONZE_DIR, "customer_reviews.csv")

REVIEW_TEMPLATES = [
    ("Delay", "Flight {carrier} from {origin} to {dest} on {date} was delayed by {mins} minutes. Missed my connecting flight! Very disappointing service.", 1),
    ("Delay", "Significant departure delay on {carrier}. Spent {mins} hours stuck at the gate without clear updates from the airline staff.", 2),
    ("Baggage", "Landed in {dest} with {carrier}, but my checked luggage was lost! Took 2 days to recover my bags.", 1),
    ("Baggage", "Careless handling of luggage on flight {carrier}. My suitcase was dented upon arrival.", 2),
    ("Service", "Flight attendants on {carrier} were super friendly and helpful throughout the trip to {dest}. Great experience!", 5),
    ("Service", "Poor inflight service on {carrier}. The staff ignored cabin requests and cabin comfort was substandard.", 2),
    ("Pricing", "Ticket prices for {carrier} to {dest} are way too high for basic economy with zero free baggage allowances.", 2),
    ("Pricing", "Got a fantastic deal on {carrier}! Smooth booking process, comfortable flight, and great value for money.", 5),
    ("None", "On-time departure and early arrival in {dest} with {carrier}. Smooth flight with no issues whatsoever.", 5),
    ("None", "Decent flight with {carrier}. Clean aircraft and punctual landing.", 4)
]

def generate_customer_reviews_csv(num_records=500):
    print(f"Generating {REVIEWS_FILE} with {num_records} review records...")
    start_date = datetime(2024, 1, 1)
    airport_ids = [a["airport_id"] for a in AIRPORTS_DATA]
    
    records = []
    for i in range(1, num_records + 1):
        carrier = random.choice(CARRIERS)
        origin_id = random.choice(airport_ids)
        dest_id = random.choice([aid for aid in airport_ids if aid != origin_id])
        random_days = random.randint(0, 700)
        review_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
        
        template_cat, text_template, default_score = random.choice(REVIEW_TEMPLATES)
        delay_mins = random.randint(30, 180)
        
        review_text = text_template.format(
            carrier=carrier,
            origin=origin_id,
            dest=dest_id,
            date=review_date,
            mins=delay_mins
        )
        
        records.append({
            "review_id": i,
            "review_date": review_date,
            "carrier_code": carrier,
            "origin_airport_id": origin_id,
            "dest_airport_id": dest_id,
            "raw_review_text": review_text
        })
        
    df_reviews = pd.DataFrame(records)
    df_reviews.to_csv(REVIEWS_FILE, index=False)
    print(f"SUCCESS: Created {REVIEWS_FILE} with {len(df_reviews)} raw customer review records.")

if __name__ == "__main__":
    generate_airports_csv()
    generate_flights_csv(10000)
    generate_customer_reviews_csv(500)

