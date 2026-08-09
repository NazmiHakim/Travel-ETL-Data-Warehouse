import os
import random
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(DATA_DIR, exist_ok=True)

AIRPORTS_FILE = os.path.join(DATA_DIR, "airports.csv")
FLIGHTS_FILE = os.path.join(DATA_DIR, "flights.csv")

# ---------------------------------------------------------------------------
# Airport reference dataset
# Airport IDs match VALID_ROUTES in generate_dummy_oltp.py
# ---------------------------------------------------------------------------
AIRPORTS_DATA = [
    {"airport_id": 10397, "city": "Atlanta",        "state": "GA", "name": "Hartsfield-Jackson Atlanta International Airport"},
    {"airport_id": 11433, "city": "Detroit",        "state": "MI", "name": "Detroit Metropolitan Wayne County Airport"},
    {"airport_id": 13303, "city": "Nashville",      "state": "TN", "name": "Nashville International Airport"},
    {"airport_id": 14869, "city": "Salt Lake City", "state": "UT", "name": "Salt Lake City International Airport"},
    {"airport_id": 12478, "city": "New York",       "state": "NY", "name": "John F. Kennedy International Airport"},
    {"airport_id": 14057, "city": "Portland",       "state": "OR", "name": "Portland International Airport"},
    {"airport_id": 15016, "city": "St. Louis",      "state": "MO", "name": "St. Louis Lambert International Airport"},
    {"airport_id": 11193, "city": "Cincinnati",     "state": "OH", "name": "Cincinnati/Northern Kentucky International Airport"},
    {"airport_id": 12892, "city": "Los Angeles",    "state": "CA", "name": "Los Angeles International Airport"},
    {"airport_id": 12266, "city": "Houston",        "state": "TX", "name": "George Bush Intercontinental Airport"},
    {"airport_id": 13930, "city": "Chicago",        "state": "IL", "name": "Chicago O'Hare International Airport"},
    {"airport_id": 11298, "city": "Dallas",         "state": "TX", "name": "Dallas/Fort Worth International Airport"},
    {"airport_id": 14771, "city": "San Francisco",  "state": "CA", "name": "San Francisco International Airport"},
    {"airport_id": 13204, "city": "Orlando",        "state": "FL", "name": "Orlando International Airport"},
    {"airport_id": 14107, "city": "Phoenix",        "state": "AZ", "name": "Phoenix Sky Harbor International Airport"},
]

# ---------------------------------------------------------------------------
# All 8 carrier codes supported by the ETL pipeline
# ---------------------------------------------------------------------------
CARRIERS = ["DL", "AA", "UA", "WN", "AS", "B6", "F9", "NK"]

# ---------------------------------------------------------------------------
# Carrier-specific delay profile (minutes)
# Legacy carriers generally have better on-time performance than budget carriers
# ---------------------------------------------------------------------------
CARRIER_DELAY_PROFILE: dict[str, tuple[int, int]] = {
    "DL": (-10, 45),   # Delta -- best OTP
    "AS": (-10, 50),   # Alaska -- second best OTP
    "UA": (-10, 60),   # United
    "AA": (-10, 65),   # American
    "B6": (-10, 75),   # JetBlue
    "WN": (-5,  80),   # Southwest
    "F9": (-5,  100),  # Frontier
    "NK": (-5,  120),  # Spirit -- worst OTP
}


def generate_airports_csv() -> None:
    """Writes the airport reference dataset to airports.csv."""
    print(f"Generating airports.csv...")
    df = pd.DataFrame(AIRPORTS_DATA)
    df.to_csv(AIRPORTS_FILE, index=False)
    print(f"SUCCESS: {AIRPORTS_FILE} -- {len(df)} airports.")


def generate_flights_csv(num_records: int = 50_000) -> None:
    """
    Generates operational flight records with carrier-specific delay distributions.

    Each record represents a single flight with a departure date, carrier, route,
    departure/arrival delay in minutes, distance, and cancellation flag.
    Carrier delay profiles ensure realistic OTP differentiation between airlines
    (e.g., Spirit/Frontier have higher delays than Delta/Alaska).

    Args:
        num_records: Number of synthetic flight records to generate.
    """
    print(f"Generating flights.csv with {num_records:,} records...")

    airport_ids = [a["airport_id"] for a in AIRPORTS_DATA]
    start_date = datetime(2024, 1, 1)

    records = []
    for _ in range(num_records):
        origin_id = random.choice(airport_ids)
        dest_id = random.choice([aid for aid in airport_ids if aid != origin_id])
        carrier = random.choice(CARRIERS)

        random_days = random.randint(0, 700)
        flight_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

        lo, hi = CARRIER_DELAY_PROFILE[carrier]
        dep_delay = random.randint(lo, hi)
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
            "Cancelled": cancelled,
        })

    df = pd.DataFrame(records)
    df.to_csv(FLIGHTS_FILE, index=False)
    print(f"SUCCESS: {FLIGHTS_FILE} -- {len(df):,} flight records.")


# ---------------------------------------------------------------------------
# Customer Review Templates
# Each entry: (complaint_category, text_template, base_satisfaction_score)
# Templates are designed to produce realistic, carrier-contextualized reviews
# that the AI enrichment script can classify correctly.
# ---------------------------------------------------------------------------
REVIEW_TEMPLATES = [
    # Delay complaints
    ("Delay", "Flight {carrier} from {origin} to {dest} on {date} was delayed by {mins} minutes. Missed my connecting flight. Very disappointing.", 1),
    ("Delay", "Significant departure delay on {carrier}. Spent {mins} minutes stuck at the gate with no updates from staff.", 2),
    ("Delay", "{carrier} flight delayed again -- {mins} minutes this time. Third delay this month. Completely unreliable.", 1),
    ("Delay", "Minor {mins}-minute delay on {carrier} but staff kept us informed throughout. Not ideal but acceptable.", 3),

    # Baggage complaints
    ("Baggage", "Landed in {dest} with {carrier} but my checked luggage was lost. Took 2 days to recover my bags.", 1),
    ("Baggage", "Careless handling on {carrier} -- my suitcase arrived damaged with a broken wheel.", 2),
    ("Baggage", "Lost baggage claim with {carrier} was incredibly slow. Waited 90 minutes at carousel before reporting the loss.", 2),
    ("Baggage", "Bags arrived intact and on-time with {carrier}. No issues at all with baggage handling.", 5),

    # Service reviews
    ("Service", "Flight attendants on {carrier} were super friendly and helpful on the trip to {dest}. Great experience!", 5),
    ("Service", "Poor inflight service on {carrier}. Staff ignored cabin requests and the cabin was dirty.", 2),
    ("Service", "{carrier} crew went above and beyond during my flight. Exceptional hospitality from start to finish.", 5),
    ("Service", "Average service on {carrier}. Nothing special but nothing terrible either.", 3),
    ("Service", "The {carrier} boarding process was chaotic and disorganized. Staff were unhelpful when asked for assistance.", 2),

    # Pricing reviews
    ("Pricing", "Ticket prices for {carrier} to {dest} are way too high for basic economy with no baggage allowance.", 2),
    ("Pricing", "Got a fantastic deal on {carrier}! Smooth booking, comfortable seat, great value for money.", 5),
    ("Pricing", "{carrier} charges extra for everything. Seat selection, baggage, drinks -- the base fare is misleading.", 1),
    ("Pricing", "Fair price for what you get with {carrier}. Budget airline but reasonable comfort.", 3),

    # Positive / neutral overall reviews
    ("None", "On-time departure and early arrival in {dest} with {carrier}. Smooth flight with no issues.", 5),
    ("None", "Decent flight with {carrier}. Clean aircraft and punctual landing.", 4),
    ("None", "Comfortable seats and good legroom on {carrier}. Would fly again.", 5),
    ("None", "{carrier} delivered exactly what I expected -- a reliable flight from {origin} to {dest}.", 4),
    ("None", "Nothing exceptional about {carrier} but a solid, reliable flight overall.", 3),
]

BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)
REVIEWS_FILE = os.path.join(BRONZE_DIR, "customer_reviews.csv")


def generate_customer_reviews_csv(num_records: int = 2_000) -> None:
    """
    Generates raw customer review records for all 8 airline carriers.

    Reviews are randomized across all carriers and airports, with varied
    complaint categories and satisfaction levels to ensure the AI enrichment
    script and agent can distinguish performance across airlines.

    Args:
        num_records: Number of raw review records to generate.
    """
    print(f"Generating customer_reviews.csv with {num_records:,} records...")
    start_date = datetime(2024, 1, 1)
    airport_ids = [a["airport_id"] for a in AIRPORTS_DATA]
    city_map = {a["airport_id"]: a["city"] for a in AIRPORTS_DATA}

    records = []
    for i in range(1, num_records + 1):
        carrier = random.choice(CARRIERS)
        origin_id = random.choice(airport_ids)
        dest_id = random.choice([aid for aid in airport_ids if aid != origin_id])
        random_days = random.randint(0, 700)
        review_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

        template_cat, text_template, _ = random.choice(REVIEW_TEMPLATES)
        delay_mins = random.randint(30, 180)

        review_text = text_template.format(
            carrier=carrier,
            origin=city_map.get(origin_id, str(origin_id)),
            dest=city_map.get(dest_id, str(dest_id)),
            date=review_date,
            mins=delay_mins,
        )

        records.append({
            "review_id": i,
            "review_date": review_date,
            "carrier_code": carrier,
            "origin_airport_id": origin_id,
            "dest_airport_id": dest_id,
            "raw_review_text": review_text,
        })

    df = pd.DataFrame(records)
    df.to_csv(REVIEWS_FILE, index=False)
    print(f"SUCCESS: {REVIEWS_FILE} -- {len(df):,} customer review records.")


if __name__ == "__main__":
    generate_airports_csv()
    generate_flights_csv(num_records=50_000)
    generate_customer_reviews_csv(num_records=2_000)

