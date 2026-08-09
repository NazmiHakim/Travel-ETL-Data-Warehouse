import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Database Configuration (loaded from .env)
# ---------------------------------------------------------------------------
BASE_DATA_PATH = "data"

DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: str = os.getenv("DB_PORT", "5432")
DB_NAME: str = os.getenv("DB_NAME", "db_dwh")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASS: str = os.getenv("DB_PASS", "your_postgres_password")

AIRPORTS_FILE = os.path.join(BASE_DATA_PATH, "airports.csv")
FLIGHTS_FILE = os.path.join(BASE_DATA_PATH, "flights.csv")
BOOKINGS_FILE = os.path.join(BASE_DATA_PATH, "bronze", "bronze_bookings.csv")

# ---------------------------------------------------------------------------
# Pre-flight file check -- abort early if any required input file is missing.
# ---------------------------------------------------------------------------
print("Verifying input file paths...")
all_files_found = True
for name, path in {"Airports": AIRPORTS_FILE, "Flights": FLIGHTS_FILE, "Bookings (Bronze)": BOOKINGS_FILE}.items():
    if not os.path.exists(path):
        print(f"ERROR: '{name}' not found at: {path}")
        all_files_found = False
    else:
        print(f"  OK: {name} -> {path}")

if not all_files_found:
    print("\nPipeline aborted. Ensure all source files exist before running.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
try:
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    print(f"\nConnected to Data Warehouse: {DB_HOST}:{DB_PORT}/{DB_NAME}")
except Exception as e:
    print(f"ERROR: Failed to connect to '{DB_NAME}': {e}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Airline carrier code -> full name lookup map
# ---------------------------------------------------------------------------
CARRIER_MAP: dict[str, str] = {
    "DL": "Delta Air Lines",
    "AA": "American Airlines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "AS": "Alaska Airlines",
    "B6": "JetBlue Airways",
    "F9": "Frontier Airlines",
    "NK": "Spirit Airlines",
}


def clean_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Applies basic Silver-layer cleansing to a DataFrame:
    - Removes exact duplicate rows.
    - Fills missing 'state' values with 'N/A'.

    Args:
        df: Raw input DataFrame.
        table_name: Name used in log messages.

    Returns:
        Cleaned DataFrame.
    """
    print(f"  Cleaning data for {table_name}...")
    df = df.drop_duplicates()
    if "state" in df.columns:
        df["state"] = df["state"].fillna("N/A")
    print(f"  Cleaned -- {len(df)} records remaining.")
    return df


def load_dim_airport() -> None:
    """
    Reads airports.csv, applies Silver-layer cleansing, and loads
    the result into the Dim_Airport dimension table.

    Note: Silently skips UniqueViolation errors to allow safe re-runs.
    """
    try:
        print("\n--- Loading Dim_Airport ---")
        df_airports = pd.read_csv(AIRPORTS_FILE)
        df_clean = clean_data(df_airports, "Dim_Airport")
        df_to_load = df_clean[["airport_id", "city", "state", "name"]]
        df_to_load.to_sql("dim_airport", engine, if_exists="append", index=False)
        print("SUCCESS: Dim_Airport loaded.")
    except Exception as e:
        if "UniqueViolation" in str(e):
            print("INFO: Dim_Airport partially exists (UniqueViolation). Continuing...")
        else:
            print(f"ERROR loading Dim_Airport: {e}")


def load_dim_airline() -> None:
    """
    Derives the carrier code list from both flights.csv and bronze_bookings.csv,
    maps codes to full airline names using CARRIER_MAP, and loads into Dim_Airline.

    Unknown carrier codes are stored as '<code> (Unknown)'.
    Note: Silently skips UniqueViolation errors to allow safe re-runs.
    """
    try:
        print("\n--- Loading Dim_Airline ---")
        df_flights = pd.read_csv(FLIGHTS_FILE)
        df_bookings = pd.read_csv(BOOKINGS_FILE)

        # Merge unique carrier codes from both source files
        carriers_flights = df_flights["Carrier"].unique()
        carriers_bookings = df_bookings["flight_carrier_code"].unique()
        all_carriers = pd.Series(list(carriers_flights) + list(carriers_bookings)).unique()

        df_airlines = pd.DataFrame(all_carriers, columns=["carrier_code"])
        df_airlines["airline_name"] = df_airlines["carrier_code"].map(CARRIER_MAP)
        df_airlines["airline_name"] = df_airlines["airline_name"].fillna(
            df_airlines["carrier_code"] + " (Unknown)"
        )

        df_airlines.to_sql("dim_airline", engine, if_exists="append", index=False)
        print("SUCCESS: Dim_Airline loaded.")
    except Exception as e:
        if "UniqueViolation" in str(e):
            print("INFO: Dim_Airline partially exists (UniqueViolation). Continuing...")
        else:
            print(f"ERROR loading Dim_Airline: {e}")


def load_fact_flights() -> None:
    """
    Aggregates booking records from bronze_bookings.csv and operational delay
    metrics from flights.csv, performs surrogate key lookups against all dimension
    tables, and bulk-loads the result into Fact_Flights.

    Aggregation logic:
    - Bookings are grouped by (date, carrier, origin, dest) -> total_passengers, total_revenue
    - Flights are grouped by (date, carrier, origin, dest) -> mean departure_delay, arrival_delay
    - The two aggregates are LEFT JOINed so flights without delay data are preserved (delay = 0).

    Note: Requires Dim_Airline to be populated before running.
    """
    try:
        print("\n--- Loading Fact_Flights ---")

        df_bookings = pd.read_csv(BOOKINGS_FILE)

        # Load dimension keys for surrogate key lookup
        print("  Reading dimension keys from the Data Warehouse...")
        dim_date = pd.read_sql("SELECT date_key, full_date FROM dim_date", engine)
        dim_airline = pd.read_sql("SELECT airline_key, carrier_code FROM dim_airline", engine)
        dim_airport = pd.read_sql("SELECT airport_id_key, airport_id FROM dim_airport", engine)

        if dim_airline.empty:
            print("ERROR: dim_airline is empty. Run load_dim_airline() first.")
            return

        # Aggregate bookings by (date, carrier, route)
        print("  Aggregating booking records by day and route...")
        df_bookings["booking_date"] = pd.to_datetime(df_bookings["booking_date"])
        df_bookings["date_only"] = df_bookings["booking_date"].dt.date
        dim_date["full_date"] = pd.to_datetime(dim_date["full_date"]).dt.date

        df_agg = df_bookings.groupby(
            ["date_only", "flight_carrier_code", "flight_origin_id", "flight_dest_id"]
        ).agg(
            total_passengers=("passengers", "sum"),
            total_revenue=("revenue", "sum"),
        ).reset_index()

        # Aggregate mean delay metrics from operational flights data
        print("  Reading operational delay data from flights.csv...")
        df_flights = pd.read_csv(FLIGHTS_FILE)
        df_flights["FlightDate"] = pd.to_datetime(df_flights["FlightDate"]).dt.date
        df_flights_agg = df_flights.groupby(
            ["FlightDate", "Carrier", "OriginAirportID", "DestAirportID"]
        ).agg(
            departure_delay=("DepDelay", "mean"),
            arrival_delay=("ArrDelay", "mean"),
        ).reset_index()

        # Merge bookings with delay data (left join preserves all booking rows)
        df_agg = pd.merge(
            df_agg, df_flights_agg,
            left_on=["date_only", "flight_carrier_code", "flight_origin_id", "flight_dest_id"],
            right_on=["FlightDate", "Carrier", "OriginAirportID", "DestAirportID"],
            how="left",
        )
        df_agg["departure_delay"] = df_agg["departure_delay"].fillna(0).round().astype(int)
        df_agg["arrival_delay"] = df_agg["arrival_delay"].fillna(0).round().astype(int)

        # Surrogate key lookups -- swap business keys for DWH surrogate keys
        print("  Performing surrogate key lookups (business keys -> DWH keys)...")
        df_fact = pd.merge(df_agg, dim_date, left_on="date_only", right_on="full_date", how="inner")
        df_fact = pd.merge(df_fact, dim_airline, left_on="flight_carrier_code", right_on="carrier_code", how="inner")

        dim_airport_origin = dim_airport.rename(
            columns={"airport_id_key": "origin_airport_key", "airport_id": "origin_id_lookup"}
        )
        df_fact = pd.merge(df_fact, dim_airport_origin, left_on="flight_origin_id", right_on="origin_id_lookup", how="inner")

        dim_airport_dest = dim_airport.rename(
            columns={"airport_id_key": "dest_airport_key", "airport_id": "dest_id_lookup"}
        )
        df_fact = pd.merge(df_fact, dim_airport_dest, left_on="flight_dest_id", right_on="dest_id_lookup", how="inner")

        final_columns = [
            "date_key", "airline_key", "origin_airport_key", "dest_airport_key",
            "departure_delay", "arrival_delay", "total_passengers", "total_revenue",
        ]
        df_final = df_fact[final_columns]

        print(f"  Loading {len(df_final)} aggregated records into Fact_Flights...")
        df_final.to_sql("fact_flights", engine, if_exists="append", index=False)
        print("SUCCESS: Fact_Flights loaded.")

    except Exception as e:
        print(f"ERROR loading Fact_Flights: {e}")


if __name__ == "__main__":
    # ---------------------------------------------------------------------------
    # Truncate existing DWH data before a full reload.
    # Note: This is a destructive TRUNCATE + INSERT pattern (not an upsert).
    # Safe for development and demo runs; not suitable for incremental production loads.
    # ---------------------------------------------------------------------------
    try:
        with engine.connect() as conn:
            conn.begin()
            conn.execute(text("TRUNCATE TABLE fact_flights RESTART IDENTITY;"))
            conn.execute(text("TRUNCATE TABLE dim_airline RESTART IDENTITY CASCADE;"))
            conn.execute(text("TRUNCATE TABLE dim_airport RESTART IDENTITY CASCADE;"))
            conn.commit()
            print("\nExisting DWH tables truncated successfully.")
    except Exception as e:
        print(f"\nWARNING: TRUNCATE failed ({e}). Tables may already be empty -- continuing...")

    load_dim_airport()
    load_dim_airline()
    load_fact_flights()

    print("\n--- ETL Transform & Load completed successfully ---")
