import os
import time
import pandas as pd
from sqlalchemy import create_engine, text, pool as sa_pool
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "db_dwh")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "your_postgres_password")

# ---------------------------------------------------------------------------
# Engine Singleton
# Best practice: one engine per process, shared across all calls.
# pool_pre_ping=True  → tests connection health before use (prevents zombie connections)
# pool_recycle=180    → recycles connections every 3 min (avoids idle-timeout kills)
# pool_size=5         → keep 5 base connections warm
# max_overflow=10     → allow up to 10 extra under burst load
# ---------------------------------------------------------------------------
_engine = None

def get_engine():
    """Returns the module-level SQLAlchemy engine singleton."""
    global _engine
    if _engine is None:
        db_url = (
            f"postgresql://{DB_USER}:{DB_PASS}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        _engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_recycle=180,
            pool_size=5,
            max_overflow=10,
        )
    return _engine


def get_schema_info() -> str:
    """Returns the static DWH schema as a text block for LLM context injection."""
    return """=== POSTGRESQL DATA WAREHOUSE SCHEMA (db_dwh) ===

1. Table: dim_airport
   - airport_id_key (SERIAL PRIMARY KEY)
   - airport_id (INT UNIQUE)
   - city (VARCHAR) -- e.g. 'Atlanta', 'Chicago'
   - state (VARCHAR)
   - name (VARCHAR) -- Full airport name

2. Table: dim_airline
   - airline_key (SERIAL PRIMARY KEY)
   - carrier_code (VARCHAR UNIQUE) -- 2-letter IATA code e.g. 'DL', 'AA'
   - airline_name (VARCHAR)

3. Table: dim_date
   - date_key (SERIAL PRIMARY KEY)
   - full_date (DATE UNIQUE)
   - day_of_week (INT)   -- 1 (Mon) to 7 (Sun)
   - day_of_month (INT)
   - month (INT)         -- 1 to 12
   - quarter (INT)       -- 1 to 4
   - year (INT)

4. Table: fact_flights
   - flight_key (SERIAL PRIMARY KEY)
   - date_key   → dim_date
   - airline_key → dim_airline
   - origin_airport_key → dim_airport
   - dest_airport_key   → dim_airport
   - departure_delay (INT) -- minutes late (positive = delayed)
   - arrival_delay   (INT)
   - total_passengers (INT)
   - total_revenue    (DECIMAL)

5. Table: fact_customer_feedback  [AI-enriched]
   - feedback_key (SERIAL PRIMARY KEY)
   - date_key    → dim_date
   - airline_key → dim_airline
   - sentiment          (VARCHAR) -- 'Positive' | 'Neutral' | 'Negative'
   - complaint_category (VARCHAR) -- 'Delay' | 'Baggage' | 'Service' | 'Pricing' | 'None'
   - satisfaction_score (INT)     -- 1 to 5
   - review_text        (TEXT)

=== JOIN RULES ===
- fact_flights.date_key          = dim_date.date_key
- fact_flights.airline_key       = dim_airline.airline_key
- fact_flights.origin_airport_key = dim_airport.airport_id_key  (alias da_orig)
- fact_flights.dest_airport_key   = dim_airport.airport_id_key  (alias da_dest)
- fact_customer_feedback.airline_key = dim_airline.airline_key
- fact_customer_feedback.date_key    = dim_date.date_key
"""


def get_db_status() -> dict:
    """
    Returns a dict with DB connectivity status and live row counts per table.
    Used by the Streamlit sidebar to show a health dashboard.
    """
    tables = [
        "fact_flights",
        "fact_customer_feedback",
        "dim_airline",
        "dim_airport",
        "dim_date",
    ]
    result = {"connected": False, "tables": {}, "error": ""}
    try:
        engine = get_engine()
        with engine.connect() as conn:
            for tbl in tables:
                row = conn.execute(
                    text(f"SELECT COUNT(*) FROM {tbl}")
                ).scalar()
                result["tables"][tbl] = row
        result["connected"] = True
    except Exception as e:
        result["error"] = str(e)
    return result


def is_db_alive() -> tuple[bool, str]:
    """Quick ping — returns (True, '') or (False, error_message)."""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, ""
    except Exception as e:
        return False, str(e)


def execute_sql(sql_query: str) -> tuple[bool, object, float]:
    """
    Executes a read-only SELECT query against db_dwh.

    Returns:
        (success: bool, result_df_or_error: DataFrame | str, elapsed_seconds: float)
    """
    t0 = time.perf_counter()

    # DB connectivity pre-check
    alive, conn_err = is_db_alive()
    if not alive:
        return False, (
            f"🔴 Database Connection Error: PostgreSQL is not reachable at "
            f"{DB_HOST}:{DB_PORT}. Start the PostgreSQL service and retry.\n"
            f"Detail: {conn_err}"
        ), 0.0

    # Security: SELECT / WITH only
    clean = sql_query.strip()
    if not clean.upper().startswith("SELECT") and not clean.upper().startswith("WITH"):
        return False, "Security Error: Only SELECT queries are permitted.", 0.0

    forbidden = ["DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT", "ALTER", "CREATE"]
    upper = f" {clean.upper()} "
    for kw in forbidden:
        if f" {kw} " in upper:
            return False, f"Security Error: Forbidden keyword '{kw}' detected.", 0.0

    try:
        with get_engine().connect() as conn:
            df = pd.read_sql(text(clean), conn)
        elapsed = time.perf_counter() - t0
        return True, df, elapsed
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return False, str(e), elapsed
