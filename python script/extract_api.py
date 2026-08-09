from amadeus import Client, ResponseError
import json
import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Amadeus API Credentials (loaded from .env)
# ---------------------------------------------------------------------------
AMADEUS_KEY: str = os.getenv("AMADEUS_KEY", "your_amadeus_key_here")
AMADEUS_SECRET: str = os.getenv("AMADEUS_SECRET", "your_amadeus_secret_here")

# Bronze output path for raw API JSON files
BRONZE_PATH = os.path.join("data", "bronze")
os.makedirs(BRONZE_PATH, exist_ok=True)


def get_amadeus_client() -> Client | None:
    """
    Initializes and returns an authenticated Amadeus API client.

    Returns:
        An authenticated Amadeus Client instance, or None if authentication fails.
    """
    try:
        amadeus = Client(client_id=AMADEUS_KEY, client_secret=AMADEUS_SECRET)
        print("Amadeus API authentication successful.")
        return amadeus
    except ResponseError as error:
        print(f"ERROR: Amadeus authentication failed: {error}")
        return None


def save_to_json(data: list, filename: str) -> None:
    """
    Serializes an API response payload and saves it as a JSON file
    in the Bronze landing zone (data/bronze/).

    Args:
        data: The list of records returned by the Amadeus API.
        filename: The target filename (e.g., 'bronze_api_inspiration.json').
    """
    filepath = os.path.join(BRONZE_PATH, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data saved to: {filepath}")
    except Exception as e:
        print(f"ERROR: Failed to save JSON to {filepath}: {e}")


def fetch_api_data(amadeus: Client) -> None:
    """
    Fetches data from 3 Amadeus API endpoints and saves each response
    as a separate JSON file in the Bronze layer:
      1. Flight Inspiration Search (origin=MAD)
      2. Flight Most Booked (origin=MAD, period=2023-01)
      3. Flight Most Traveled (origin=CGK, period=2023-01)

    Args:
        amadeus: An authenticated Amadeus Client instance.
    """
    # --- [1/3] Flight Inspiration Search ---
    try:
        print("\nFetching [1/3] Flight Inspiration Search (origin=MAD)...")
        response = amadeus.shopping.flight_destinations.get(origin="MAD")
        if response.data:
            save_to_json(response.data, "bronze_api_inspiration.json")
        else:
            print("INFO [1]: Flight Inspiration Search returned no data.")
    except ResponseError as error:
        print(f"ERROR [1]: {error}")

    # --- [2/3] Most Booked Destinations ---
    try:
        print("\nFetching [2/3] Flight Most Booked (origin=MAD, period=2023-01)...")
        response = amadeus.travel.analytics.air_traffic.booked.get(
            originCityCode="MAD",
            period="2023-01",
        )
        if response.data:
            save_to_json(response.data, "bronze_api_most_booked.json")
        else:
            print("INFO [2]: Flight Most Booked returned no data for MAD, 2023-01.")
    except ResponseError as error:
        print(f"ERROR [2]: {error}")

    # --- [3/3] Most Traveled Destinations ---
    try:
        print("\nFetching [3/3] Flight Most Traveled (origin=CGK, period=2023-01)...")
        response = amadeus.travel.analytics.air_traffic.traveled.get(
            originCityCode="CGK",
            period="2023-01",
        )
        if response.data:
            save_to_json(response.data, "bronze_api_most_traveled.json")
        else:
            print("INFO [3]: Flight Most Traveled returned no data for CGK, 2023-01.")
    except ResponseError as error:
        print(f"ERROR [3]: {error}")


if __name__ == "__main__":
    if AMADEUS_KEY == "your_amadeus_key_here":
        print("=" * 60)
        print("ERROR: Amadeus API credentials not configured.")
        print("Set AMADEUS_KEY and AMADEUS_SECRET in your .env file.")
        print("=" * 60)
    else:
        client = get_amadeus_client()
        if client:
            fetch_api_data(client)
            print("\n--- Amadeus API extraction complete ---")