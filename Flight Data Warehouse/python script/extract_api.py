from amadeus import Client, ResponseError
import json
import os

AMADEUS_KEY = ""
AMADEUS_SECRET = ""

BRONZE_PATH = os.path.join("data", "bronze")
os.makedirs(BRONZE_PATH, exist_ok=True) 

def get_amadeus_client():
    """Menginisialisasi dan mengembalikan klien Amadeus."""
    try:
        amadeus = Client(
            client_id=AMADEUS_KEY,
            client_secret=AMADEUS_SECRET
        )
        print("Otentikasi Amadeus berhasil.")
        return amadeus
    except ResponseError as error:
        print(f"Error otentikasi Amadeus: {error}")
        return None

def save_to_json(data, filename):
    """Menyimpan data (response API) ke file JSON."""
    filepath = os.path.join(BRONZE_PATH, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data berhasil disimpan ke: {filepath}")
    except Exception as e:
        print(f"Error menyimpan JSON: {e}")

def fetch_api_data(amadeus):
    """Mengambil data dari 3 endpoint API."""
    try:
        print("\nFetching [1/3] Flight Inspiration Search (origin=CGK)...")
        response = amadeus.shopping.flight_destinations.get(origin='MAD')
        
        if response.data:
            save_to_json(response.data, "bronze_api_inspiration.json")
        else:
            print("Info [1]: Flight Inspiration Search tidak mengembalikan data.")
            
    except ResponseError as error:
        print(f"Error [1]: {error}")

    try:
        print("\nFetching [2/3] Flight Most Booked (origin=CGK, period=2023-01)...")
        response = amadeus.travel.analytics.air_traffic.booked.get(
            originCityCode='MAD',
            period='2023-01' 
        )
        
        if response.data:
            save_to_json(response.data, "bronze_api_most_booked.json")
        else:
            print("Info [2]: Flight Most Booked tidak mengembalikan data untuk CGK, 2023-01.")
            
    except ResponseError as error:
        print(f"Error [2]: {error}")

    try:
        print("\nFetching [3/3] Flight Most Traveled (origin=CGK, period=2023-01)...")
        response = amadeus.travel.analytics.air_traffic.traveled.get(
            originCityCode='CGK',
            period='2023-01' 
        )
        
        if response.data:
            save_to_json(response.data, "bronze_api_most_traveled.json")
        else:
            print("Info [3]: Flight Most Traveled tidak mengembalikan data untuk CGK, 2023-01.")
            
    except ResponseError as error:
        print(f"Error [3]: {error}")

if __name__ == "__main__":
    if AMADEUS_KEY == "YOUR_API_KEY":
        print("===============================================================")
        print("ERROR: Harap masukkan API Key dan Secret Anda di dalam script.")
        print("Ganti 'YOUR_API_KEY' dan 'YOUR_API_SECRET' dengan kunci asli Anda.")
        print("===============================================================")
    else:
        client = get_amadeus_client()
        if client:
            fetch_api_data(client)
            print("\n--- Proses Ekstraksi API Selesai ---")