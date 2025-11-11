import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys 

BASE_DATA_PATH = r""

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "db_dwh"  
DB_USER = ""
DB_PASS = ""

AIRPORTS_FILE = os.path.join(BASE_DATA_PATH, "airports.csv")
FLIGHTS_FILE = os.path.join(BASE_DATA_PATH, "flights.csv")
BOOKINGS_FILE = os.path.join(BASE_DATA_PATH, "bronze", "bronze_bookings.csv")

file_checks = {
    "Airports": AIRPORTS_FILE,
    "Flights": FLIGHTS_FILE,
    "Bookings (Bronze)": BOOKINGS_FILE
}

print("Mengecek lokasi file...")
all_files_found = True
for name, path in file_checks.items():
    if not os.path.exists(path):
        print(f"!!! ERROR: File {name} tidak ditemukan di: {path}")
        all_files_found = False
    else:
        print(f"--- OK: File {name} ditemukan.")

if not all_files_found:
    print("\nProses dihentikan. Harap periksa kembali path file Anda di dalam script.")
    sys.exit() 

try:
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    print("\nKoneksi ke database 'db_dwh' berhasil.")
except Exception as e:
    print(f"Error koneksi ke 'db_dwh': {e}")
    sys.exit() 

def clean_data(df, table_name):
    """Fungsi pembersihan sederhana (Contoh Silver Layer)."""
    print(f"Membersihkan data untuk {table_name}...")
    df = df.drop_duplicates()
    if 'state' in df.columns:
        df['state'] = df['state'].fillna('N/A')
    print(f"Pembersihan selesai. Jumlah record: {len(df)}")
    return df

def load_dim_airport():
    """Membaca airports.csv, membersihkan, dan memuat ke Dim_Airport."""
    try:
        print("\n--- Memulai Load Dim_Airport ---")
        df_airports = pd.read_csv(AIRPORTS_FILE)
        df_airports_clean = clean_data(df_airports, "Dim_Airport")
        df_to_load = df_airports_clean[['airport_id', 'city', 'state', 'name']]
        df_to_load.to_sql('dim_airport', engine, if_exists='append', index=False)
        print("BERHASIL: Dim_Airport telah dimuat.")
    except Exception as e:
        if 'UniqueViolation' in str(e):
             print("INFO: Data Dim_Airport sebagian sudah ada (UniqueViolation). Melanjutkan...")
        else:
            print(f"ERROR saat memuat Dim_Airport: {e}")

def load_dim_airline():
    """Membaca carrier dari flights & bookings, dan memuat ke Dim_Airline."""
    try:
        print("\n--- Memulai Load Dim_Airline ---")
        df_flights = pd.read_csv(FLIGHTS_FILE)
        df_bookings = pd.read_csv(BOOKINGS_FILE)
        
        carriers_flights = df_flights['Carrier'].unique()
        carriers_bookings = df_bookings['flight_carrier_code'].unique()
        
        all_carriers = pd.Series(list(carriers_flights) + list(carriers_bookings)).unique()
        
        df_airlines = pd.DataFrame(all_carriers, columns=['carrier_code'])
        
        carrier_map = {
            'DL': 'Delta Air Lines', 'AA': 'American Airlines',
            'UA': 'United Airlines', 'WN': 'Southwest Airlines',
            'AS': 'Alaska Airlines', 'B6': 'JetBlue Airways',
            'F9': 'Frontier Airlines', 'NK': 'Spirit Airlines'
        }
           
        df_airlines['airline_name'] = df_airlines['carrier_code'].map(carrier_map)

        df_airlines['airline_name'] = df_airlines['airline_name'].fillna(df_airlines['carrier_code'] + " (Unknown)")
 
        df_airlines.to_sql('dim_airline', engine, if_exists='append', index=False)
        print("BERHASIL: Dim_Airline telah dimuat.")
    
    except Exception as e:
        if 'UniqueViolation' in str(e):
             print("INFO: Data Dim_Airline sebagian sudah ada (UniqueViolation). Melanjutkan...")
        else:
            print(f"ERROR saat memuat Dim_Airline: {e}")

def load_fact_flights():
    """Mengagregasi data bookings dan memuatnya ke Fact_Flights."""
    try:
        print("\n--- Memulai Load Fact_Flights ---")
        
        df_bookings = pd.read_csv(BOOKINGS_FILE)

        print("Membaca data dimensi dari DWH untuk lookup...")
        dim_date = pd.read_sql("SELECT date_key, full_date FROM dim_date", engine)
        dim_airline = pd.read_sql("SELECT airline_key, carrier_code FROM dim_airline", engine)
        dim_airport = pd.read_sql("SELECT airport_id_key, airport_id FROM dim_airport", engine)

        if dim_airline.empty:
            print("!!! PERINGATAN: dim_airline kosong. Proses Fact_Flights tidak akan menghasilkan data.")
            print("Pastikan load_dim_airline() berhasil dijalankan terlebih dahulu.")
            return 

        print("Mentransformasi data bookings...")
        df_bookings['booking_date'] = pd.to_datetime(df_bookings['booking_date'])
        df_bookings['date_only'] = df_bookings['booking_date'].dt.date
        dim_date['full_date'] = pd.to_datetime(dim_date['full_date']).dt.date

        print("Mengagregasi data bookings per hari...")
        df_agg = df_bookings.groupby(
            ['date_only', 'flight_carrier_code', 'flight_origin_id', 'flight_dest_id']
        ).agg(
            total_passengers=('passengers', 'sum'),
            total_revenue=('revenue', 'sum')
        ).reset_index()

        print("Melakukan Key Lookup (menukar kode bisnis dengan key DWH)...")
        
        df_fact = pd.merge(df_agg, dim_date, left_on='date_only', right_on='full_date', how='inner')
        df_fact = pd.merge(df_fact, dim_airline, left_on='flight_carrier_code', right_on='carrier_code', how='inner')
        
        dim_airport_origin = dim_airport.rename(columns={'airport_id_key': 'origin_airport_key', 'airport_id': 'origin_id_lookup'})
        df_fact = pd.merge(df_fact, dim_airport_origin, left_on='flight_origin_id', right_on='origin_id_lookup', how='inner')
        
        dim_airport_dest = dim_airport.rename(columns={'airport_id_key': 'dest_airport_key', 'airport_id': 'dest_id_lookup'})
        df_fact = pd.merge(df_fact, dim_airport_dest, left_on='flight_dest_id', right_on='dest_id_lookup', how='inner')

        df_fact['departure_delay'] = 0
        df_fact['arrival_delay'] = 0
        
        final_columns = [
            'date_key', 'airline_key', 'origin_airport_key', 'dest_airport_key',
            'departure_delay', 'arrival_delay', 'total_passengers', 'total_revenue'
        ]
        
        df_final_fact = df_fact[final_columns]
        
        print(f"Memuat {len(df_final_fact)} data agregat ke Fact_Flights...")
        df_final_fact.to_sql('fact_flights', engine, if_exists='append', index=False)
        
        print("BERHASIL: Fact_Flights telah dimuat.")

    except Exception as e:
        print(f"ERROR saat memuat Fact_Flights: {e}")

if __name__ == "__main__":
    try:
        with engine.connect() as conn:
            conn.begin()
            conn.execute(text("TRUNCATE TABLE fact_flights RESTART IDENTITY;"))
            conn.execute(text("TRUNCATE TABLE dim_airline RESTART IDENTITY CASCADE;"))
            conn.execute(text("TRUNCATE TABLE dim_airport RESTART IDENTITY CASCADE;"))
            # ---------------------------------
            conn.commit()
            print("\nPEMBERSIHAN (TRUNCATE) TABEL DWH BERHASIL.")
    except Exception as e:
        print(f"\nError saat TRUNCATE tabel: {e}")
        print("Memutuskan untuk melanjutkan (mungkin tabel masih kosong)...")

    load_dim_airport()
    load_dim_airline()
    load_fact_flights()
    
    print("\n--- Proses ETL (Transform & Load) Selesai ---")