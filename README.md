# TravelNusantara - End-to-End ETL & Data Warehouse Project

This repository contains a complete, end-to-end Data Warehouse implementation for a fictional Online Travel Agent (OTA) named **TravelNusantara**. The project implements a full ETL (Extract, Transform, Load) pipeline, a star-schema data warehouse, and an interactive business intelligence dashboard using Python, PostgreSQL, and Power BI.

The design specification follows **Kimball Methodology** (Star Schema dimensional modeling) and the **Medallion Architecture** (Bronze, Silver, Gold data processing layers). Detailed project background and design diagrams are available in `Document.pdf`.

---

## 🎯 Project Goal & Analytical Capabilities

The primary objective of this Data Warehouse is to transform raw, fragmented transactional booking data and operational flight records into a clean, conformed, and aggregated format optimized for analytics.

This Data Warehouse powers two analytical tiers:

### 1. Descriptive Analytics (Historical Performance)
* **Top Destinations & Routes:** Identifies the highest-volume flight routes and top destination airports based on passenger counts.
* **Airline Performance & Revenue:** Measures revenue generation (`total_revenue`) and market share across different air carriers (e.g., Delta, American Airlines, United, Southwest).
* **Seasonal Demand Patterns:** Analyzes travel demand fluctuations across months, quarters, and days of the week.

### 2. Predictive Analytics (Forward-Looking Intelligence)
* **Passenger Demand Forecasting:** Utilizes historical booking volumes and seasonal trends to forecast passenger demand over 6–12 month horizons, enabling data-driven pricing, marketing budget allocation, and capacity planning.

---

## 📸 Live Dashboard Preview (Power BI)

The final deliverable is an interactive Power BI dashboard (`Data Warehouse Visualization.pbix`) connected directly to the Gold Layer in PostgreSQL (`db_dwh`).

![TravelNusantara Dashboard](image_274f62.png)

---

## 🏛️ Architecture & Design Methodology

```
+-----------------------------------------------------------------------------------+
|                                  DATA SOURCES                                     |
|  [generate_source_data.py]   [generate_dummy_oltp.py]      [Amadeus Flight API]   |
|   airports.csv & flights.csv   db_oltp (Bookings Table)    Inspiration/Traffic    |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                             MEDALLION ARCHITECTURE                                |
|                                                                                   |
|  🥉 BRONZE LAYER (Raw Landing Zone)                                               |
|     - Raw CSV files (data/airports.csv, data/flights.csv)                         |
|     - Raw OLTP Extract (data/bronze/bronze_bookings.csv)                          |
|     - Raw API JSON Extracts (data/bronze/bronze_api_*.json)                       |
|                                                                                   |
|  🥈 SILVER LAYER (Conformed & Cleansed In-Memory)                                 |
|     - Deduplication (drop_duplicates)                                             |
|     - Null value imputation (state fillna)                                        |
|     - Carrier code mapping (e.g., DL -> Delta Air Lines)                          |
|     - Date standardization & granularity truncation                               |
|                                                                                   |
|  🥇 GOLD LAYER (Star Schema Data Warehouse in db_dwh)                             |
|     - Dim_Airport (Surrogate Keys for Airports)                                   |
|     - Dim_Airline (Surrogate Keys for Carriers)                                   |
|     - Dim_Date (Pre-populated Calendar Dimension)                                 |
|     - Fact_Flights (Aggregations: total_passengers, total_revenue)                 |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                            BUSINESS INTELLIGENCE                                  |
|                 Power BI Dashboard (Data Warehouse Visualization.pbix)            |
+-----------------------------------------------------------------------------------+
```

### 1. Kimball Dimensional Modeling (Star Schema)
A **Star Schema** dimensional model was selected for the Gold Layer. Centered around a single fact table (`Fact_Flights`) surrounded by conformed dimension tables (`Dim_Airport`, `Dim_Airline`, `Dim_Date`), this layout minimizes query complexity, optimizes aggregation performance, and integrates seamlessly with Power BI data modeling engine.

### 2. Medallion Processing Architecture
* 🥉 **Bronze Layer (Raw):** Stores un-transformed data directly from sources (CSVs, relational database extracts, JSON API responses) to maintain a persistent audit trail.
* 🥈 **Silver Layer (Cleansed & Conformed):** Applies data cleaning, deduplication, missing value handling, date parsing, and code lookup mappings in memory.
* 🥇 **Gold Layer (Curated Star Schema):** Stores business-level aggregations and dimensional models in PostgreSQL (`db_dwh`), ready for reporting and analytical consumption.

---

## 🗃️ Data Sources & Synthetic Generators

This project integrates data from three primary source streams:

| Source Name | Data Type | Generator Script / Origin | Target Location | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Airport Reference Data** | CSV File | `python script/generate_source_data.py` | `data/airports.csv` | Master reference file containing 15 major US airports. |
| **Flight Operations Data** | CSV File | `python script/generate_source_data.py` | `data/flights.csv` | Operational dataset with 10,000 synthetic flight departure/arrival records. |
| **Transactional OLTP Database** | PostgreSQL Table | `python script/generate_dummy_oltp.py` | `db_oltp.Bookings` | Live booking transaction table populated with 5,000 synthetic records using `Faker`. |
| **Amadeus Flight API** | JSON Response | `python script/extract_api.py` | `data/bronze/bronze_api_*.json` | Live web API extracts for flight inspiration, most booked, and most traveled air traffic. |

---

## 🗄️ Database Schemas & Attribute Data Dictionary

### 1. OLTP Database (`db_oltp`)

The OLTP database represents the transactional booking system.

#### Table: `Bookings`
* **Primary Key:** `booking_id`
* **Description:** Records individual customer booking transactions.

| Column Name | Data Type | Key Type | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `booking_id` | `SERIAL` | **PK** | No | Auto-incrementing unique booking identifier. |
| `booking_date` | `TIMESTAMP` | - | Yes | Timestamp when the transaction occurred. |
| `user_id` | `INT` | - | Yes | Unique ID of the purchasing user (range: 1001–5000). |
| `flight_carrier_code` | `VARCHAR(10)` | - | Yes | 2-letter IATA carrier code (e.g., `DL`, `AA`, `UA`). |
| `flight_origin_id` | `INT` | - | Yes | DOT airport ID for flight origin. |
| `flight_dest_id` | `INT` | - | Yes | DOT airport ID for flight destination. |
| `passengers` | `INT` | - | Yes | Number of passenger seats reserved (1–4). |
| `revenue` | `DECIMAL(10,2)`| - | Yes | Total transaction value in USD. |

---

### 2. Data Warehouse Database (`db_dwh`) - Star Schema

The Data Warehouse contains three dimension tables and one fact table.

```
       +--------------------+          +--------------------+
       |    Dim_Airport     |          |    Dim_Airline     |
       +--------------------+          +--------------------+
       | PK airport_id_key  |<----+    | PK airline_key     |
       |    airport_id      |     |    |    carrier_code    |
       |    city            |     |    |    airline_name    |
       |    state           |     |    +--------------------+
       |    name            |     |              ^
       +--------------------+     |              |
                 ^                |              |
                 | (origin/dest)  |              |
                 +----------+     |              |
                            |     |              |
                     +-------------------------------+
                     |          Fact_Flights         |
                     +-------------------------------+
                     | PK flight_key                 |
                     | FK date_key ---------------------> +--------------------+
                     | FK airline_key                |    |      Dim_Date      |
                     | FK origin_airport_key         |    +--------------------+
                     | FK dest_airport_key           |    | PK date_key        |
                     |    departure_delay            |    |    full_date       |
                     |    arrival_delay              |    |    day_of_week     |
                     |    total_passengers           |    |    day_of_month    |
                     |    total_revenue              |    |    month           |
                     +-------------------------------+    |    quarter         |
                                                          |    year            |
                                                          +--------------------+
```

#### A. Dimension Table: `Dim_Airport`
* **Primary Key:** `airport_id_key` (Surrogate Key)
* **Natural Key:** `airport_id`

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `airport_id_key` | `SERIAL` | **PK** | Internal surrogate primary key. |
| `airport_id` | `INT` | **NK / Unique** | Official US DOT airport ID. |
| `city` | `VARCHAR(100)` | - | City location (e.g., `Atlanta`, `Chicago`). |
| `state` | `VARCHAR(50)` | - | US State code (e.g., `GA`, `IL`). |
| `name` | `VARCHAR(255)` | - | Full official airport name. |

#### B. Dimension Table: `Dim_Airline`
* **Primary Key:** `airline_key` (Surrogate Key)
* **Natural Key:** `carrier_code`

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `airline_key` | `SERIAL` | **PK** | Internal surrogate primary key. |
| `carrier_code` | `VARCHAR(10)` | **NK / Unique** | 2-letter IATA airline code. |
| `airline_name` | `VARCHAR(100)` | - | Full carrier commercial name (e.g., `Delta Air Lines`). |

#### C. Dimension Table: `Dim_Date`
* **Primary Key:** `date_key` (Surrogate Key)
* **Natural Key:** `full_date`

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `date_key` | `SERIAL` | **PK** | Internal surrogate primary key. |
| `full_date` | `DATE` | **NK / Unique** | Calendar date (YYYY-MM-DD). |
| `day_of_week` | `INT` | - | ISO day of week (1 = Monday, 7 = Sunday). |
| `day_of_month` | `INT` | - | Day of the month (1–31). |
| `month` | `INT` | - | Month number (1–12). |
| `quarter` | `INT` | - | Calendar quarter (1–4). |
| `year` | `INT` | - | Four-digit calendar year (e.g., `2024`, `2025`). |

#### D. Fact Table: `Fact_Flights`
* **Primary Key:** `flight_key`
* **Foreign Keys:** `date_key`, `airline_key`, `origin_airport_key`, `dest_airport_key`

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `flight_key` | `SERIAL` | **PK** | Internal primary key for the fact record. |
| `date_key` | `INT` | **FK** | Foreign key referencing `Dim_Date(date_key)`. |
| `airline_key` | `INT` | **FK** | Foreign key referencing `Dim_Airline(airline_key)`. |
| `origin_airport_key` | `INT` | **FK** | Foreign key referencing origin airport in `Dim_Airport(airport_id_key)`. |
| `dest_airport_key` | `INT` | **FK** | Foreign key referencing destination airport in `Dim_Airport(airport_id_key)`. |
| `departure_delay` | `INT` | - | Departure delay in minutes (aggregated / default 0). |
| `arrival_delay` | `INT` | - | Arrival delay in minutes (aggregated / default 0). |
| `total_passengers` | `INT` | - | Sum of passengers for date, airline, origin, and destination. |
| `total_revenue` | `DECIMAL(10,2)`| - | Sum of booking revenue (USD) for date, airline, origin, and destination. |

---

## 🔄 Data Flow & Processing Steps

The ETL pipeline operates across four discrete stages:

### Stage 1: Data Generation & Source Preparation
1. Executing `generate_source_data.py` generates local CSV files (`data/airports.csv` and `data/flights.csv`).
2. Executing `generate_dummy_oltp.py` connects to PostgreSQL (`db_oltp`) and populates the `Bookings` table with 5,000 synthetic transaction records.

### Stage 2: Data Extraction (Bronze Layer)
1. `extract_oltp.py` queries `SELECT * FROM Bookings;` on `db_oltp` and exports raw data to `data/bronze/bronze_bookings.csv`.
2. `extract_api.py` calls Amadeus Flight APIs (Flight Inspiration, Most Booked, Most Traveled) and saves raw responses to JSON files inside `data/bronze/`.

### Stage 3: Data Transformation (Silver Layer)
During `transform_and_load.py`:
1. **Deduplication:** Removes duplicate records across source datasets using `.drop_duplicates()`.
2. **Imputation:** Fills missing `state` values in airport data with `'N/A'`.
3. **Carrier Mapping:** Maps 2-letter carrier codes to full airline names (e.g., `DL` -> `Delta Air Lines`, `AA` -> `American Airlines`). Unmapped codes fall back to `CODE (Unknown)`.
4. **Timestamp Truncation:** Converts timestamp string values (`booking_date`) to daily date values (`date_only`).
5. **Aggregation:** Groups transactional records by `['date_only', 'flight_carrier_code', 'flight_origin_id', 'flight_dest_id']` and calculates:
   * `total_passengers = SUM(passengers)`
   * `total_revenue = SUM(revenue)`

### Stage 4: Dimensional Key Lookup & Loading (Gold Layer)
1. Loads cleaned airport reference records into `Dim_Airport`.
2. Extracts unique carrier codes from operational flights and booking datasets, maps names, and populates `Dim_Airline`.
3. Queries surrogate keys from `Dim_Date`, `Dim_Airline`, and `Dim_Airport` tables in `db_dwh`.
4. Performs inner joins to swap natural business keys (e.g., `flight_carrier_code`, `flight_origin_id`) with Data Warehouse surrogate keys (`airline_key`, `origin_airport_key`, `dest_airport_key`, `date_key`).
5. Executes a `TRUNCATE` command on `Fact_Flights` to ensure clean incremental re-loads, then appends the transformed fact records to `Fact_Flights`.

---

## 📦 Output Artifacts

Upon completing the pipeline execution, the following artifacts are produced:

1. **`db_dwh` PostgreSQL Database:** Star schema warehouse containing conformed dimensions and populated `Fact_Flights` table.
2. **`data/bronze/` Raw Files:** Persistent extraction files (`bronze_bookings.csv` and Amadeus JSON extracts).
3. **`Data Warehouse Visualization.pbix`:** Interactive Power BI dashboard displaying revenue analytics, passenger trends, route rankings, and forecast models.

---

## 🛠️ Technology Stack

* **Database Engine:** PostgreSQL 12+ (Relational OLTP & Data Warehouse)
* **Programming Language:** Python 3.8+
* **Data Processing Libraries:**
  * `pandas`: Data cleaning, deduplication, aggregation, and structural transformations.
  * `sqlalchemy`: SQL Object Relational Mapping (ORM) and engine database connections.
  * `psycopg2-binary`: PostgreSQL database driver.
  * `amadeus`: Official Python SDK for Amadeus Developer API integration.
  * `Faker`: Synthetic data generation library.
  * `python-dotenv`: Environment variable management.
* **Business Intelligence & Visualization:** Power BI Desktop

---

## 🚀 End-to-End Tutorial: How to Set Up & Run the Pipeline

Follow this step-by-step guide to run the entire ETL pipeline from scratch on your local machine.

### Step 1: Prerequisites Verification
Ensure the following tools are installed and operational:
* **PostgreSQL Server:** Running locally on port `5432` with administrative access (e.g., via pgAdmin or `psql`).
* **Python 3.8+:** Installed and available in your system path.
* **Power BI Desktop:** Installed for visualizing dashboard reports.
* *(Optional)* **Amadeus Developer Account:** Free key/secret from [Amadeus for Developers](https://developers.amadeus.com/).

---

### Step 2: Database Initialization

1. Open **pgAdmin** or your preferred SQL client and create two empty databases:
   * `db_oltp` (Simulated operational booking database)
   * `db_dwh` (Analytical Data Warehouse)

2. Execute the setup SQL script `setup_database.sql` against PostgreSQL:
   * Run lines 1–10 against `db_oltp` to create the `Bookings` table.
   * Run lines 17–67 against `db_dwh` to create `Dim_Airport`, `Dim_Airline`, `Dim_Date`, and `Fact_Flights` tables, and populate `Dim_Date` with calendar entries (2020–2025).

---

### Step 3: Python Environment & Dependencies Setup

1. Open your terminal in the project root directory (`d:\Flight ETL integrated with agent`).

2. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   # On Windows PowerShell:
   .\venv\Scripts\Activate.ps1
   ```

3. Install required Python packages:
   ```bash
   pip install pandas sqlalchemy psycopg2-binary amadeus python-dotenv Faker
   ```

4. Create a `.env` file in the root directory to store database credentials and API keys:
   ```env
   # Database Credentials
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=postgres
   DB_PASS=your_postgres_password

   # Amadeus API Credentials (Optional for extract_api.py)
   AMADEUS_KEY=YOUR_AMADEUS_API_KEY
   AMADEUS_SECRET=YOUR_AMADEUS_API_SECRET
   ```

---

### Step 4: Complete Pipeline Execution Sequence

Execute the Python scripts in the exact sequence specified below:

#### 1. Generate Source Reference & Operational CSVs
Generates `data/airports.csv` and `data/flights.csv`.
```bash
python "python script/generate_source_data.py"
```
*Expected Output:*
```
Generating data/airports.csv...
SUCCESS: Created data/airports.csv with 15 airport records.
Generating data/flights.csv with 10000 records...
SUCCESS: Created data/flights.csv with 10000 flight records.
```

#### 2. Populate OLTP Database with Synthetic Bookings
Inserts 5,000 booking transactions into `db_oltp.Bookings`.
```bash
python "python script/generate_dummy_oltp.py"
```
*Expected Output:*
```
Mulai membuat 5000 data dummy...
Memasukkan data ke database db_oltp...
BERHASIL: 5000 data dummy berhasil dimasukkan ke tabel Bookings.
```

#### 3. Extract OLTP Data to Bronze Layer
Extracts transactional data from `db_oltp` into `data/bronze/bronze_bookings.csv`.
```bash
python "python script/extract_oltp.py"
```
*Expected Output:*
```
Terhubung ke db_oltp...
BERHASIL: 5000 data diekstrak dari 'Bookings'.
Data mentah disimpan di: data/bronze/bronze_bookings.csv
```

#### 4. Extract API Data to Bronze Layer (Optional)
Fetches flight destinations and air traffic analytics from Amadeus API.
```bash
python "python script/extract_api.py"
```

#### 5. Run Transformation & Load to Gold Data Warehouse
Cleans, aggregates, performs key lookups, and populates `db_dwh`.
```bash
python "python script/transform_and_load.py"
```
*Expected Output:*
```
Mengecek lokasi file...
--- OK: File Airports ditemukan.
--- OK: File Flights ditemukan.
--- OK: File Bookings (Bronze) ditemukan.

Koneksi ke database 'db_dwh' berhasil.
PEMBERSIHAN (TRUNCATE) TABEL DWH BERHASIL.

--- Memulai Load Dim_Airport ---
BERHASIL: Dim_Airport telah dimuat.

--- Memulai Load Dim_Airline ---
BERHASIL: Dim_Airline telah dimuat.

--- Memulai Load Fact_Flights ---
Memuat data agregat ke Fact_Flights...
BERHASIL: Fact_Flights telah dimuat.

--- Proses ETL (Transform & Load) Selesai ---
```

---

### Step 5: View & Refresh Power BI Dashboard

1. Launch **Power BI Desktop**.
2. Open `Data Warehouse Visualization.pbix`.
3. Click the **Refresh** button on the Home ribbon to pull the newly loaded Gold Layer data from PostgreSQL `db_dwh`.
4. Interact with the visuals, route performance metrics, revenue reports, and demand predictions!

---

## 📁 Repository Directory Structure

```
.
├── Data Warehouse Visualization.pbix   # Interactive Power BI Dashboard report
├── Document.pdf                        # Full architectural & design specification
├── README.md                           # Project documentation & execution guide
├── setup_database.sql                  # DDL script for OLTP and DWH databases
├── data/                               # Data directory (Bronze & source datasets)
│   ├── airports.csv                    # Airport reference master file (generated)
│   ├── flights.csv                     # Operational flight records (generated)
│   └── bronze/                         # Raw ingested datasets
│       ├── bronze_bookings.csv         # Raw extract from db_oltp
│       ├── bronze_api_inspiration.json # Raw Amadeus API response
│       └── bronze_api_most_booked.json  # Raw Amadeus API response
└── python script/                      # ETL Python pipeline scripts
    ├── generate_source_data.py         # Synthetic generator for source CSV files
    ├── generate_dummy_oltp.py          # Synthetic generator for OLTP database
    ├── extract_oltp.py                 # Extractor script (OLTP -> Bronze CSV)
    ├── extract_api.py                  # Extractor script (Amadeus API -> Bronze JSON)
    └── transform_and_load.py           # Main ETL script (Bronze -> Silver -> Gold DWH)
```

---

## 📜 License & Acknowledgments

* Designed & Developed for **TravelNusantara** Data Engineering Architecture.
* Built using Python, PostgreSQL, and Power BI.
