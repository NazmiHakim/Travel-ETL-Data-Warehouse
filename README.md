# TravelNusantara: End-to-End Flight ETL & Local AI Data Analyst Agent

This repository implements a Data Warehouse and Local AI Analytics engine for **TravelNusantara**, a fictional Online Travel Agency (OTA).

The system combines a Kimball Star Schema data warehouse (Medallion Architecture) with a hybrid local/offline AI agent:
1. **Silver Layer AI ETL Enrichment:** `ai_enrich_reviews.py` extracts sentiment scores and complaint categories from raw customer text reviews.
2. **Local AI Data Analyst Agent:** An interactive Text-to-SQL engine uses Vector-RAG domain scoring, live schema introspection, local query routing (~700+ QPS), and an automated self-correction reflection loop.

---

## Technical Documentation Reference
> [!NOTE]
> Read **[MASTER_PROJECT_DOCUMENTATION.md](MASTER_PROJECT_DOCUMENTATION.md)** for phase-by-phase explanations, mathematical formulas, and benchmarking data.

---

## System Capabilities

### 1. Descriptive Analytics and Business Intelligence
* **Route and Destination Analysis:** Ranks flight routes and cities by passenger volume and gross revenue.
* **Carrier Performance and Delay Metrics:** Computes average departure delays, arrival delays, and revenue per airline.
* **Seasonal Demand Patterns:** Tracks booking trends across months, quarters, and days of the week.
* **Executive Dashboards:** Includes a Power BI report file (`Data Warehouse Visualization.pbix`) for visual data exploration.

### 2. Local/Offline Agent Automation
* **Unstructured Text Enrichment:** Converts raw customer reviews into structured metrics (`sentiment`, `complaint_category`, `satisfaction_score`).
* **Dual-Mode Text-to-SQL Processing:** 
  * **Mode A (Neural LLM Reasoning):** Queries the Google Gemini 2.5 Flash API to resolve complex natural language questions.
  * **Mode B (Local Deterministic Engine):** Uses regex matching, dynamic schema checks, and TF-IDF cosine similarity to generate SQL locally with zero API latency.
* **Vector RAG Domain Scoring:** Computes TF-IDF vector similarity between user queries and target data domains to avoid keyword lookup failures.
* **Self-Correction Reflection Loop:** Intercepts PostgreSQL execution errors and feeds the error trace back to the generator for automated retries (up to 3 cycles).

---

## Medallion System Architecture

```
+-----------------------------------------------------------------------------------+
|                                  DATA SOURCES                                     |
|  [generate_source_data.py]   [generate_dummy_oltp.py]      [Amadeus Flight API]   |
|   airports/flights/reviews     db_oltp (Bookings Table)    Inspiration/Traffic    |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|                             MEDALLION ARCHITECTURE                                |
|                                                                                   |
|    BRONZE LAYER (Raw Landing Zone)                                                |
|     - Raw CSV files (data/airports.csv, data/flights.csv)                         |
|     - Raw Customer Reviews (data/bronze/customer_reviews.csv)                     |
|     - Raw OLTP Extract (data/bronze/bronze_bookings.csv)                          |
|     - Raw API JSON Extracts (data/bronze/bronze_api_*.json)                       |
|                                                                                   |
|    SILVER LAYER (Conformed, Cleansed & AI Enriched)                               |
|     - In-Memory Cleaning, Deduplication & Code Mappings (transform_and_load.py)   |
|     - AI Review Text Analysis: Sentiment & Category Extraction (ai_enrich_reviews) |
|                                                                                   |
|    GOLD LAYER (Star Schema Data Warehouse in db_dwh)                              |
|     - Dim_Airport, Dim_Airline, Dim_Date                                         |
|     - Fact_Flights (Operational Delays + Revenue Aggregations)                    |
|     - Fact_Customer_Feedback (AI-Enriched Sentiment & Complaint Categories)       |
+-----------------------------------------------------------------------------------+
                                         |
                                         +----------------------------------+
                                         |                                  |
                                         v                                  v
+--------------------------------------------------+ +------------------------------+
|             BUSINESS INTELLIGENCE                | |     LOCAL / OFFLINE AGENT    |
|  Power BI (Data Warehouse Visualization.pbix)    | |  Streamlit Text-to-SQL Agent|
|                                                  | |  (Vector RAG + Reflection)  |
+--------------------------------------------------+ +------------------------------+
```

---

## Database Schemas & Data Dictionary

### 1. OLTP Database (`db_oltp`)
* **Table: `Bookings`** — `booking_id (PK)`, `booking_date`, `user_id`, `flight_carrier_code`, `flight_origin_id`, `flight_dest_id`, `passengers`, `revenue`.

---

### 2. Data Warehouse Database (`db_dwh`) - Star Schema

```
       +--------------------+          +--------------------+
       |    Dim_Airport     |          |    Dim_Airline     |
       +--------------------+          +--------------------+
       | PK airport_id_key  |<----+    | PK airline_key     |<-------+
       |    airport_id      |     |    |    carrier_code    |        |
       |    city            |     |    |    airline_name    |        |
       |    state           |     |    +--------------------+        |
       |    name            |     |              ^                   |
       +--------------------+     |              |                   |
                 ^                |              |                   |
                 | (origin/dest)  |              |                   |
                 +----------+     |              |                   |
                            |     |              |                   |
                     +-------------------------------+    +---------------------------+
                     |          Fact_Flights         |    |  Fact_Customer_Feedback   |
                     +-------------------------------+    +---------------------------+
                     | PK flight_key                 |    | PK feedback_key           |
                     | FK date_key ---------------------> | FK date_key               |
                     | FK airline_key                |    | FK airline_key            |
                     | FK origin_airport_key         |    |    sentiment              |
                     | FK dest_airport_key           |    |    complaint_category     |
                     |    avg_departure_delay        |    |    satisfaction_score     |
                     |    avg_arrival_delay          |    |    review_text            |
                     |    total_passengers           |    +---------------------------+
                     |    total_revenue              |
                     +-------------------------------+
```

---

## Local/Offline Agent Engine Architecture

```
                                +-----------------------------------+
                                |         USER INPUT PROMPT         |
                                +-----------------------------------+
                                                  |
                                                  v
                                +-----------------------------------+
                                |     LOCAL / OFFLINE INTENT ROUTER |
                                +-----------------------------------+
                                  /                               \
                                 /                                 \
  (Lexical Match OR Vector TF-IDF Score > 0.12)           (Ambiguous / Complex Prompt)
                               /                                     \
                              v                                       v
         +-----------------------------------------+   +----------------------------------+
         |     MODE B: LOCAL DETERMINISTIC ENGINE  |   |    MODE A: NEURAL LLM ENGINE    |
         |  - Sub-millisecond execution (~700 QPS) |   |  - Google Gemini 2.5 Flash API   |
         |  - Zero API Latency & Zero Cost         |   |  - Deep reasoning & multi-joins  |
         +-----------------------------------------+   +----------------------------------+
                              \                                       /
                               \                                     /
                                v                                   v
                                +-----------------------------------+
                                |    SCHEMA INSPECTION & DDL CONVERT|
                                +-----------------------------------+
                                                  |
                                                  v
                                +-----------------------------------+
                                |   POSTGRESQL READ-ONLY SANDBOX    |
                                +-----------------------------------+
                                                  |
                                   (If SQL Execution Exception)
                                                  |
                                                  v
                                +-----------------------------------+
                                |   SELF-CORRECTION REFLECTION LOOP |
                                |   (Catches error trace, retries)  |
                                +-----------------------------------+
                                                  |
                                                  v
                                +-----------------------------------+
                                |  PLOTLY CHARTS & MARKDOWN OUTPUT  |
                                +-----------------------------------+
```

---

## Execution Guide

Follow these steps to set up and run the project locally.

---

### Step 1: Environment Setup

#### 1. Prerequisites
- **Python**: Version 3.8 or higher.
- **PostgreSQL**: Version 14 or higher running on port `5432`.

#### 2. Virtual Environment Setup
Run the following commands in your project directory:

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows PowerShell:
.\venv\Scripts\Activate.ps1
# macOS / Linux:
source venv/bin/activate

# Install dependencies
pip install pandas sqlalchemy psycopg2-binary google-genai python-dotenv Faker streamlit plotly scikit-learn
```

#### 3. Database Initialization
Create the target databases using `psql` or pgAdmin:

```sql
CREATE DATABASE db_oltp;
CREATE DATABASE db_dwh;
```

Execute `setup_database.sql` against both databases:
```bash
psql -U postgres -d db_oltp -f setup_database.sql
psql -U postgres -d db_dwh -f setup_database.sql
```

#### 4. Environment Variables Configuration
Create a `.env` file in the root directory:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASS=your_postgres_password

# Gemini API Key (Optional: system defaults to Mode B if omitted)
GEMINI_API_KEY=your_gemini_api_key_here
```

---

### Step 2: Pipeline Execution

Run the scripts in sequential order to execute the ETL pipeline:

```bash
# 1. Generate synthetic source CSV files
python "python script/generate_source_data.py"

# 2. Populate OLTP database with 5,000 records
python "python script/generate_dummy_oltp.py"

# 3. Extract OLTP data into the Bronze layer
python "python script/extract_oltp.py"

# 4. Transform and load data into the Gold Data Warehouse
python "python script/transform_and_load.py"

# 5. Run AI review enrichment
python "python script/ai_enrich_reviews.py"
```

---

### Step 3: Launch Streamlit Application

Start the interactive web application:

```bash
streamlit run app.py
```

The app will open automatically at `http://localhost:8501`. You can test sample questions such as:
- *"Which airline generated the highest total revenue in 2024?"*
- *"Show the top 5 destination cities by passenger volume."*
- *"Which airline received the lowest average rating and highest departure delays?"*

---

## Directory Structure

```text
.
├── app.py                             # Interactive Streamlit Web UI
├── setup_database.sql                 # DDL Script for db_oltp & db_dwh
├── MASTER_PROJECT_DOCUMENTATION.md    # Technical documentation reference
├── Data Warehouse Visualization.pbix  # Power BI Dashboard file
├── README.md                          # Repository overview
├── test_whitebox_100k.py              # 200,000-Query White-Box Benchmark
├── test_whitebox_5k.py                # 10,000-Case SQL Assertion Suite
├── agent/                             # AI Agent Module
│   ├── db_tools.py                    # Database connection interface
│   ├── schema_inspector.py            # Dynamic schema introspection
│   ├── rag_retriever.py               # Vector RAG & similarity engine
│   └── sql_agent.py                   # Text-to-SQL Agent with Reflection Loop
├── data/                              # Source and Bronze Data Directory
│   ├── airports.csv                   # Airport reference data
│   ├── flights.csv                    # Operational flight data
│   └── bronze/                        # Ingested datasets and text reviews
└── python script/                     # Pipeline Execution Scripts
    ├── generate_source_data.py        # Synthetic dataset generator
    ├── generate_dummy_oltp.py         # OLTP dataset generator
    ├── extract_oltp.py                # OLTP extraction script
    ├── extract_api.py                 # Amadeus API extraction script
    ├── transform_and_load.py          # ETL transformation script
    └── ai_enrich_reviews.py           # Text review enrichment script
```
