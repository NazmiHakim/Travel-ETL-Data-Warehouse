# TravelNusantara: End-to-End Flight ETL & Local AI Data Analyst Agent

[![CI](https://github.com/NazmiHakim/Travel-ETL-Data-Warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/NazmiHakim/Travel-ETL-Data-Warehouse/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

This repository implements a Data Warehouse and Local AI Analytics engine for **TravelNusantara**, a fictional Online Travel Agency (OTA).

The system combines a Kimball Star Schema data warehouse (Medallion Architecture) with a hybrid local/offline AI agent:
1. **Silver Layer AI ETL Enrichment:** `ai_enrich_reviews.py` extracts sentiment scores and complaint categories from raw customer text reviews.
2. **Local AI Data Analyst Agent:** An interactive Text-to-SQL engine uses Vector-RAG domain scoring, live schema introspection, local query routing (~700+ QPS), and an automated self-correction reflection loop.

---

## Quick Start

### Option A: Docker (Recommended — one command)
```bash
cp .env.example .env   # fill in your Gemini API key (optional)
docker-compose up --build
# → Opens at http://localhost:8501
```

### Option B: Local Python
```bash
pip install -r requirements.txt
cp .env.example .env   # configure DB credentials
python run_pipeline.py # runs all 5 ETL steps in order
streamlit run app.py
```

---

## Benchmark Results

| Test Suite | Cases | Pass Rate | Throughput |
|---|---|---|---|
| SQL Assertion (happy path) | 5,000 | 100% | — |
| Adversarial / Security | 5,000 | 100% block rate | — |
| Mode B Live QPS Stress Test | 200,000 queries | 100% | **316 QPS** |
| Intent Router Unit Tests | 16 cases | 100% | — |

---

## Technical Documentation Reference
> [!NOTE]
> Read **[MASTER_PROJECT_DOCUMENTATION.md](MASTER_PROJECT_DOCUMENTATION.md)** for phase-by-phase explanations, mathematical formulas, and benchmarking data.

---

## System Capabilities

### 1. Descriptive Analytics and Business Intelligence
* **Route and Destination Analysis:** The agent ranks flight routes and cities by passenger volume and gross revenue.
* **Carrier Performance and Delay Metrics:** The pipeline computes departure delays, arrival delays, and revenue aggregations per airline.
* **Seasonal Demand Patterns:** The data warehouse tracks booking trends across months, quarters, and years via `Dim_Date`.
* **Executive Dashboards:** The repository includes a Power BI file (`Data Warehouse Visualization.pbix`) for visual data exploration.

### 2. Local/Offline Agent Automation
* **Unstructured Text Enrichment:** The AI enrichment script converts raw customer reviews into structured metrics (`sentiment`, `complaint_category`, `satisfaction_score`).
* **Dual-Mode Text-to-SQL Processing:**
  * **Mode A (Gemini LLM):** The agent calls the Google Gemini 2.5 Flash API when a valid `GEMINI_API_KEY` is configured in `.env`.
  * **Mode B (Local Deterministic Engine):** The agent uses its local NLP pattern engine when no API key is present, generating SQL via keyword matching and TF-IDF domain scoring with zero external latency.
* **Vector RAG Domain Scoring:** The RAG retriever computes TF-IDF cosine similarity scores between user queries and domain knowledge chunks to route queries to the correct table (`fact_flights`, `fact_customer_feedback`, `dim_airline`, `dim_airport`).
* **Self-Correction Reflection Loop:** The agent intercepts PostgreSQL execution errors and feeds the error trace back into the prompt for automated retries (up to 3 cycles).

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
|     - AI Review Text Analysis: Sentiment & Category Extraction (ai_enrich_reviews)|
|                                                                                   |
|    GOLD LAYER (Star Schema Data Warehouse in db_dwh)                              |
|     - Dim_Airport, Dim_Airline, Dim_Date                                          |
|     - Fact_Flights (Operational Delays + Revenue Aggregations)                    |
|     - Fact_Customer_Feedback (AI-Enriched Sentiment & Complaint Categories)       |
+-----------------------------------------------------------------------------------+
                                         |
                                         +----------------------------------+
                                         |                                  |
                                         v                                  v
+--------------------------------------------------+ +------------------------------+
|             BUSINESS INTELLIGENCE                | |     LOCAL / OFFLINE AGENT    |
|  Power BI (Data Warehouse Visualization.pbix)    | |  Streamlit Text-to-SQL Agent |
|                                                  | |  (Vector RAG + Reflection)   |
+--------------------------------------------------+ +------------------------------+
```

---

## Database Schemas & Data Dictionary

### 1. OLTP Database (`db_oltp`)
* **Table: `Bookings`** — `booking_id (PK)`, `booking_date`, `user_id`, `flight_carrier_code`, `flight_origin_id`, `flight_dest_id`, `passengers`, `revenue`.

---

### 2. Data Warehouse Database (`db_dwh`) — Star Schema

```
       +--------------------+          +--------------------+          +-------------------+
       |    Dim_Airport     |          |    Dim_Airline     |          |     Dim_Date      |
       +--------------------+          +--------------------+          +-------------------+
       | PK airport_id_key  |          | PK airline_key     |          | PK date_key       |
       |    airport_id      |          |    carrier_code    |          |    full_date      |
       |    city            |          |    airline_name    |          |    day_of_week    |
       |    state           |          +--------------------+          |    month          |
       |    name            |                   ^                      |    quarter        |
       +--------------------+                   |                      |    year           |
                 ^                              |                      +-------------------+
                 | (origin/dest FKs)            |                             ^
                 |                              |                             |
       +-------------------------------+   +---------------------------+      |
       |         Fact_Flights          |   |  Fact_Customer_Feedback   |      |
       +-------------------------------+   +---------------------------+      |
       | PK flight_key                 |   | PK feedback_key           |      |
       | FK date_key --------------------------> FK date_key ----------+      |
       | FK airline_key --------------->   | FK airline_key            |
       | FK origin_airport_key         |   |    sentiment              |
       | FK dest_airport_key           |   |    complaint_category     |
       |    departure_delay            |   |    satisfaction_score     |
       |    arrival_delay              |   |    review_text            |
       |    total_passengers           |   +---------------------------+
       |    total_revenue              |
       +-------------------------------+
```

> [!NOTE]
> Column names match `setup_database.sql` exactly. `Fact_Flights` stores raw `departure_delay` and `arrival_delay` integers; averages are computed at query time using `AVG()`.

---

## Local/Offline Agent Engine Architecture

```
                                +-----------------------------------+
                                |         USER INPUT PROMPT         |
                                +-----------------------------------+
                                                  |
                                                  v
                                +-----------------------------------+
                                |       ANALYTICAL QUERY GUARD      |
                                |  (Non-analytical prompts rejected)|
                                +-----------------------------------+
                                                  |
                    (GEMINI_API_KEY set in .env?)
                                /                              \
                          YES  /                                \  NO
                              v                                  v
       +----------------------------------+   +------------------------------------------+
       |   MODE A: GEMINI LLM ENGINE      |   |   MODE B: LOCAL DETERMINISTIC ENGINE      |
       |  - google-genai SDK call         |   |  - Keyword matching + TF-IDF domain score |
       |  - gemini-2.5-flash model        |   |  - Sub-millisecond execution (~700+ QPS)  |
       |  - Full schema context injected  |   |  - Zero API latency & zero external cost  |
       +----------------------------------+   +------------------------------------------+
                              \                                /
                               \                              /
                                v                            v
                                +----------------------------+
                                |    SQL QUERY EXTRACTION    |
                                |  (parse ```sql ``` block)  |
                                +----------------------------+
                                              |
                                              v
                                +----------------------------+
                                |  POSTGRESQL READ-ONLY DB   |
                                |       EXECUTION            |
                                +----------------------------+
                                              |
                              (If SQL Execution Exception)
                                              |
                                              v
                                +----------------------------+
                                |  SELF-CORRECTION LOOP      |
                                |  Append error to prompt,   |
                                |  retry up to 3 times       |
                                +----------------------------+
                                              |
                                              v
                                +----------------------------+
                                | PLOTLY CHARTS & MARKDOWN   |
                                |       OUTPUT               |
                                +----------------------------+
```

---

### Deep-Dive: Mode B (Local Deterministic Engine)

Mode B activates when no `GEMINI_API_KEY` is set in `.env`, or when the Gemini API call fails. The engine processes natural language prompts entirely on local CPU resources with no external network calls.

#### Component Breakdown

* **Vector RAG Retriever (`agent/rag_retriever.py`)**
  The RAG retriever builds a TF-IDF vector matrix from a curated data dictionary containing domain knowledge for `Dim_Airline`, `Dim_Airport`, `Fact_Flights`, and `Fact_Customer_Feedback`. The retriever computes cosine similarity scores between the user query and each domain chunk to identify which table should anchor the SQL query. The relevance threshold for returning a context chunk is 0.05. Domain-routing thresholds (e.g., `feedback_score >= 0.12`, `delay_score >= 0.15`) are applied *inside* `generate_dynamic_sql` to select the correct query template.

* **Entity Normalizer (`agent/rag_retriever.py` — `normalize_entities`)**
  The entity normalizer scans the raw user prompt for IATA airport codes (e.g., `ATL`, `JFK`) and airline carrier codes (e.g., `AA`, `DL`) and replaces them with their full city or airline names before SQL generation. This prevents unresolvable token mismatches against dimension table string values.

* **Deterministic SQL Generator (`agent/sql_agent.py` — `generate_dynamic_sql`)**
  The local SQL generator applies regex-based intent parsing across six keyword dictionaries (`REVENUE_KEYWORDS`, `DELAY_KEYWORDS`, `REVIEW_KEYWORDS`, `LOCATION_KEYWORDS`, `PASSENGER_KEYWORDS`, `TIME_KEYWORDS`) to determine sort direction, `LIMIT` clause, year filter, and target domain. The generator then constructs a raw SQL `SELECT` string using f-string templates with multi-table `JOIN`, `GROUP BY`, `ORDER BY`, and optional `WHERE` year conditions. No external API is called.

* **Reflection & Self-Correction Loop (`agent/sql_agent.py` — `process_query`)**
  The main agent loop executes the generated SQL against `db_dwh` via `agent/db_tools.py` in a read-only sandbox. If PostgreSQL returns an error, the loop appends the full error trace and the failed SQL back into the prompt and calls `call_llm` again — up to `max_retries=3` cycles. Connection errors (`OperationalError`) abort the loop immediately, as SQL changes cannot fix connectivity failures.

#### Mode B Step-by-Step Workflow

1. **Guard Check:** The agent verifies the prompt contains at least one analytical keyword from `ALL_ANALYTICAL`; non-analytical inputs receive a help response immediately.
2. **Cache Lookup:** The agent checks a session-level SHA-256 keyed dictionary to return cached results for repeated identical questions without a DB round-trip.
3. **RAG Context Grounding:** The RAG retriever fetches the top 2 most relevant data dictionary chunks and entity normalizations.
4. **SQL Generation:** `generate_dynamic_sql` builds a complete SQL query string based on keyword domain scores and parsed intent tokens.
5. **DB Execution:** `execute_sql` runs the query against `db_dwh` and returns `(success: bool, DataFrame | error_msg, elapsed_seconds)`.
6. **Reflection Loop:** On failure, the error message is appended to the prompt and a corrected SQL is generated; this repeats up to 3 times.
7. **Output Rendering:** The Streamlit app renders the result `DataFrame` as interactive Plotly charts and a natural-language markdown summary.

---

## Execution Guide

Follow these steps to set up and run the project locally.

---

### Step 1: Environment Setup

#### 1. Prerequisites
- **Python**: Version 3.8 or higher.
- **PostgreSQL**: Version 14 or higher, running on port `5432`.

#### 2. Virtual Environment Setup

```bash
# Create and activate virtual environment
python -m venv venv

# Windows PowerShell:
.\venv\Scripts\Activate.ps1
# macOS / Linux:
source venv/bin/activate

# Install dependencies
pip install pandas sqlalchemy psycopg2-binary google-genai python-dotenv Faker streamlit plotly scikit-learn
```

#### 3. Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=db_dwh
DB_USER=postgres
DB_PASS=your_postgres_password

# Optional — agent runs fully offline (Mode B) if omitted
GEMINI_API_KEY=your_gemini_api_key_here

# Optional — only needed for extract_api.py
AMADEUS_KEY=your_amadeus_key_here
AMADEUS_SECRET=your_amadeus_secret_here
```

#### 4. Database Initialization

Create both databases in PostgreSQL:

```sql
CREATE DATABASE db_oltp;
CREATE DATABASE db_dwh;
```

Run `setup_database.sql` — it contains DDL for both databases:

```bash
psql -U postgres -d db_oltp -f setup_database.sql
psql -U postgres -d db_dwh -f setup_database.sql
```

---

### Step 2: Pipeline Execution

Run the scripts in order:

```bash
# 1. Generate synthetic airports/flights/reviews CSV files
python "python script/generate_source_data.py"

# 2. Populate the OLTP database with 5,000 booking records
python "python script/generate_dummy_oltp.py"

# 3. Extract OLTP bookings to Bronze layer CSV
python "python script/extract_oltp.py"

# 4. Transform and load all data into the Gold Data Warehouse (db_dwh)
python "python script/transform_and_load.py"

# 5. Run AI enrichment to populate Fact_Customer_Feedback
python "python script/ai_enrich_reviews.py"
```

---

### Step 3: Launch the Agent

```bash
streamlit run app.py
```

The app opens at `http://localhost:8501`. Example questions to test:
- *"Which airline generated the highest total revenue?"*
- *"Show the top 5 destination cities by passenger volume."*
- *"Which airline has the most negative reviews?"*
- *"What are the average departure delays per airline?"*

---

## Directory Structure

```text
.
├── app.py                             # Streamlit Web UI
├── setup_database.sql                 # DDL for db_oltp & db_dwh
├── .env.example                       # Environment variable template
├── MASTER_PROJECT_DOCUMENTATION.md    # Full technical documentation
├── Data Warehouse Visualization.pbix  # Power BI dashboard file
├── README.md                          # This file
├── test_whitebox_suite.py             # 200,000-query white-box stress test
├── test_whitebox_5k.py                # 10,000-case SQL assertion suite
├── test_routing.py                    # Intent router unit tests
├── test_rag_schema.py                 # RAG retriever & schema inspector tests
├── agent/
│   ├── db_tools.py                    # SQLAlchemy engine & execute_sql
│   ├── schema_inspector.py            # Live PostgreSQL schema introspection
│   ├── rag_retriever.py               # TF-IDF Vector RAG engine
│   └── sql_agent.py                   # TextToSQLAgent (Mode A + Mode B + Reflection)
├── data/
│   ├── airports.csv                   # Airport reference data
│   ├── flights.csv                    # Operational flight data
│   └── bronze/                        # Bronze-layer extracted files
└── python script/
    ├── generate_source_data.py        # Synthetic CSV generator
    ├── generate_dummy_oltp.py         # OLTP booking record generator
    ├── extract_oltp.py                # OLTP → Bronze extractor
    ├── extract_api.py                 # Amadeus API extractor (optional)
    ├── transform_and_load.py          # Silver → Gold ETL transformer
    └── ai_enrich_reviews.py           # AI review enrichment script
```
