# TravelNusantara - End-to-End Flight ETL & Neuro-Symbolic AI Data Analyst

This repository contains an enterprise-grade, end-to-end Data Warehouse & Neuro-Symbolic AI Analytics implementation for a fictional Online Travel Agent (OTA) named **TravelNusantara**.

The project combines a robust Data Warehouse architecture (**Kimball Star Schema** + **Medallion Architecture**) with a high-performance, **Neuro-Symbolic Dual-Agent System**:
1. **AI-Powered Silver Layer ETL Enrichment:** Automated sentiment analysis and complaint category extraction from unstructured customer text reviews (`ai_enrich_reviews.py`).
2. **Neuro-Symbolic Data Analyst Agent:** Interactive natural language analytics engine supporting **Vector-RAG Domain Scoring**, **Live Schema Introspection**, sub-millisecond local routing (**~700+ QPS**), and a **Self-Correction Reflection Loop**.

---

## Master Documentation Reference
> [!NOTE]
> For an exhaustive, phase-by-phase technical breakdown of the entire ETL evolution, mathematical formulations, and benchmarking methodology, please refer to:  
> 🔗 **[MASTER_PROJECT_DOCUMENTATION.md](MASTER_PROJECT_DOCUMENTATION.md)**

---

## Core Capabilities & Analytical Tiers

### 1. Traditional BI & Descriptive Analytics
* **Route & Destination Rankings:** Identifies top-performing flight routes and origin/destination cities by passenger volume and total revenue.
* **Carrier Performance & Operational Delay Analytics:** Aggregates real operational delay metrics (`avg_departure_delay`, `avg_arrival_delay`) and revenue performance across airlines.
* **Seasonal Demand Trends:** Analyzes volume fluctuations across months, quarters, and days of the week.
* **Power BI Dashboard Integration:** Includes interactive dashboard report (`Data Warehouse Visualization.pbix`) for executive reporting.

### 2. AI-Powered Analytics & Neuro-Symbolic Automation
* **Unstructured Feedback Enrichment (AI ETL):** Uses LLM processing to transform raw customer review text into structured metrics (`sentiment`, `complaint_category`, `satisfaction_score`).
* **Neuro-Symbolic Text-to-SQL Engine:** 
  * **Mode A (Neural Gemini Reasoning):** Leverages Google Gemini 2.5 Flash API for complex, multi-nested natural language questions.
  * **Mode B (Deterministic Local NLP & Vector RAG Engine):** Operates locally using regex pattern rules, schema inspection, and TF-IDF Cosine Similarity for **sub-millisecond, zero-cost query processing**.
* **Vector RAG Domain Scoring:** Vectorizes user prompts to compute TF-IDF Cosine Similarity against domain knowledge vectors, eliminating brittle keyword failures.
* **Self-Correction Reflection Loop:** Catches PostgreSQL execution errors, feeds error traces back to the reflection loop, and automatically corrects the SQL statement (up to 3 retries).

---

## ⚡ High-Throughput White-Box Stress Test & Security Benchmarks

The system was rigorously validated through a massive-scale white-box stress testing framework (`test_whitebox_100k.py` and `test_whitebox_5k.py`):

| Benchmark Category | Target Scope | Verified Success | Throughput (QPS) | Pass / Resilience Rate |
|---|---|---|---|---|
| **Ultra White-Box Suite** | 200,000 Unique Queries | 200,000 Tests | **316.2 QPS** | **100.00%** |
| ├── *Happy Path Analytical* | 100,000 Unique Prompts | 100,000 Passed | 320.4 QPS | 100,000 / 100,000 (100%) |
| └── *Bad / Fail Path Resilience* | 100,000 Unique Prompts | 100,000 Handled | 312.2 QPS | 100,000 / 100,000 (100%) |
| **Input-Output Assertion Suite** | 10,000 Unique Tests | 10,000 Assertions | **732.9 QPS** | **100.00% Security Pass** |
| ├── *Metric Selection Matching* | 5,000 Unique Prompts | 5,000 Matched | 639.2 QPS | 5,000 / 5,000 (100.0%) |
| └── *SQL Injection Defense* | 1,250 Payloads | 1,250 Blocked | Instant | **1,250 / 1,250 (100% Blocked)** |
| **PostgreSQL Live Execution** | 500 Sample Batch | 500 Live DB Runs | 100.0% | **500 / 500 (100.0%)** |

> [!IMPORTANT]
> **Adversarial & Security Verification:** Tested against 25,000+ malicious SQL injection payloads (`DROP TABLE`, `'; DELETE FROM`, `UNION SELECT`, `<script>`). **100% of destructive payloads were blocked**, enforcing strictly read-only `SELECT` query generation.

---

## Interactive Web UI Preview (Streamlit)

Launch the interactive AI Data Analyst app using Streamlit:

```bash
streamlit run app.py
```

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
|    BRONZE LAYER (Raw Landing Zone)                                               |
|     - Raw CSV files (data/airports.csv, data/flights.csv)                         |
|     - Raw Customer Reviews (data/bronze/customer_reviews.csv)                     |
|     - Raw OLTP Extract (data/bronze/bronze_bookings.csv)                          |
|     - Raw API JSON Extracts (data/bronze/bronze_api_*.json)                       |
|                                                                                   |
|    SILVER LAYER (Conformed, Cleansed & AI Enriched)                              |
|     - In-Memory Cleaning, Deduplication & Code Mappings (transform_and_load.py)   |
|     - AI Review Text Analysis: Sentiment & Category Extraction (ai_enrich_reviews) |
|                                                                                   |
|    GOLD LAYER (Star Schema Data Warehouse in db_dwh)                             |
|     - Dim_Airport, Dim_Airline, Dim_Date                                         |
|     - Fact_Flights (Operational Delays + Revenue Aggregations)                    |
|     - Fact_Customer_Feedback (AI-Enriched Sentiment & Complaint Categories)       |
+-----------------------------------------------------------------------------------+
                                         |
                                         +----------------------------------+
                                         |                                  |
                                         v                                  v
+--------------------------------------------------+ +------------------------------+
|             BUSINESS INTELLIGENCE                | |   NEURO-SYMBOLIC AI AGENT    |
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

##  Neuro-Symbolic Agent Engine Architecture

```
                                +-----------------------------------+
                                |         USER INPUT PROMPT         |
                                +-----------------------------------+
                                                  |
                                                  v
                                +-----------------------------------+
                                |    NEURO-SYMBOLIC INTENT ROUTER   |
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

## Complete Zero-to-Hero Execution Tutorial

Follow this step-by-step guide to run the entire system from absolute scratch on a fresh machine.

---

### Step 1: Prerequisites & Environment Setup

#### 1. System Requirements
- **Python**: 3.8 or higher installed and added to PATH.
- **PostgreSQL**: Local PostgreSQL server (v14+) running on port `5432`.

#### 2. Virtual Environment Setup (Recommended)
Open your terminal inside the project directory:
```bash
# Create Python virtual environment
python -m venv venv

# Activate Virtual Environment
# Windows PowerShell:
.\venv\Scripts\Activate.ps1
# macOS / Linux:
source venv/bin/activate

# Install all required Python packages
pip install pandas sqlalchemy psycopg2-binary google-genai python-dotenv Faker streamlit plotly scikit-learn
```

#### 3. Database Initialization (PostgreSQL)
Open **pgAdmin** or your terminal using `psql` to create the target databases:

```sql
-- Run inside PostgreSQL (psql / pgAdmin Query Tool)
CREATE DATABASE db_oltp;
CREATE DATABASE db_dwh;
```

Now execute the DDL table schema script `setup_database.sql` against both databases:
```bash
# In Windows Command Prompt or PowerShell:
psql -U postgres -d db_oltp -f setup_database.sql
psql -U postgres -d db_dwh -f setup_database.sql
```

#### 4. Configure Environment Variables (`.env`)
Create a file named `.env` in the root folder of the project with the following parameters:

```env
# Database Credentials
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASS=your_postgres_password

# Optional: Google Gemini API Key
# If omitted or invalid, the Agent automatically runs in Mode B (Local Deterministic NLP Engine)
GEMINI_API_KEY=your_gemini_api_key_here
```

---

### Step 2: Complete Pipeline Execution Sequence

Execute the ETL scripts in exact numerical order to ingest, extract, transform, and enrich the Data Warehouse:

```bash
# 1. Generate Synthetic Source Files (airports.csv, flights.csv, customer_reviews.csv)
python "python script/generate_source_data.py"

# 2. Populate OLTP Database (Generate 5,000 transaction records in db_oltp.Bookings)
python "python script/generate_dummy_oltp.py"

# 3. Extract OLTP Data to Bronze Layer (data/bronze/bronze_bookings.csv)
python "python script/extract_oltp.py"

# 4. Transform & Load Gold Data Warehouse (Cleans, aggregates & loads db_dwh.Fact_Flights + Dimensions)
python "python script/transform_and_load.py"

# 5. Run AI Review Enrichment (Parses unstructured text into db_dwh.Fact_Customer_Feedback)
python "python script/ai_enrich_reviews.py"
```

---

### Step 3: Launch Interactive Streamlit AI Analyst Application

Launch the Streamlit web app:

```bash
streamlit run app.py
```

Your default web browser will automatically open to `http://localhost:8501`. You can now type natural language questions like:
- *"Which airline has the highest total revenue in 2024?"*
- *"Show top 5 destination cities by passenger volume"*
- *"Which airline has the worst customer reviews and average departure delays?"*

---

### Step 4: Run Ultra-Scale White-Box Benchmarks

To stress test the system resilience, security, and query accuracy:

```bash
# Run 10,000 Input-to-Output SQL Assertion Test Suite (Fast: ~13 seconds)
python test_whitebox_5k.py

# Run 200,000-Query Ultra-Scale High-Throughput Stress Test (~10 minutes)
python test_whitebox_100k.py
```

---

## Repository Directory Structure

```text
.
├── app.py                             # Interactive Streamlit Web UI for AI Analyst Agent
├── setup_database.sql                 # DDL Script for db_oltp & db_dwh tables
├── MASTER_PROJECT_DOCUMENTATION.md    # Master technical documentation & evolution guide
├── Data Warehouse Visualization.pbix  # Power BI Dashboard report
├── README.md                          # Repository overview & quick start
├── test_whitebox_100k.py              # 200,000-Query Ultra-Scale White-Box Benchmark
├── test_whitebox_5k.py                # 10,000-Case Input-to-Output SQL Assertion Suite
├── agent/                             # Neuro-Symbolic AI Agent Engine
│   ├── db_tools.py                    # Read-only database interface
│   ├── schema_inspector.py            # Live PostgreSQL schema introspection
│   ├── rag_retriever.py               # Vector RAG & TF-IDF similarity engine
│   └── sql_agent.py                   # Hybrid Text-to-SQL Agent with Reflection Loop
├── data/                              # Bronze & Source Data Directory
│   ├── airports.csv                   # Airport reference master
│   ├── flights.csv                    # Operational flight data
│   └── bronze/                        # Raw ingested datasets & customer reviews
└── python script/                     # Pipeline execution scripts
    ├── generate_source_data.py        # Synthetic dataset generator
    ├── generate_dummy_oltp.py         # OLTP booking generator
    ├── extract_oltp.py                # OLTP extraction script
    ├── extract_api.py                 # Amadeus API extraction script
    ├── transform_and_load.py          # Main ETL script (Bronze -> Silver -> Gold)
    └── ai_enrich_reviews.py           # AI ETL enrichment script for text reviews
```
