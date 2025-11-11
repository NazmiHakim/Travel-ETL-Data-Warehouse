# TravelNusantara - End-to-End ETL & Data Warehouse Project

This repository contains a complete, end-to-end data warehouse implementation for a fictional Online Travel Agent (OTA) named TravelNusantara. The project spans from dummy data generation to a final predictive dashboard, encompassing a full ETL (Extract, Transform, Load) pipeline, a star-schema data warehouse, and a business intelligence report.

The complete project design specification (including Medallion Architecture and Kimball Methodology) is available in `Document.pdf`.

## 🎯 Project Goal

The primary objective of this data warehouse is to empower the business with actionable insights by transforming raw, transactional data into a clean, aggregated, and analysis-ready format.

This DWH is specifically designed to answer critical business questions, moving from historical analysis to future-facing predictions.

### 1. Descriptive Analytics (What happened?)
* **Top Destinations:** Which destinations are most popular among customers (by passenger volume)?
* **Top Airlines:** Which airlines are the most profitable (by `total_revenue`) and carry the most passengers?
* **Seasonality:** What are the peak and low travel seasons? Are there specific months or quarters with high demand?

### 2. Predictive Analytics (What will happen?)
* **Demand Forecasting:** Based on historical trends and seasonality, what is the expected passenger demand for the next 6-12 months? This allows the company to make better decisions on pricing, marketing spend, and resource allocation.

## 📸 Live Dashboard Preview (Power BI)

The final output is an interactive Power BI dashboard connected directly to our Gold Layer (the PostgreSQL Data Warehouse). This report visualizes the answers to our key business questions.

![TravelNusantara Dashboard](image_274f62.png)

## 🏛️ Architecture & Methodology

* **Methodology: Kimball (Star Schema)**
    A *bottom-up* approach was chosen. This focuses on specific business processes (like bookings) to deliver value quickly. The resulting **Star Schema** is simple, intuitive for business users, and highly optimized for the fast analytical queries required by Power BI.

* **Architecture: Medallion (Bronze, Silver, Gold)**
    This three-layer architecture ensures data quality, governance, and traceability.
    * 🥉 **Bronze Layer:** A "raw data" landing zone. Data from all sources (CSV, API, OLTP) is ingested and stored here as-is, ensuring a complete, auditable history.
    * 🥈 **Silver Layer:** The data is cleaned, validated, de-duplicated, and conformed. This layer represents a single source of truth for the entire enterprise.
    * 🥇 **Gold Layer:** The final, presentation-ready layer. Data is aggregated and modeled into our **Star Schema** (`Fact_Flights` and its dimensions), ready for consumption by BI tools.

## 🛠️ Technology Stack

* **Database (OLTP & DWH):** **PostgreSQL**
* **ETL Pipeline:** **Python 3**
    * `pandas`: For all data transformation and aggregation.
    * `sqlalchemy`: For creating a robust connection engine between Python and PostgreSQL.
    * `psycopg2-binary`: The PostgreSQL driver for Python.
    * `amadeus`: The official Python SDK for the Amadeus Flight APIs.
    * `python-dotenv`: To securely manage environment variables (API keys, DB passwords).
    * `Faker`: To generate realistic dummy transactional data.
* **Visualization & Analytics:** **Power BI Desktop**

---

## 🚀 How to Run This Project (Local Setup Guide)

Follow these steps to set up and run the entire data pipeline on your local machine.

### Step 1: Prerequisites
Before you begin, ensure you have the following software installed:
* **PostgreSQL:** A running instance of PostgreSQL. A tool like **pgAdmin** is highly recommended.
* **Python 3:** Version 3.8 or newer.
* **Power BI Desktop:** Available for free from the Microsoft Store.
* **Amadeus Developer Account:** A free-tier account from [Amadeus for Developers](https://developers.amadeus.com/) to get your `API Key` and `API Secret`.

### Step 2: Database Setup
This project requires **two** separate databases to simulate a real-world environment.

1.  Using pgAdmin or `psql`, create two new, empty databases:
    * `db_oltp` (This will simulate the live transactional booking system)
    * `db_dwh` (This will be our final data warehouse)
2.  Open the `setup_database.sql` file from this repository.
3.  **Execute the first part** of the script (the `db_oltp` section) against your `db_oltp` database.
4.  **Execute the second part** of the script (the `db_dwh` section) against your `db_dwh` database.
    * This will create all the required tables (`Dim_Airport`, `Dim_Airline`, `Dim_Date`, `Fact_Flights`) and will also pre-populate the `Dim_Date` table with all necessary dates.

### Step 3: Environment & Dependencies

1.  **Install Python Libraries:**
    Create a `requirements.txt` file, paste the content below, and run `pip install -r requirements.txt`.
    ```txt
    pandas
    sqlalchemy
    psycopg2-binary
    amadeus
    python-dotenv
    Faker
    ```

2.  **Create `.env` File (CRITICAL):**
    In the root folder of the project, create a file named `.env`. This file will hold all your secrets and **must not** be uploaded to GitHub.
    ```
    # Amadeus API
    AMADEUS_KEY=YOUR_AMADEUS_API_KEY_HERE
    AMADEUS_SECRET=YOUR_AMADEUS_API_SECRET_HERE

    # PostgreSQL Database
    DB_USER=your_postgres_username
    DB_PASS=your_postgres_password
    ```

3.  **Create `data/` Folder:**
    In the root folder, create a new folder named `data`. Download your source CSV files (`flights.csv` and `airports.csv`) and place them inside this `data` folder.

4.  **Create `.gitignore` File:**
    To prevent uploading sensitive data and virtual environments, create a `.gitignore` file in the root folder with the following content:
    ```gitignore
    # Python
    __pycache__/
    venv/
    *.pyc

    # Data & Secrets
    .env
    data/
    *.csv
    *.json

    # Power BI
    *.pbit.backup
    ```

### Step 4: Execute the ETL Pipeline (In Order)

You must run the Python scripts in the correct sequence. Open your terminal in the project directory.

**1. Generate Dummy Data:**
*(This script populates your `db_oltp` with 5,000+ fake booking transactions).*
```bash
python "python script/generate_dummy_oltp.py"
```

**2. Run Extraction (to Bronze Layer):**
*(These scripts pull data from the `db_oltp` and Amadeus API, saving the raw results into a new `data/bronze/` folder).*
```bash
python "python script/extract_oltp.py"
python "python script/extract_api.py"
```

**3. Run Transform & Load (to Gold Layer):**
*(This is the main ETL script. It reads from the `data/` and `data/bronze/` folders, cleans and transforms the data into our Star Schema, and loads it into the final `db_dwh` database).*
```bash
python "python script/transform_and_load.py"
```
If this script completes successfully, your data warehouse is now fully populated.

### Step 5: View the Visualization

1.  Open the `Data Warehouse Visualization.pbix` file with Power BI Desktop.
2.  You will likely see a blank report. Click the **"Refresh"** button on the "Home" ribbon.
3.  Power BI will connect to your `localhost` PostgreSQL database (`db_dwh`), import the data, and populate all the visuals.
4.  Explore the dashboard and interact with the data-driven insights!
