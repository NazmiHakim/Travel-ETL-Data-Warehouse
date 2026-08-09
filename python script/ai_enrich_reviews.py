import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Base Directory Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
REVIEWS_FILE = os.path.join(DATA_DIR, "bronze", "customer_reviews.csv")

# Database Credentials
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "db_dwh")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "growtopia123")

def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def analyze_review_text(text):
    """
    LLM/Heuristic Enrichment Function:
    Analyzes raw review text to determine sentiment, complaint category, and satisfaction score.
    If GEMINI_API_KEY is available, calls Gemini API. Otherwise uses rule-based NLP parser.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        try:
            from google import genai
            client = genai.Client(api_key=api_key)
            prompt = (
                "Analyze the following flight review. Return JSON with keys:\n"
                "- sentiment: 'Positive', 'Neutral', or 'Negative'\n"
                "- complaint_category: 'Delay', 'Baggage', 'Service', 'Pricing', or 'None'\n"
                "- satisfaction_score: Integer 1 to 5\n\n"
                f"Review: \"{text}\""
            )
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )
            import json
            res_text = response.text.strip()
            if res_text.startswith("```json"):
                res_text = res_text.split("```json")[1].split("```")[0].strip()
            data = json.loads(res_text)
            return data["sentiment"], data["complaint_category"], int(data["satisfaction_score"])
        except Exception as e:
            print(f"Fallback to rule-based parser due to API error: {e}")

    # Rule-based NLP fallback for reliable execution without API keys
    text_lower = text.lower()
    if "delayed" in text_lower or "delay" in text_lower:
        return "Negative", "Delay", 1
    elif "bag" in text_lower or "luggage" in text_lower:
        return "Negative", "Baggage", 2
    elif "friendly" in text_lower or "helpful" in text_lower or "great" in text_lower:
        return "Positive", "None", 5
    elif "price" in text_lower or "expensive" in text_lower or "deal" in text_lower:
        if "deal" in text_lower or "fantastic" in text_lower:
            return "Positive", "Pricing", 5
        return "Negative", "Pricing", 2
    elif "poor" in text_lower or "ignored" in text_lower:
        return "Negative", "Service", 2
    else:
        return "Positive", "None", 4

def run_ai_enrichment():
    print("\n--- Starting AI-Powered Unstructured Review Enrichment ---")
    if not os.path.exists(REVIEWS_FILE):
        print(f"ERROR: {REVIEWS_FILE} not found. Please run generate_source_data.py first.")
        sys.exit(1)

    df_reviews = pd.read_csv(REVIEWS_FILE)
    print(f"Loaded {len(df_reviews)} raw customer review records.")

    engine = get_db_engine()

    # Load Dimension Keys for Lookup
    with engine.connect() as conn:
        dim_date = pd.read_sql("SELECT date_key, full_date FROM dim_date", conn)
        dim_airline = pd.read_sql("SELECT airline_key, carrier_code FROM dim_airline", conn)

    if dim_airline.empty:
        print("ERROR: dim_airline is empty. Please run transform_and_load.py first.")
        sys.exit(1)

    print("Analyzing unstructured review text (Extracting Sentiment & Category)...")
    enriched_rows = []
    for idx, row in df_reviews.iterrows():
        sentiment, category, score = analyze_review_text(row['raw_review_text'])
        enriched_rows.append({
            'review_date': row['review_date'],
            'carrier_code': row['carrier_code'],
            'sentiment': sentiment,
            'complaint_category': category,
            'satisfaction_score': score,
            'review_text': row['raw_review_text']
        })

    df_enriched = pd.DataFrame(enriched_rows)

    # Convert dates to match dim_date full_date format
    df_enriched['review_date'] = pd.to_datetime(df_enriched['review_date']).dt.date
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date']).dt.date

    # Perform Key Lookups
    df_merged = pd.merge(df_enriched, dim_date, left_on='review_date', right_on='full_date', how='inner')
    df_merged = pd.merge(df_merged, dim_airline, left_on='carrier_code', right_on='carrier_code', how='inner')

    final_columns = [
        'date_key', 'airline_key', 'sentiment', 'complaint_category',
        'satisfaction_score', 'review_text'
    ]
    df_final = df_merged[final_columns]

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Fact_Customer_Feedback (
        feedback_key SERIAL PRIMARY KEY,
        date_key INT REFERENCES Dim_Date(date_key),
        airline_key INT REFERENCES Dim_Airline(airline_key),
        sentiment VARCHAR(20),
        complaint_category VARCHAR(50),
        satisfaction_score INT,
        review_text TEXT
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.execute(text("TRUNCATE TABLE Fact_Customer_Feedback RESTART IDENTITY;"))
        conn.commit()

    
    df_final.to_sql('fact_customer_feedback', engine, if_exists='append', index=False)
    print("SUCCESS: Fact_Customer_Feedback table successfully populated!")

if __name__ == "__main__":
    run_ai_enrichment()
