import os
import re
import hashlib
import pandas as pd
from agent.db_tools import get_schema_info, execute_sql
from agent.rag_retriever import DataDictionaryVectorRAG
from agent.schema_inspector import DatabaseSchemaInspector


def _wm(keywords: list, text: str) -> bool:
    """
    Word-boundary keyword match.
    Prevents 'poor' matching inside 'poorest', 'delay' inside 'delayed' as suffix, etc.
    Uses regex \\b boundaries so only whole words trigger a domain match.
    """
    for k in keywords:
        # Multi-word phrases need a lookahead/lookbehind approach
        pattern = r'(?<![\w])' + re.escape(k) + r'(?![\w])'
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False

# ---------------------------------------------------------------------------
# Keyword Dictionaries -- used for both intent routing and analytical guard
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Keyword Dictionaries -- enriched for intent routing and analytical guard
# ---------------------------------------------------------------------------
REVENUE_KEYWORDS  = [
    "richest", "rich", "wealthiest", "wealthy", "revenue", "money",
    "earnings", "grossing", "gross", "income", "sales", "profitable",
    "profit", "profits", "valuable", "earn", "earning", "turnover",
    "fare", "fares", "pricing", "yield", "financial", "cash",
    "richest to poorest", "top grossing", "cheapest"
]

DELAY_KEYWORDS    = [
    "delay", "delays", "departure delay", "arrival delay", "depdelay",
    "arrdelay", "late", "lateness", "tardy", "tardiness", "lag",
    "punctual", "punctuality", "on time", "ontime", "behind schedule",
    "wait time", "cancellation", "cancelled", "disruption"
]

REVIEW_KEYWORDS   = [
    "complaint", "complaints", "feedback", "review", "reviews", "reviewed",
    "sentiment", "satisfaction", "csat", "nps", "rating", "ratings", "rated",
    "score", "scores", "opinion", "opinions", "comment", "comments",
    "bad", "negative", "poor", "unhappy", "angry", "terrible",
    "awful", "worst", "good", "positive", "best", "great", "excellent", "star", "stars",
    "luggage", "baggage", "service", "staff", "food", "meal", "wifi", "comfort"
]

LOCATION_KEYWORDS = [
    "city", "cities", "airport", "airports", "destination", "destinations",
    "dest", "origin", "origins", "route", "routes", "location", "locations",
    "hub", "hubs", "terminal", "where", "from", "to"
]

PASSENGER_KEYWORDS = [
    "passenger", "passengers", "pax", "traveler", "travelers",
    "traveller", "travellers", "seat", "seats", "capacity", "traffic",
    "volume", "busiest", "crowded", "popular", "booking", "bookings",
    "customer", "customers", "headcount", "people"
]

TIME_KEYWORDS     = [
    "month", "monthly", "quarter", "quarterly", "year", "yearly",
    "annual", "annually", "trend", "trends", "timeline", "seasonality",
    "over time", "ytd", "per month", "per year", "per quarter"
]

GENERAL_KEYWORDS  = [
    "airline", "airlines", "carrier", "carriers", "flight", "flights",
    "show", "list", "which", "top", "most", "highest", "lowest", "rank",
    "ranking", "report", "summary", "breakdown"
]

PERFORMANCE_KEYWORDS = [
    "performance", "performing", "overall", "composite", "combined",
    "comprehensive", "all-around", "all around", "holistic",
    "score", "index", "benchmark", "kpi", "metrics",
    "best to worst", "worst to best", "rank", "ranking", "ranked",
    "compare", "comparison", "versus", "vs",
]

ALL_ANALYTICAL = (
    REVENUE_KEYWORDS + DELAY_KEYWORDS + REVIEW_KEYWORDS +
    LOCATION_KEYWORDS + PASSENGER_KEYWORDS + TIME_KEYWORDS +
    GENERAL_KEYWORDS + PERFORMANCE_KEYWORDS
)

# ---------------------------------------------------------------------------
# Session-level query result cache
# key  = SHA-256 hash of (user_question.lower().strip())
# value = previously computed result dict
# ---------------------------------------------------------------------------
_query_cache: dict[str, dict] = {}


def _cache_key(question: str) -> str:
    return hashlib.sha256(question.lower().strip().encode()).hexdigest()


def is_analytical_query(user_question: str) -> bool:
    """Returns True when the question contains at least one analytical keyword."""
    return _wm(ALL_ANALYTICAL, user_question)


# ---------------------------------------------------------------------------
# Dynamic NLP SQL Generator
# Receives ONLY the user question -- never the full system prompt.
# ---------------------------------------------------------------------------
_GLOBAL_RAG_ENGINE = DataDictionaryVectorRAG()

def generate_dynamic_sql(user_question: str) -> str:
    """
    RAG-grounded NLP engine to translate natural language into valid SQL queries
    for db_dwh PostgreSQL schema without requiring external API calls.
    """
    # Entity normalization via RAG (e.g. JFK -> New York, AA -> American Airlines)
    rag_norm = _GLOBAL_RAG_ENGINE
    entities = rag_norm.normalize_entities(user_question)
    domain_scores = rag_norm.get_domain_scores(user_question)
    feedback_score = domain_scores.get("customer_feedback", 0.0)
    delay_score = domain_scores.get("flight_delays", 0.0)


    normalized_question = user_question
    for token, norm_val in entities.items():
        normalized_question = re.sub(r'\b' + re.escape(token) + r'\b', norm_val, normalized_question, flags=re.IGNORECASE)

    p = normalized_question.lower()

    # --- 1. Limit Parsing ---
    # "all" keyword explicitly removes the LIMIT cap so the full dataset is returned.
    # "top N" / "limit N" sets exact LIMIT. Standalone superlatives default to LIMIT 1.
    no_limit = _wm(["all", "every", "entire"], p) and not re.search(r"\b(?:top|limit)\s+\d+", p)
    limit = 10  # default: return top 10 for unspecified requests
    m = re.search(r"\b(?:top|limit)\s+(\d+)\b", p)
    if no_limit:
        limit = 50   # effectively "show all" -- use a generous cap to avoid unbounded queries
    elif m:
        limit = int(m.group(1))
    elif _wm(["10"], p) and _wm(["airline", "airlines", "carrier", "carriers", "city", "cities", "route", "routes"], p):
        limit = 10
    elif _wm(["3", "three"], p) and _wm(["airline", "airlines", "carrier", "carriers", "city", "cities"], p):
        limit = 3
    elif _wm(["5", "five"], p) and _wm(["airline", "airlines", "carrier", "carriers", "city", "cities"], p):
        limit = 5
    elif _wm(["most", "richest", "highest", "best", "worst", "busiest"], p) and not _wm(["10", "5", "3", "top", "list", "all", "rank", "ranking"], p):
        limit = 1
    limit_clause = f"LIMIT {limit}"

    # --- 2. Sort Direction ---
    order_dir = "DESC"
    if any(phrase in p for phrase in ["richest to poorest", "highest to lowest", "best to worst", "top to bottom"]):
        order_dir = "DESC"
    elif any(phrase in p for phrase in ["poorest to richest", "lowest to highest", "worst to best"]):
        order_dir = "ASC"
    elif _wm(["lowest", "least", "min", "bottom", "cheapest", "fewest", "poorest"], p) and not _wm(["richest", "highest", "most", "rich"], p):
        order_dir = "ASC"

    # --- 3. Year / Time Range Filter ---
    year_condition = ""
    if "2024" in p:
        year_condition = "dd.year = 2024"
    elif "2025" in p:
        year_condition = "dd.year = 2025"
    year_where = f"WHERE {year_condition}" if year_condition else ""

    # --- 4. Sort Column Selection ---
    primary_sort = "total_revenue"
    if _wm(PASSENGER_KEYWORDS, p) and not _wm(REVENUE_KEYWORDS, p):
        primary_sort = "total_passengers"
    elif (_wm(DELAY_KEYWORDS, p) or delay_score >= 0.15) and not _wm(REVENUE_KEYWORDS, p):
        primary_sort = "avg_departure_delay"

    # =========================================================================
    # DOMAIN ROUTING RULES (Vector RAG + Lexical Grounding)
    # =========================================================================

    # --- Domain 0: Composite Airline Performance (v_airline_performance VIEW) ---
    # Triggered when the query contains performance/ranking/overall keywords AND
    # references airlines -- routes to the composite performance view directly.
    is_performance_query = _wm(PERFORMANCE_KEYWORDS, p) and (
        _wm(["airline", "airlines", "carrier", "carriers"], p)
        or not _wm(LOCATION_KEYWORDS + ["route", "city", "cities", "destination"], p)
    )
    if is_performance_query and not _wm(DELAY_KEYWORDS + REVENUE_KEYWORDS, p):
        return (
            f"SELECT airline_name, carrier_code, "
            f"performance_score, performance_rank, "
            f"otp_percentage, avg_departure_delay, "
            f"total_revenue, avg_satisfaction, total_reviews "
            f"FROM v_airline_performance "
            f"ORDER BY performance_rank ASC "
            f"{limit_clause};"
        )

    # --- Domain A: Operational Flight Delays ---
    # Checked before reviews if delay/lateness keywords or high delay vector scores are present
    if (_wm(DELAY_KEYWORDS, p) or delay_score >= 0.15) and not _wm(["review", "reviews", "feedback", "rating", "csat", "nps", "sentiment"], p) and not _wm(REVENUE_KEYWORDS, p):
        if _wm(["route", "routes", "city", "cities", "destination", "origin"], p):
            return (
                f"SELECT da_orig.city AS origin, da_dest.city AS destination, "
                f"ROUND(AVG(ff.departure_delay)::numeric, 2) AS avg_departure_delay, "
                f"ROUND(AVG(ff.arrival_delay)::numeric, 2) AS avg_arrival_delay "
                f"FROM fact_flights ff "
                f"JOIN dim_airport da_orig ON ff.origin_airport_key = da_orig.airport_id_key "
                f"JOIN dim_airport da_dest ON ff.dest_airport_key = da_dest.airport_id_key "
                f"JOIN dim_date dd ON ff.date_key = dd.date_key "
                f"{year_where} "
                f"GROUP BY da_orig.city, da_dest.city "
                f"ORDER BY avg_departure_delay {order_dir} {limit_clause};"
            )
        else:
            return (
                f"SELECT da.airline_name, "
                f"ROUND(AVG(ff.departure_delay)::numeric, 2) AS avg_departure_delay, "
                f"ROUND(AVG(ff.arrival_delay)::numeric, 2) AS avg_arrival_delay "
                f"FROM fact_flights ff "
                f"JOIN dim_airline da ON ff.airline_key = da.airline_key "
                f"JOIN dim_date dd ON ff.date_key = dd.date_key "
                f"{year_where} "
                f"GROUP BY da.airline_name "
                f"ORDER BY avg_departure_delay {order_dir} {limit_clause};"
            )

    # --- Domain B: Customer Reviews & Feedback (Vector Grounded) ---
    is_review_domain = _wm(REVIEW_KEYWORDS, p) or feedback_score >= 0.12
    if is_review_domain and not (_wm(REVENUE_KEYWORDS, p) and _wm(["richest", "poorest", "revenue", "money"], p)):
        if _wm(["bad", "negative", "poor", "worst", "complaint", "complaints", "grievance", "unhappy"], p):
            if _wm(["airline", "carrier", "airlines", "carriers"], p):
                return (
                    f"SELECT da.airline_name, "
                    f"COUNT(fcf.feedback_key) AS negative_reviews, "
                    f"ROUND(AVG(fcf.satisfaction_score)::numeric, 2) AS avg_satisfaction "
                    f"FROM fact_customer_feedback fcf "
                    f"JOIN dim_airline da ON fcf.airline_key = da.airline_key "
                    f"WHERE fcf.sentiment = 'Negative' OR fcf.satisfaction_score <= 2 "
                    f"GROUP BY da.airline_name "
                    f"ORDER BY negative_reviews {order_dir} {limit_clause};"
                )
            else:
                return (
                    f"SELECT complaint_category, "
                    f"COUNT(*) AS negative_count, "
                    f"ROUND(AVG(satisfaction_score)::numeric, 2) AS avg_satisfaction "
                    f"FROM fact_customer_feedback "
                    f"WHERE sentiment = 'Negative' "
                    f"GROUP BY complaint_category "
                    f"ORDER BY negative_count {order_dir} {limit_clause};"
                )
        elif _wm(["good", "best", "positive", "highly rated", "top rated", "great", "excellent"], p):
            if _wm(["airline", "carrier", "airlines", "carriers"], p):
                return (
                    f"SELECT da.airline_name, "
                    f"COUNT(fcf.feedback_key) AS positive_reviews, "
                    f"ROUND(AVG(fcf.satisfaction_score)::numeric, 2) AS avg_satisfaction "
                    f"FROM fact_customer_feedback fcf "
                    f"JOIN dim_airline da ON fcf.airline_key = da.airline_key "
                    f"WHERE fcf.sentiment = 'Positive' OR fcf.satisfaction_score >= 4 "
                    f"GROUP BY da.airline_name "
                    f"ORDER BY avg_satisfaction {order_dir}, positive_reviews {order_dir} {limit_clause};"
                )
            else:
                return (
                    f"SELECT complaint_category, "
                    f"COUNT(*) AS positive_count, "
                    f"ROUND(AVG(satisfaction_score)::numeric, 2) AS avg_satisfaction "
                    f"FROM fact_customer_feedback "
                    f"WHERE sentiment = 'Positive' OR satisfaction_score >= 4 "
                    f"GROUP BY complaint_category "
                    f"ORDER BY avg_satisfaction {order_dir} {limit_clause};"
                )
        elif _wm(["sentiment", "rating", "score", "csat"], p):
            return (
                f"SELECT sentiment, COUNT(*) AS total_reviews, "
                f"ROUND(AVG(satisfaction_score)::numeric, 2) AS avg_satisfaction "
                f"FROM fact_customer_feedback "
                f"GROUP BY sentiment ORDER BY total_reviews {order_dir};"
            )
        elif _wm(["airline", "carrier", "airlines"], p):
            return (
                f"SELECT da.airline_name, "
                f"COUNT(fcf.feedback_key) AS total_reviews, "
                f"ROUND(AVG(fcf.satisfaction_score)::numeric, 2) AS avg_satisfaction "
                f"FROM fact_customer_feedback fcf "
                f"JOIN dim_airline da ON fcf.airline_key = da.airline_key "
                f"GROUP BY da.airline_name "
                f"ORDER BY total_reviews {order_dir} {limit_clause};"
            )
        else:
            return (
                f"SELECT complaint_category, sentiment, COUNT(*) AS review_count "
                f"FROM fact_customer_feedback "
                f"GROUP BY complaint_category, sentiment "
                f"ORDER BY review_count {order_dir} {limit_clause};"
            )

    # --- Domain C: Location / Destination / Airport Volume ---
    elif _wm(LOCATION_KEYWORDS, p) and not _wm(REVENUE_KEYWORDS, p):
        if _wm(["destination", "dest", "city", "cities", "to"], p):
            return (
                f"SELECT da_dest.city AS destination_city, da_dest.name AS airport_name, "
                f"SUM(ff.total_passengers) AS total_passengers, "
                f"SUM(ff.total_revenue) AS total_revenue "
                f"FROM fact_flights ff "
                f"JOIN dim_airport da_dest ON ff.dest_airport_key = da_dest.airport_id_key "
                f"JOIN dim_date dd ON ff.date_key = dd.date_key "
                f"{year_where} "
                f"GROUP BY da_dest.city, da_dest.name "
                f"ORDER BY total_passengers {order_dir} {limit_clause};"
            )
        elif _wm(["origin", "from"], p):
            return (
                f"SELECT da_orig.city AS origin_city, da_orig.name AS airport_name, "
                f"SUM(ff.total_passengers) AS total_passengers "
                f"FROM fact_flights ff "
                f"JOIN dim_airport da_orig ON ff.origin_airport_key = da_orig.airport_id_key "
                f"GROUP BY da_orig.city, da_orig.name "
                f"ORDER BY total_passengers {order_dir} {limit_clause};"
            )
        else:
            return (
                f"SELECT da_orig.city AS origin_city, da_dest.city AS destination_city, "
                f"SUM(ff.total_passengers) AS total_passengers, "
                f"SUM(ff.total_revenue) AS total_revenue "
                f"FROM fact_flights ff "
                f"JOIN dim_airport da_orig ON ff.origin_airport_key = da_orig.airport_id_key "
                f"JOIN dim_airport da_dest ON ff.dest_airport_key = da_dest.airport_id_key "
                f"GROUP BY da_orig.city, da_dest.city "
                f"ORDER BY total_passengers {order_dir} {limit_clause};"
            )

    # --- Domain D: Time Series Trends ---
    elif _wm(TIME_KEYWORDS, p) and not _wm(["airline", "carrier"], p):
        if _wm(["quarter", "quarterly"], p):
            return (
                f"SELECT dd.year, dd.quarter, "
                f"SUM(ff.total_revenue) AS total_revenue, "
                f"SUM(ff.total_passengers) AS total_passengers "
                f"FROM fact_flights ff "
                f"JOIN dim_date dd ON ff.date_key = dd.date_key "
                f"GROUP BY dd.year, dd.quarter ORDER BY dd.year, dd.quarter ASC;"
            )
        else:
            return (
                f"SELECT dd.year, dd.month, "
                f"SUM(ff.total_revenue) AS total_revenue, "
                f"SUM(ff.total_passengers) AS total_passengers "
                f"FROM fact_flights ff "
                f"JOIN dim_date dd ON ff.date_key = dd.date_key "
                f"{year_where} "
                f"GROUP BY dd.year, dd.month ORDER BY dd.year, dd.month ASC;"
            )

    # --- Domain E: Financial & Airline Performance (Default Fallback) ---
    else:
        # Check if temporal trend requested within airline financial domain
        if _wm(TIME_KEYWORDS, p):
            return (
                f"SELECT dd.year, dd.month, "
                f"SUM(ff.total_revenue) AS total_revenue, "
                f"SUM(ff.total_passengers) AS total_passengers "
                f"FROM fact_flights ff "
                f"JOIN dim_date dd ON ff.date_key = dd.date_key "
                f"{year_where} "
                f"GROUP BY dd.year, dd.month ORDER BY dd.year, dd.month ASC;"
            )
        return (
            f"SELECT da.airline_name, da.carrier_code, "
            f"SUM(ff.total_revenue) AS total_revenue, "
            f"SUM(ff.total_passengers) AS total_passengers "
            f"FROM fact_flights ff "
            f"JOIN dim_airline da ON ff.airline_key = da.airline_key "
            f"JOIN dim_date dd ON ff.date_key = dd.date_key "
            f"{year_where} "
            f"GROUP BY da.airline_name, da.carrier_code "
            f"ORDER BY {primary_sort} {order_dir} {limit_clause};"
        )


def call_llm(full_prompt: str, user_question: str, api_key: str = None) -> tuple[str, bool]:
    """
    Calls Gemini API when a key is available and the call succeeds.
    Returns (response_text, used_api) where used_api=True only when the
    Gemini API call actually succeeds -- not just when a key string exists.
    Fallback: generate_dynamic_sql(user_question) -- NOT the full prompt.
    """
    key = api_key or os.getenv("GEMINI_API_KEY")
    if key:
        try:
            from google import genai
            client = genai.Client(api_key=key)
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=full_prompt,
            )
            return response.text, True   # API succeeded
        except Exception as e:
            print(f"Gemini API Error: {e}. Falling back to NLP engine.")
    return f"```sql\n{generate_dynamic_sql(user_question)}\n```", False


def extract_sql_code(text: str) -> str:
    """Extracts the first SQL block from a markdown-formatted response."""
    m = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"```\s*(SELECT.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    s = text.strip()
    if s.upper().startswith("SELECT") or s.upper().startswith("WITH"):
        return s.rstrip(";")
    return s


class TextToSQLAgent:
    def __init__(self):
        self.schema_info = get_schema_info()
        self._rag_engine = DataDictionaryVectorRAG()
        self._schema_inspector = DatabaseSchemaInspector()

    @property
    def schema_inspector(self):
        if not hasattr(self, "_schema_inspector") or self._schema_inspector is None:
            self._schema_inspector = DatabaseSchemaInspector()
        return self._schema_inspector

    @property
    def rag_engine(self):
        if not hasattr(self, "_rag_engine") or self._rag_engine is None:
            self._rag_engine = DataDictionaryVectorRAG()
        return self._rag_engine

    def process_query(
        self,
        user_question: str,
        api_key: str = None,
        max_retries: int = 3,
        use_cache: bool = True,
        **kwargs,
    ) -> dict:
        """
        Full Text-to-SQL pipeline with:
        - Non-analytical query guard
        - Vector RAG Data Dictionary Retrieval & Entity Normalization
        - Live Database Schema Inspection
        - Session-level result cache (skip DB round-trip for repeat questions)
        - Gemini API or dynamic NLP fallback
        - Reflection & Self-Correction loop
        - Query execution timing
        """
        logs = []
        logs.append(f"Received user prompt: '{user_question}'")

        # ---- Vector RAG Context Grounding -------------------------------
        rag_context = self.rag_engine.format_rag_context(user_question)
        logs.append(f"[Vector RAG Context Grounding]:\n{rag_context}")

        effective_key = api_key or os.getenv("GEMINI_API_KEY")

        # ---- Non-analytical guard ----------------------------------------
        if not effective_key and not is_analytical_query(user_question):
            return {
                "success": False,
                "sql_query": "",
                "data": None,
                "summary": (
                    "Hi! I'm TravelNusantara's AI Data Analyst.\n\n"
                    "I can answer questions about:\n"
                    "-  **Flight revenue & passengers** by airline\n"
                    "-  **Departure & arrival delays** by airline or route\n"
                    "-  **Top destination & origin cities**\n"
                    "- **Customer sentiment & complaint categories**\n\n"
                    "Try: *\"Which airline has the highest revenue?\"* "
                    "or *\"Show top 5 destination cities.\"*"
                ),
                "logs": logs,
                "is_greeting": True,
                "elapsed_seconds": 0.0,
            }

        # ---- Session result cache -----------------------------------------
        ck = _cache_key(user_question)
        if use_cache and ck in _query_cache:
            cached = _query_cache[ck].copy()
            cached["logs"] = [f"Cache hit -- returning stored result for: '{user_question}'"]
            cached["from_cache"] = True
            return cached

        if effective_key:
            logs.append("Attempting Gemini LLM API (gemini-2.5-flash)...")

        system_instruction = f"""You are an expert PostgreSQL Data Analyst for TravelNusantara.
Translate the user's question into a valid PostgreSQL query for database 'db_dwh'.

{self.schema_info}

{rag_context}

RULES:
1. Return ONLY valid PostgreSQL inside ```sql ... ```.
2. Never use DROP, UPDATE, DELETE, INSERT, ALTER, CREATE, TRUNCATE.
3. All table names are lowercase (fact_flights, dim_airline, dim_date, dim_airport, fact_customer_feedback).
4. For bad/negative reviews query fact_customer_feedback WHERE sentiment = 'Negative'.
5. Use ROUND(value::numeric, 2) for decimal rounding in PostgreSQL.
6. Always ORDER BY the primary metric DESC and add LIMIT unless user asks for all rows.
"""
        current_prompt = f"{system_instruction}\n\nUser Question: {user_question}"

        sql_query = ""
        success = False
        result_df = None
        error_msg = ""
        total_elapsed = 0.0

        for attempt in range(1, max_retries + 1):
            logs.append(f"--- Attempt {attempt}/{max_retries} ---")

            llm_response, used_api = call_llm(
                current_prompt, user_question=user_question, api_key=api_key
            )
            if attempt == 1:
                if used_api:
                    logs.append("Using Gemini LLM API (gemini-2.5-flash).")
                else:
                    logs.append("Using Dynamic NLP Pattern Engine.")
            sql_query = extract_sql_code(llm_response)
            logs.append(f"Generated SQL: `{sql_query}`")

            # execute_sql now returns (success, data_or_error, elapsed)
            success, result_df_or_err, elapsed = execute_sql(sql_query)
            total_elapsed += elapsed

            if success:
                result_df = result_df_or_err
                logs.append(
                    f"SUCCESS -- {len(result_df)} row(s) in {elapsed:.3f}s."
                )
                if len(result_df) == 0 and attempt < max_retries:
                    logs.append("0 rows -- relaxing filter conditions...")
                    current_prompt += (
                        f"\n\nAttempt {attempt}: 0 rows for:\n{sql_query}\n"
                        "Relax any date/filter conditions and regenerate."
                    )
                    continue
                break
            else:
                error_msg = str(result_df_or_err)
                # Break immediately for connectivity errors -- SQL changes cannot fix these
                if any(k in error_msg for k in ["Connection Refused", "not reachable", "OperationalError"]):
                    logs.append(f"DB CONNECTION ERROR -- aborting retries.")
                    break
                logs.append(f"Error: {error_msg}")
                logs.append("Triggering Reflection & Correction Loop...")
                current_prompt += (
                    f"\n\nAttempt {attempt} failed:\n{error_msg}\n"
                    f"Query was:\n{sql_query}\n"
                    "Fix the SQL and return corrected query inside ```sql ... ```."
                )

        # Build summary
        if success and result_df is not None and not result_df.empty:
            summary = self._synthesize_answer(user_question, result_df)
        elif success and result_df is not None and result_df.empty:
            summary = "The query ran successfully but returned 0 records for the given criteria."
        else:
            summary = (
                f"Unable to generate a valid query after {max_retries} attempt(s).\n\n"
                f"**Error:** {error_msg}"
            )

        result = {
            "success": success,
            "sql_query": sql_query,
            "data": result_df,
            "summary": summary,
            "logs": logs,
            "is_greeting": False,
            "from_cache": False,
            "elapsed_seconds": total_elapsed,
        }

        # Cache successful non-empty results
        if success and result_df is not None and not result_df.empty:
            _query_cache[ck] = result

        return result

    def _synthesize_answer(self, user_question: str, df: pd.DataFrame) -> str:
        n = len(df)
        summary = "### Analytical Summary\n"
        summary += f"Found **{n} record(s)** for: *\"{user_question}\"*.\n\n"
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            summary += "#### Key Highlights:\n"
            for col in numeric_cols:
                total = df[col].sum()
                avg = df[col].mean()
                title = col.replace("_", " ").title()
                if any(k in col.lower() for k in ["revenue", "price"]):
                    summary += f"- **Total {title}:** ${total:,.2f} *(avg ${avg:,.2f})*\n"
                elif any(k in col.lower() for k in ["passenger", "count", "review"]):
                    summary += f"- **Total {title}:** {total:,.0f}\n"
                elif any(k in col.lower() for k in ["delay", "satisfaction", "score"]):
                    summary += f"- **Avg {title}:** {avg:.2f}\n"
        return summary

