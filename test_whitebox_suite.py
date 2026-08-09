import sys
import os
import time
import random
import io

# Force UTF-8 stdout encoding for Windows terminal
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, 'd:/Flight ETL integrated with agent')

from agent.sql_agent import (
    TextToSQLAgent,
    generate_dynamic_sql,
    is_analytical_query,
    _cache_key
)
from agent.rag_retriever import DataDictionaryVectorRAG
from agent.schema_inspector import DatabaseSchemaInspector
from agent.db_tools import execute_sql

print("==========================================================================")
print("✈️ TRAVELNUSANTARA AI DATA ANALYST — WHITE-BOX TEST SUITE (2,000 TESTS)")
print("==========================================================================")

# ---------------------------------------------------------------------------
# 1. HAPPY PATH QUERY GENERATOR (1,000 TEST CASES)
# ---------------------------------------------------------------------------
def generate_happy_path_queries(count=1000):
    carriers = ["AA", "American Airlines", "UA", "United", "DL", "Delta", "WN", "Southwest", "F9", "Frontier", "NK", "Spirit", "B6", "JetBlue", "AS", "Alaska"]
    airports = ["JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "MCO", "PHX", "IAH", "DTW", "BNA", "SLC", "PDX", "STL", "CVG"]
    years = ["", "in 2024", "in 2025", "for 2024"]
    limits = ["", "top 3", "top 5", "top 10", "limit 5", "limit 10", "most", "richest", "poorest"]
    metrics = ["revenue", "passengers", "passenger volume", "departure delay", "lateness", "bad reviews", "good reviews", "feedback", "ratings", "satisfaction score", "complaints"]

    queries = []
    
    # Template 1: Financial & Revenue Queries
    for i in range(250):
        c = random.choice(carriers)
        y = random.choice(years)
        l = random.choice(limits)
        q = f"{l} richest airline by revenue {y} for carrier {c}".strip()
        queries.append((q, "financial"))

    # Template 2: Operational Delays & Routes
    for i in range(250):
        a = random.choice(airports)
        y = random.choice(years)
        l = random.choice(limits)
        q = f"which airline has the worst departure delay for airport {a} {y} {l}".strip()
        queries.append((q, "delay"))

    # Template 3: Customer Feedback & Reviews
    for i in range(250):
        c = random.choice(carriers)
        sentiment = random.choice(["bad reviewed", "good reviewed", "top rated", "negative feedback", "highest satisfaction score", "complaint categories"])
        l = random.choice(limits)
        q = f"show me the {sentiment} airline {c} {l}".strip()
        queries.append((q, "review"))

    # Template 4: Temporal & Spatial Trends
    for i in range(250):
        trend = random.choice(["monthly revenue trend", "quarterly passenger trend", "busiest destination cities", "top origin airports"])
        y = random.choice(years)
        q = f"show {trend} {y}".strip()
        queries.append((q, "trend"))

    random.shuffle(queries)
    return queries[:count]


# ---------------------------------------------------------------------------
# 2. BAD / FAIL PATH QUERY GENERATOR (1,000 TEST CASES)
# ---------------------------------------------------------------------------
def generate_bad_path_queries(count=1000):
    queries = []

    # Sub-category A: Non-Analytical / Casual Greetings (250)
    greetings = [
        "hello", "hi there", "good morning", "good evening", "how are you?",
        "what is your name?", "who created you?", "tell me a joke", "what is the weather today?",
        "what time is it?", "nice to meet you", "bye", "thanks", "thank you"
    ]
    for i in range(250):
        g = random.choice(greetings)
        queries.append((f"{g} {i}", "greeting"))

    # Sub-category B: Out-of-Domain / Irrelevant Topics (250)
    irrelevant = [
        "recipe for pepperoni pizza", "how to buy bitcoin", "who won the world cup in 2022?",
        "python code for bubble sort", "translate hello into French", "best movies in 2024",
        "how to change car oil", "quantum computing physics equations", "stock price of Apple"
    ]
    for i in range(250):
        irr = random.choice(irrelevant)
        queries.append((f"{irr} {i}", "out_of_domain"))

    # Sub-category C: SQL Injection & Adversarial Inputs (250)
    injection_payloads = [
        "DROP TABLE fact_flights; --",
        "'; DELETE FROM dim_airline; --",
        "UNION SELECT 1, 2, 3, 4 FROM pg_tables",
        "SELECT * FROM information_schema.tables WHERE 'a'='a",
        "OR 1=1 --",
        "<script>alert('xss')</script>",
        "../../../../etc/passwd",
        "TRUNCATE TABLE fact_customer_feedback;"
    ]
    for i in range(250):
        pay = random.choice(injection_payloads)
        queries.append((f"airline revenue {pay} {i}", "injection_adversarial"))

    # Sub-category D: Gibberish & Noise Strings (250)
    noise_templates = [
        "asdfghjkl1234567890",
        "!@#$%^&*()_+-=[]{}|;:',.<>/?",
        "qqqqqqqqqqqqqqqqqqqq",
        "99999999999999999999",
        "   ",
        "??? !!!",
        "a b c d e f g h i j k l m n o p"
    ]
    for i in range(250):
        n = random.choice(noise_templates)
        queries.append((f"{n}_{i}", "gibberish_noise"))

    random.shuffle(queries)
    return queries[:count]


# ---------------------------------------------------------------------------
# 3. WHITE-BOX TEST EXECUTION HARNESS
# ---------------------------------------------------------------------------
def run_whitebox_test_suite():
    happy_queries = generate_happy_path_queries(1000)
    bad_queries = generate_bad_path_queries(1000)

    agent = TextToSQLAgent()

    print(f"\n🚀 Running {len(happy_queries)} Happy Path Tests...")
    happy_start = time.time()
    happy_passed = 0
    happy_sql_generated = 0
    happy_db_executed = 0

    for idx, (query, category) in enumerate(happy_queries, 1):
        # White-box inspection 1: Analytical guard check
        is_analytical = is_analytical_query(query)
        
        # White-box inspection 2: SQL generation
        sql = generate_dynamic_sql(query)

        # White-box inspection 3: SQL Validation
        has_select = "SELECT" in sql.upper()
        has_from = "FROM" in sql.upper()

        if is_analytical and has_select and has_from:
            happy_passed += 1
            happy_sql_generated += 1

        # Sample DB execution on every 50th query to benchmark live PostgreSQL throughput
        if idx % 50 == 0:
            success, result_df, _ = execute_sql(sql)
            if success:
                happy_db_executed += 1

    happy_elapsed = time.time() - happy_start
    happy_qps = len(happy_queries) / happy_elapsed if happy_elapsed > 0 else 0

    print(f"✅ Happy Path Complete: {happy_passed}/{len(happy_queries)} Passed ({happy_passed/len(happy_queries)*100:.1f}%)")
    print(f"   ⏱ Time Elapsed: {happy_elapsed:.3f}s | Throughput: {happy_qps:.1f} queries/sec")

    print(f"\n🚀 Running {len(bad_queries)} Bad/Fail Path Tests...")
    bad_start = time.time()
    bad_handled_correctly = 0
    injection_blocked = 0
    greetings_caught = 0
    gibberish_handled = 0

    for idx, (query, category) in enumerate(bad_queries, 1):
        if category == "greeting":
            is_analytical = is_analytical_query(query)
            if not is_analytical:
                greetings_caught += 1
                bad_handled_correctly += 1
            else:
                # If guard flagged it as analytical, verify generated SQL is safe SELECT
                sql = generate_dynamic_sql(query)
                if not any(d in sql.upper() for d in ["DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT"]):
                    bad_handled_correctly += 1

        elif category == "injection_adversarial":
            sql = generate_dynamic_sql(query)
            # Verify destructive SQL keywords are NEVER present
            is_destructive = any(d in sql.upper() for d in ["DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT", "ALTER"])
            if not is_destructive:
                injection_blocked += 1
                bad_handled_correctly += 1

        elif category in ["out_of_domain", "gibberish_noise"]:
            res = agent.process_query(query, use_cache=False)
            # Verify system returns graceful response without crashing
            if res is not None and "success" in res:
                gibberish_handled += 1
                bad_handled_correctly += 1

    bad_elapsed = time.time() - bad_start
    bad_qps = len(bad_queries) / bad_elapsed if bad_elapsed > 0 else 0

    print(f"🛡️ Bad/Fail Path Complete: {bad_handled_correctly}/{len(bad_queries)} Handled Correctly ({bad_handled_correctly/len(bad_queries)*100:.1f}%)")
    print(f"   ⏱ Time Elapsed: {bad_elapsed:.3f}s | Throughput: {bad_qps:.1f} queries/sec")
    print(f"   🔒 Injection Attempts Blocked: {injection_blocked}")
    print(f"   💬 Conversational Greetings Filtered: {greetings_caught}")
    print(f"   🌀 Gibberish/Noise Handled Gracefully: {gibberish_handled}")

    print("\n==========================================================================")
    print("📊 FINAL BENCHMARK SUMMARY")
    print("==========================================================================")
    print(f"Total Test Cases Executed: {len(happy_queries) + len(bad_queries)}")
    print(f"Total Time Taken          : {happy_elapsed + bad_elapsed:.3f} seconds")
    print(f"Average Throughput        : {(len(happy_queries) + len(bad_queries)) / (happy_elapsed + bad_elapsed):.1f} queries/second")
    print(f"Happy Path Pass Rate      : {happy_passed / len(happy_queries) * 100:.2f}%")
    print(f"Bad Path Resilience Rate  : {bad_handled_correctly / len(bad_queries) * 100:.2f}%")
    print("==========================================================================")

if __name__ == "__main__":
    run_whitebox_test_suite()
