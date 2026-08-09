"""
test_routing.py — Intent Router Unit Tests
==========================================
Tests that generate_dynamic_sql() routes natural language queries
to the correct SQL template for each analytical domain.

All tests are purely in-process: no database connection required.
"""
import pytest
from agent.sql_agent import generate_dynamic_sql, _wm, REVIEW_KEYWORDS


# ---------------------------------------------------------------------------
# Parametrized routing correctness tests
# ---------------------------------------------------------------------------
ROUTING_CASES = [
    # (prompt, expected_token_in_sql, test_id)
    ("which airplane has the most passenger seat?",
     "total_passengers", "passenger-sort"),
    ("please provide me with 10 airline from richest to poorest based on revenue",
     "total_revenue", "richest-to-poorest-revenue"),
    ("which airline has the most bad reviews?",
     "fact_customer_feedback", "bad-reviews-routing"),
    ("which airline has the highest average departure delay?",
     "departure_delay", "delay-routing"),
    ("show monthly revenue trend",
     "dd.month", "monthly-trend"),
    ("top 3 busiest destination cities",
     "city", "busiest-destination"),
    ("what are the top complaint categories?",
     "complaint_category", "complaint-categories"),
    ("show quarterly revenue trend",
     "dd.quarter", "quarterly-trend"),
    ("airlines with the worst lateness",
     "departure_delay", "lateness-routing"),
    ("top grossing airlines",
     "total_revenue", "top-grossing"),
    ("which airline has the most positive reviews?",
     "fact_customer_feedback", "positive-reviews-routing"),
    ("show top 5 airlines by total revenue",
     "total_revenue", "top-5-airlines-revenue"),
]


@pytest.mark.parametrize("prompt,expected,test_id", ROUTING_CASES, ids=[c[2] for c in ROUTING_CASES])
def test_sql_routing(prompt: str, expected: str, test_id: str) -> None:
    """
    Verifies that generate_dynamic_sql() produces a SQL string containing
    the expected token for the given natural language prompt.
    """
    sql = generate_dynamic_sql(prompt)
    assert expected in sql, (
        f"[{test_id}] Expected '{expected}' in generated SQL.\n"
        f"  Prompt : {prompt}\n"
        f"  Got SQL: {sql[:200]}"
    )


# ---------------------------------------------------------------------------
# False-positive guard: ensure 'poorest' doesn't activate the REVIEW domain
# ---------------------------------------------------------------------------
def test_poorest_does_not_trigger_review_domain() -> None:
    """
    'richest to poorest' contains 'poor' as a substring, which could
    accidentally trigger REVIEW_KEYWORDS matching. The _wm() function
    uses word-boundary regex to prevent this.
    """
    assert not _wm(REVIEW_KEYWORDS, "richest to poorest"), (
        "'poorest' incorrectly matched REVIEW_KEYWORDS — word boundary guard failed."
    )


# ---------------------------------------------------------------------------
# LIMIT clause parsing tests
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("prompt,expected_limit", [
    ("top 5 airlines by revenue", "LIMIT 5"),
    ("show top 10 destination cities", "LIMIT 10"),
    ("which airline is richest?", "LIMIT 1"),
    ("show top 3 airlines by delay", "LIMIT 3"),
])
def test_limit_parsing(prompt: str, expected_limit: str) -> None:
    """Verifies that LIMIT clauses are correctly parsed from the user prompt."""
    sql = generate_dynamic_sql(prompt)
    assert expected_limit in sql.upper(), (
        f"Expected '{expected_limit}' in SQL for prompt: '{prompt}'\n"
        f"  Got: {sql}"
    )


# ---------------------------------------------------------------------------
# ORDER BY direction tests
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("prompt,expected_direction", [
    ("show the airline with the lowest revenue", "ASC"),
    ("which airline has the worst delays?", "DESC"),   # worst = highest delay value = DESC
    ("richest airline", "DESC"),
    ("airline with the most passengers", "DESC"),
])
def test_sort_direction(prompt: str, expected_direction: str) -> None:
    """Verifies that ORDER BY direction (ASC/DESC) is correctly inferred."""
    sql = generate_dynamic_sql(prompt)
    assert expected_direction in sql.upper(), (
        f"Expected ORDER BY {expected_direction} for prompt: '{prompt}'\n"
        f"  Got: {sql}"
    )
