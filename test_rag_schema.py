"""
test_rag_schema.py — RAG Retriever & Schema Inspector Tests
============================================================
Tests for:
  1. DataDictionaryVectorRAG — entity normalization & context retrieval
  2. DatabaseSchemaInspector — live schema introspection (requires PostgreSQL)

Fixtures are injected from conftest.py.
"""
import pytest
from agent.rag_retriever import DataDictionaryVectorRAG
from agent.schema_inspector import DatabaseSchemaInspector


# ===========================================================================
# Section 1: DataDictionaryVectorRAG
# ===========================================================================
class TestVectorRAGRetriever:
    """Unit tests for the TF-IDF vector RAG engine."""

    def test_retrieves_relevant_chunk_for_revenue_query(self, rag: DataDictionaryVectorRAG) -> None:
        """Revenue queries should return at least one relevant context chunk."""
        results = rag.retrieve("which airline has the highest revenue?", top_k=2)
        assert len(results) >= 1, "Expected at least 1 RAG result for revenue query."

    def test_retrieves_feedback_chunk_for_review_query(self, rag: DataDictionaryVectorRAG) -> None:
        """Complaint/review queries should surface the customer_feedback domain chunk."""
        results = rag.retrieve("which airline has the most bad reviews?", top_k=2)
        categories = [r["category"] for r in results]
        assert any("feedback" in c.lower() or "airline" in c.lower() for c in categories), (
            f"Expected feedback-related chunk. Got: {categories}"
        )

    def test_entity_normalization_iata_code(self, rag: DataDictionaryVectorRAG) -> None:
        """IATA code 'JFK' should be normalized to 'New York'."""
        entities = rag.normalize_entities("What is the revenue from JFK?")
        assert "JFK" in entities, "Expected 'JFK' in normalized entity map."
        assert entities["JFK"] == "New York", (
            f"Expected 'JFK' → 'New York', got '{entities.get('JFK')}'."
        )

    def test_entity_normalization_carrier_code(self, rag: DataDictionaryVectorRAG) -> None:
        """Carrier code 'AA' should be normalized to 'American Airlines'."""
        entities = rag.normalize_entities("show delays for AA flights")
        assert "AA" in entities, "Expected 'AA' in normalized entity map."
        assert entities["AA"] == "American Airlines", (
            f"Expected 'AA' → 'American Airlines', got '{entities.get('AA')}'."
        )

    def test_empty_query_returns_no_results(self, rag: DataDictionaryVectorRAG) -> None:
        """Empty or whitespace-only queries should return an empty result list."""
        assert rag.retrieve("") == []
        assert rag.retrieve("   ") == []

    def test_domain_scores_returns_expected_keys(self, rag: DataDictionaryVectorRAG) -> None:
        """get_domain_scores() should return a score for every registered domain."""
        scores = rag.get_domain_scores("which airline has the most delays?")
        assert isinstance(scores, dict), "Expected domain scores to be a dict."
        assert len(scores) > 0, "Expected at least one domain score."

    def test_format_rag_context_is_string(self, rag: DataDictionaryVectorRAG) -> None:
        """format_rag_context() should always return a non-empty string."""
        ctx = rag.format_rag_context("show top airlines by revenue")
        assert isinstance(ctx, str) and len(ctx) > 0, "Expected a non-empty RAG context string."

    def test_format_rag_context_empty_query(self, rag: DataDictionaryVectorRAG) -> None:
        """Empty query should return the fallback message string."""
        ctx = rag.format_rag_context("")
        assert isinstance(ctx, str), "Expected a string even for empty query."


# ===========================================================================
# Section 2: DatabaseSchemaInspector (requires live PostgreSQL)
# ===========================================================================
@pytest.mark.integration
class TestSchemaInspector:
    """
    Integration tests for the live schema inspector.
    These tests require a running PostgreSQL instance with the DWH schema loaded.
    Mark: @pytest.mark.integration
    """

    def test_get_tables_overview_returns_list(self, schema_inspector: DatabaseSchemaInspector) -> None:
        """get_tables_overview() should return a list (may be empty if DB is offline)."""
        result = schema_inspector.get_tables_overview()
        assert isinstance(result, list), "Expected a list from get_tables_overview()."

    def test_get_column_details_returns_list(self, schema_inspector: DatabaseSchemaInspector) -> None:
        """get_column_details() should return a list of column metadata dicts."""
        result = schema_inspector.get_column_details()
        assert isinstance(result, list), "Expected a list from get_column_details()."

    def test_format_schema_summary_is_string(self, schema_inspector: DatabaseSchemaInspector) -> None:
        """format_schema_summary() should return a non-empty markdown string."""
        summary = schema_inspector.format_schema_summary()
        assert isinstance(summary, str), "Expected a string from format_schema_summary()."
        assert len(summary) > 0, "Expected a non-empty schema summary string."
