"""
conftest.py — Shared pytest fixtures for TravelNusantara test suite.

Fixtures defined here are automatically available to all test modules
without needing explicit imports.
"""
import pytest
from agent.sql_agent import TextToSQLAgent
from agent.rag_retriever import DataDictionaryVectorRAG
from agent.schema_inspector import DatabaseSchemaInspector


@pytest.fixture(scope="session")
def agent() -> TextToSQLAgent:
    """
    Session-scoped TextToSQLAgent instance.
    Created once per test session to avoid repeated initialization overhead.
    """
    return TextToSQLAgent()


@pytest.fixture(scope="session")
def rag() -> DataDictionaryVectorRAG:
    """
    Session-scoped DataDictionaryVectorRAG instance.
    The TF-IDF vectorizer is fitted once and shared across all RAG tests.
    """
    return DataDictionaryVectorRAG()


@pytest.fixture(scope="session")
def schema_inspector() -> DatabaseSchemaInspector:
    """
    Session-scoped DatabaseSchemaInspector instance.
    Used for live schema introspection tests (requires running PostgreSQL).
    """
    return DatabaseSchemaInspector()
