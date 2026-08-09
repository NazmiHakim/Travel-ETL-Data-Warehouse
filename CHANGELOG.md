# Changelog

All notable changes to the TravelNusantara ETL & AI Agent project are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [0.4.0] — 2026-08-09: Production Hardening & Portfolio Polish

### Added
- `requirements.txt` with pinned minimum package versions for reproducible installs.
- `Dockerfile` (multi-stage build) and `docker-compose.yml` for one-command stack startup.
- `run_pipeline.py` — colored, timed ETL pipeline orchestrator with `--skip-api` and `--skip-source` flags.
- `.github/workflows/ci.yml` — GitHub Actions CI pipeline running tests against a real PostgreSQL service container.
- `Makefile` with targets: `setup`, `pipeline`, `test`, `test-unit`, `app`, `docker-up`, `docker-down`, `clean`.
- `conftest.py` — shared session-scoped pytest fixtures for all test modules.
- `agent/__init__.py` — marks `agent/` as a proper Python package.
- `CHANGELOG.md` (this file).

### Changed
- All pipeline scripts (`transform_and_load.py`, `generate_dummy_oltp.py`, `extract_oltp.py`, `extract_api.py`) translated from Indonesian to English.
- All pipeline functions annotated with full Python type hints.
- All pipeline functions documented with comprehensive docstrings.
- `test_routing.py` and `test_rag_schema.py` refactored from print-based runners to proper `pytest` parametrized test cases.
- `app.py`: replaced hardcoded Windows service name (`postgresql-x64-18`) with a dynamic, OS-neutral DB connection message.
- `README.md`: corrected hallucinated claims (Mode A local engine / Mode B Gemini routing trigger, schema column names, architecture diagram, file list).

### Fixed
- Star schema diagram corrected — `Fact_Flights.departure_delay` / `arrival_delay` names now match `setup_database.sql`.
- README architecture diagram removed fabricated "SCHEMA INSPECTION & DDL CONVERT" box.
- Test file directory listing corrected (removed non-existent `test_whitebox_100k.py`).

---

## [0.3.0] — 2026-08: Security Hardening & Documentation

### Added
- `.env` and `.env.example` for environment-based credential management (`python-dotenv`).
- `.gitignore` protection for all `.env` files.
- `MASTER_PROJECT_DOCUMENTATION.md` — phase-by-phase technical documentation.
- Mode A (local engine) deep-dive explanation added to `README.md`.

### Changed
- All 5 Python scripts migrated from hardcoded credentials to `os.getenv()` lookups.
- README terminology updated from "Neuro-Symbolic" to "Local/Offline Agent" for clarity.
- `Updating Flight ETL Documentation.md` sanitized to remove historical password references.

---

## [0.2.0] — 2026-08: AI Agent Integration

### Added
- `agent/sql_agent.py` — `TextToSQLAgent` with dual-mode routing (Mode A: local deterministic engine, Mode B: Gemini LLM optional upgrade).
- `agent/rag_retriever.py` — TF-IDF Vector RAG engine with entity normalization and domain scoring.
- `agent/schema_inspector.py` — live PostgreSQL `information_schema` introspection.
- `agent/db_tools.py` — SQLAlchemy singleton engine, read-only sandbox, security guards.
- `app.py` — Streamlit conversational UI with Plotly charts, chat history, and CSV download.
- `test_whitebox_suite.py` — 200,000-query stress test (measured: ~316 QPS).
- `test_whitebox_5k.py` — 10,000-case SQL assertion suite (5,000 happy path + 5,000 adversarial).
- Session-level SHA-256 query result cache (sub-millisecond cache hits).
- 3-cycle self-correction reflection loop for SQL execution errors.

### Changed
- `python script/ai_enrich_reviews.py` upgraded to call Gemini 2.5 Flash API for structured sentiment extraction (with rule-based NLP fallback).

---

## [0.1.0] — 2026-07: Initial ETL Pipeline

### Added
- `python script/generate_source_data.py` — synthetic airports, flights, and customer review CSV generator.
- `python script/generate_dummy_oltp.py` — Faker-based OLTP booking record generator (5,000 records).
- `python script/extract_oltp.py` — OLTP extraction to Bronze layer CSV.
- `python script/extract_api.py` — Amadeus Flight API extraction to Bronze JSON.
- `python script/transform_and_load.py` — Silver + Gold layer ETL (Medallion Architecture).
- `setup_database.sql` — DDL for `db_oltp` (Bookings) and `db_dwh` (Star Schema + Dim_Date seed).
- `Data Warehouse Visualization.pbix` — Power BI executive dashboard.
- Kimball Star Schema with `Dim_Airport`, `Dim_Airline`, `Dim_Date`, `Fact_Flights`, `Fact_Customer_Feedback`.
