# ============================================================
# TravelNusantara — Makefile
# Common development commands for the ETL + AI Agent project.
# Usage: make <target>
# ============================================================

.PHONY: help setup pipeline test test-unit test-integration app docker-up docker-down clean

# Default target — show help
help:
	@echo ""
	@echo "TravelNusantara — Available Commands:"
	@echo "--------------------------------------"
	@echo "  make setup          Install all Python dependencies"
	@echo "  make pipeline       Run the full ETL pipeline (all 5 steps)"
	@echo "  make pipeline-skip  Run the pipeline without Amadeus API extraction"
	@echo "  make test           Run the full test suite with coverage"
	@echo "  make test-unit      Run only non-integration (offline) tests"
	@echo "  make app            Launch the Streamlit AI Analyst app"
	@echo "  make docker-up      Start the full stack (PostgreSQL + Streamlit) in Docker"
	@echo "  make docker-down    Stop and remove Docker containers and volumes"
	@echo "  make clean          Remove __pycache__ directories and .pyc files"
	@echo ""

# Install all Python dependencies
setup:
	pip install --upgrade pip
	pip install -r requirements.txt

# Run the full ETL pipeline
pipeline:
	python run_pipeline.py

# Run pipeline without the Amadeus API step (offline mode)
pipeline-skip:
	python run_pipeline.py --skip-api

# Run the full test suite (requires live PostgreSQL)
test:
	pytest test_routing.py test_rag_schema.py test_whitebox_5k.py \
		--tb=short -v \
		--cov=agent \
		--cov-report=term-missing

# Run only offline unit tests (no PostgreSQL required)
test-unit:
	pytest test_routing.py test_rag_schema.py \
		-m "not integration" \
		--tb=short -v

# Launch the Streamlit AI Analyst app
app:
	streamlit run app.py

# Start full stack in Docker (PostgreSQL + Streamlit)
docker-up:
	docker-compose up --build

# Stop and remove Docker containers + volumes
docker-down:
	docker-compose down -v

# Clean build artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".coverage" -delete 2>/dev/null || true
	find . -name "coverage.xml" -delete 2>/dev/null || true
	@echo "Clean complete."
