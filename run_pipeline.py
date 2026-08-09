# -*- coding: utf-8 -*-
"""
run_pipeline.py -- TravelNusantara ETL Pipeline Orchestrator
============================================================
Executes all pipeline steps in the correct dependency order with
proper error propagation. If any step fails, the pipeline halts
immediately with a non-zero exit code.

Usage:
    python run_pipeline.py            -- full pipeline (all 5 steps)
    python run_pipeline.py --skip-api -- skip Amadeus API extraction (offline mode)

Steps:
    1. generate_source_data.py    -- synthesize airports/flights/reviews CSVs
    2. generate_dummy_oltp.py     -- populate db_oltp Bookings table
    3. extract_oltp.py            -- extract Bookings to Bronze CSV
    4. transform_and_load.py      -- Silver + Gold ETL into db_dwh
    5. ai_enrich_reviews.py       -- AI-enriched customer feedback -> Fact_Customer_Feedback
    [6. extract_api.py]           -- Amadeus API extraction (optional, skipped with --skip-api)
"""
import io
import subprocess
import sys
import time
import argparse
from datetime import datetime

# Force UTF-8 stdout so ANSI colors and emoji work on Windows CMD / PowerShell
# without a UnicodeEncodeError on cp1252 consoles.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Terminal color helpers (ANSI escape codes)
# ---------------------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def banner(text: str) -> None:
    """Prints a prominent section header."""
    print(f"\n{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}{CYAN}  {text}{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")


def run_step(step_name: str, cmd: list[str]) -> float:
    """
    Runs a single pipeline step as a subprocess.

    Prints the step name, streams output live, and returns elapsed time.
    Raises SystemExit with code 1 on any non-zero return code.

    Args:
        step_name: Human-readable name shown in pipeline logs.
        cmd: The subprocess command list (e.g. ['python', 'script.py']).

    Returns:
        Elapsed seconds as a float.
    """
    print(f"\n{BOLD}>>  {step_name}{RESET}")
    print(f"   Command: {' '.join(cmd)}")
    print(f"   Started: {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 50)

    t0 = time.perf_counter()
    result = subprocess.run(cmd, check=False)
    elapsed = time.perf_counter() - t0

    if result.returncode != 0:
        print(f"\n{RED}{BOLD}FAILED: '{step_name}' exited with code {result.returncode}.{RESET}")
        print(f"{RED}  Resolve the error above before re-running.{RESET}")
        sys.exit(1)

    print(f"{GREEN}OK  '{step_name}' completed in {elapsed:.1f}s{RESET}")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TravelNusantara ETL Pipeline Orchestrator"
    )
    parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Skip the Amadeus API extraction step (useful for offline runs)",
    )
    parser.add_argument(
        "--skip-source",
        action="store_true",
        help="Skip source data generation (use if CSVs already exist)",
    )
    args = parser.parse_args()

    # ---------------------------------------------------------------------------
    # Pipeline Step Definitions
    # ---------------------------------------------------------------------------
    PYTHON = [sys.executable]  # same interpreter that launched this script
    SCRIPT_DIR = "python script"

    ALL_STEPS: list[tuple[str, list[str], bool]] = [
        # (label, command, skip_flag)
        (
            "Step 1 -- Generate Synthetic Source Data",
            PYTHON + [f"{SCRIPT_DIR}/generate_source_data.py"],
            args.skip_source,
        ),
        (
            "Step 2 -- Populate OLTP Database (db_oltp)",
            PYTHON + [f"{SCRIPT_DIR}/generate_dummy_oltp.py"],
            False,
        ),
        (
            "Step 3 -- Extract OLTP Bookings to Bronze Layer",
            PYTHON + [f"{SCRIPT_DIR}/extract_oltp.py"],
            False,
        ),
        (
            "Step 4 -- Transform & Load to Gold Data Warehouse (db_dwh)",
            PYTHON + [f"{SCRIPT_DIR}/transform_and_load.py"],
            False,
        ),
        (
            "Step 5 -- AI Review Enrichment to Fact_Customer_Feedback",
            PYTHON + [f"{SCRIPT_DIR}/ai_enrich_reviews.py"],
            False,
        ),
        (
            "Step 6 -- Amadeus API Extraction to Bronze Layer (optional)",
            PYTHON + [f"{SCRIPT_DIR}/extract_api.py"],
            args.skip_api,
        ),
    ]

    # ---------------------------------------------------------------------------
    # Execution
    # ---------------------------------------------------------------------------
    banner("TravelNusantara ETL Pipeline -- Starting")
    print(f"  Start time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Skip API   : {args.skip_api}")
    print(f"  Skip Source: {args.skip_source}")

    pipeline_start = time.perf_counter()
    elapsed_per_step: list[tuple[str, float]] = []

    for label, cmd, skip in ALL_STEPS:
        if skip:
            print(f"\n{YELLOW}>>  Skipping: {label}{RESET}")
            continue
        elapsed = run_step(label, cmd)
        elapsed_per_step.append((label, elapsed))

    total = time.perf_counter() - pipeline_start

    # ---------------------------------------------------------------------------
    # Summary Report
    # ---------------------------------------------------------------------------
    banner("Pipeline Summary")
    for label, elapsed in elapsed_per_step:
        print(f"  OK  {label:<50} {elapsed:>6.1f}s")
    print("-" * 60)
    print(f"  {BOLD}Total pipeline time: {total:.1f}s{RESET}")
    print(f"\n{GREEN}{BOLD}All pipeline steps completed successfully!{RESET}")
    print(f"  Launch the analyst agent:  streamlit run app.py\n")


if __name__ == "__main__":
    main()
