"""
Master Execution Script for the Medallion Data Quality Architecture.

Runs:
1. End-to-End Medallion Pipeline (Bronze CSV -> Silver CSV/Parquet -> Gold CSV/Parquet)
2. DQ Health Scoring & Scorecard Calculation (Across all 6 DQ Dimensions)
3. Generates Interactive HTML Scorecard (`reports/dq_scorecard_report.html`)
4. Generates JSON and Markdown DQ Summaries (`reports/dq_summary.json`, `reports/dq_summary.md`)
5. Generates Official Data Documentation PDF (`docs/DATA_DOCUMENTATION.pdf`)
6. Runs Integration & Verification Test Suite with Allure Reports (`reports/allure-results`)
"""

import sys
import os
import subprocess
import time

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


def main():
    venv_python = sys.executable
    print("\n" + "#" * 80)
    print("  MEDALLION DATA QUALITY FRAMEWORK - COMPLETE RUNNER")
    print("#" * 80 + "\n")

    # Step 1: Run End-to-End Pipeline & Score Generator & PDF Generator
    print(">>> [STEP 1/2] Executing Medallion Pipeline, DQ Scoring & PDF Documentation...")
    start_time = time.time()
    pipeline_script = os.path.join(PROJECT_ROOT, "scripts", "run_pipeline.py")
    if not os.path.exists(pipeline_script):
        # Fallback to root run_pipeline.py if present
        pipeline_script = os.path.join(PROJECT_ROOT, "run_pipeline.py")

    pipe_res = subprocess.run(
        [venv_python, pipeline_script],
        cwd=PROJECT_ROOT,
        env=dict(os.environ, PYTHONPATH=PROJECT_ROOT)
    )
    if pipe_res.returncode != 0:
        print("[-] Pipeline execution failed.")
        sys.exit(pipe_res.returncode)

    # Step 2: Run Tests with Allure Results
    print("\n>>> [STEP 2/2] Running Automated Verification Test Suite with Allure...")
    test_file = os.path.join(PROJECT_ROOT, "tests", "test_keyword_framework.py")
    allure_dir = os.path.join(PROJECT_ROOT, "reports", "allure-results")
    
    test_res = subprocess.run(
        [venv_python, "-m", "pytest", test_file, f"--alluredir={allure_dir}", "-v"],
        cwd=PROJECT_ROOT,
        env=dict(os.environ, PYTHONPATH=PROJECT_ROOT)
    )
    if test_res.returncode != 0:
        print("[-] Test suite execution failed.")
        sys.exit(test_res.returncode)

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 80)
    print(f" ALL OPERATIONS COMPLETED IN {elapsed}s")
    print("=" * 80)
    print("Generated Governance, Documentation & Test Artifacts:")
    print("  1. Interactive HTML Scorecard : reports/dq_scorecard_report.html")
    print("  2. JSON Summary Output        : reports/dq_summary.json")
    print("  3. Markdown Summary Report    : reports/dq_summary.md")
    print("  4. Data Profiling JSON Report : reports/data_profiling_summary.json")
    print("  5. Data Documentation PDF     : docs/DATA_DOCUMENTATION.pdf")
    print("  6. Allure Test Report Data    : reports/allure-results/")
    print("  7. Medallion Physical Data    : data/bronze/ (CSV), data/silver/, data/gold/")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
