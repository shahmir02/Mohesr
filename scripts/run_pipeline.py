"""
Pipeline Orchestration Script for Medallion DQ Architecture.
Executes:
1. Source Ingestion & Statistical Data Profiling
2. Source -> Bronze Ingestion with 4-Way Cryptographic Reconciliation
3. Bronze -> Silver Cleansing, Standardization, Deduplication & Quarantine Routing
4. Silver -> Gold SCD Type 2 Dimensioning
5. Silver -> Gold 4-Way Consolidated Analytical 360 Mart
6. DQ Health Scoring (Overall + 6 DQ Dimensions)
7. Report Generation (HTML Scorecard, JSON, Markdown, and PDF Documentation)
"""

import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.keyword_dq_engine import KeywordDQEngine, DataQualityException
from src.medallion_pipeline import MedallionPipeline
from src.dq_score import DQScoreCalculator, DQHtmlReporter
from src.pdf_generator import generate_pdf_documentation
from src.generate_test_summary_csv import generate_test_summary_csv
from config.rules_config import (
    INSTITUTION_RULES, PROGRAM_RULES, STUDENT_RULES,
    ENROLLMENT_RULES, GOLD_BUSINESS_RULES
)


def print_banner(title: str):
    print("\n" + "=" * 80)
    print(f" {title.upper()}")
    print("=" * 80)


def run_medallion_pipeline():
    spark = SparkSession.builder \
        .appName("Medallion-DQ-Scoring-Pipeline") \
        .master("local[*]") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    pipeline = MedallionPipeline(spark)
    dq_engine = KeywordDQEngine()
    score_calc = DQScoreCalculator()

    print_banner("Medallion Architecture & Data Quality Scoring Pipeline")

    # Paths (relative to PROJECT_ROOT)
    base_data_path = os.path.join(PROJECT_ROOT, "data")
    src_dir = os.path.join(base_data_path, "source")
    brz_dir = os.path.join(base_data_path, "bronze")
    slv_dir = os.path.join(base_data_path, "silver")
    slv_clean_dir = os.path.join(base_data_path, "silver", "clean")
    gold_dir = os.path.join(base_data_path, "gold")
    gold_scd2_dir = os.path.join(base_data_path, "gold", "scd2_dimensions")
    gold_mart_dir = os.path.join(base_data_path, "gold", "analytics_marts")
    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    docs_dir = os.path.join(PROJECT_ROOT, "docs")
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(docs_dir, exist_ok=True)

    # =========================================================================
    # STAGE 1: SOURCE PROFILING
    # =========================================================================
    print_banner("Stage 1: Source Data Profiling")
    datasets = ["institution", "program", "student", "enrollment"]
    profiling_reports = {}
    
    for ds in datasets:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(os.path.join(src_dir, f"{ds}.csv"))
        prof = dq_engine.profile_data(df, dataset_name=ds.upper())
        profiling_reports[ds] = prof
        
        print(f"\n[PROFILE] Dataset: {prof['dataset_name']} (Records: {prof['total_records']}, Columns: {prof['total_columns']})")
        print(f"  {'Column':<20} | {'Type':<15} | {'Nulls (%)':<12} | {'Unique (%)':<14} | {'Min':<15} | {'Max':<15} | {'Avg':<8}")
        print("  " + "-" * 105)
        for col_name, stats in prof["columns"].items():
            type_short = stats['type'].replace("Type()", "").replace("Type", "")
            null_str = f"{stats['null_count']} ({stats['null_pct']}%)"
            uniq_str = f"{stats['distinct_count']} ({stats['uniqueness_pct']}%)"
            min_str = str(stats['min']) if stats['min'] is not None else "-"
            max_str = str(stats['max']) if stats['max'] is not None else "-"
            avg_str = str(stats['avg']) if stats['avg'] is not None else "-"
            print(f"  {col_name:<20} | {type_short:<15} | {null_str:<12} | {uniq_str:<14} | {min_str:<15} | {max_str:<15} | {avg_str:<8}")

    # Export Data Profiling Reports
    profiling_json_path = os.path.join(reports_dir, "data_profiling_summary.json")
    with open(profiling_json_path, "w") as f:
        json.dump(profiling_reports, f, indent=2)
    print(f"\n[✓] Detailed Data Profiling JSON exported: {profiling_json_path}")

    # =========================================================================
    # STAGE 2: SOURCE TO BRONZE INGESTION & 4-WAY RECONCILIATION
    # =========================================================================
    print_banner("Stage 2: Source to Bronze Ingestion & 4-Way Reconciliation")
    bronze_tables = {}
    
    for ds in datasets:
        src_path = os.path.join(src_dir, f"{ds}.csv")
        src_df, brz_df, recon = pipeline.ingest_source_to_bronze(src_path, table_name=ds, score_calculator=score_calc)
        bronze_tables[ds] = brz_df
        pipeline.save_layer_data(brz_df, os.path.join(brz_dir, ds), format_type="csv")
        
        print(f"[RECONCILIATION] {ds.upper():<12} -> Schema: {'PASS' if recon['schema_check']['passed'] else 'FAIL'} | Count: {'PASS' if recon['record_count_check']['passed'] else 'FAIL'} | Checksum: {'PASS' if recon['checksum_check']['passed'] else 'FAIL'}")

    # =========================================================================
    # STAGE 3: BRONZE TO SILVER CLEANSING & TRANSFORMATION
    # =========================================================================
    print_banner("Stage 3: Bronze to Silver Transformation & Cleansing")
    
    # 3.1 Institution
    clean_inst, quar_inst = pipeline.transform_bronze_to_silver(
        bronze_tables["institution"], "institution", INSTITUTION_RULES, score_calculator=score_calc
    )
    pipeline.save_layer_data(clean_inst, os.path.join(slv_dir, "institution"), format_type="both")
    pipeline.save_layer_data(clean_inst, os.path.join(slv_clean_dir, "institution"), format_type="both")
    print(f"[SILVER] Institution -> Clean: {clean_inst.count():<2} | Validated & Cleaned (Quarantine Storage Removed)")

    # 3.2 Program
    clean_prg, quar_prg = pipeline.transform_bronze_to_silver(
        bronze_tables["program"], "program", PROGRAM_RULES, context={"institution": clean_inst}, score_calculator=score_calc
    )
    pipeline.save_layer_data(clean_prg, os.path.join(slv_dir, "program"), format_type="both")
    pipeline.save_layer_data(clean_prg, os.path.join(slv_clean_dir, "program"), format_type="both")
    print(f"[SILVER] Program     -> Clean: {clean_prg.count():<2} | Validated & Cleaned (Quarantine Storage Removed)")

    # 3.3 Student
    clean_stu, quar_stu = pipeline.transform_bronze_to_silver(
        bronze_tables["student"], "student", STUDENT_RULES, context={"institution": clean_inst}, score_calculator=score_calc
    )
    pipeline.save_layer_data(clean_stu, os.path.join(slv_dir, "student"), format_type="both")
    pipeline.save_layer_data(clean_stu, os.path.join(slv_clean_dir, "student"), format_type="both")
    print(f"[SILVER] Student     -> Clean: {clean_stu.count():<2} | Validated & Cleaned (Quarantine Storage Removed)")

    # 3.4 Enrollment
    enrollment_context = {
        "student": clean_stu,
        "institution": clean_inst,
        "program": clean_prg
    }
    clean_enr, quar_enr = pipeline.transform_bronze_to_silver(
        bronze_tables["enrollment"], "enrollment", ENROLLMENT_RULES, context=enrollment_context, score_calculator=score_calc
    )
    pipeline.save_layer_data(clean_enr, os.path.join(slv_dir, "enrollment"), format_type="both")
    pipeline.save_layer_data(clean_enr, os.path.join(slv_clean_dir, "enrollment"), format_type="both")
    print(f"[SILVER] Enrollment  -> Clean: {clean_enr.count():<2} | Validated & Cleaned (Quarantine Storage Removed)")

    # =========================================================================
    # STAGE 4: SILVER TO GOLD SCD TYPE 2 DIMENSION (STUDENT)
    # =========================================================================
    print_banner("Stage 4: Silver to Gold SCD Type 2 Dimension Processing")
    scd2_stu_gold = pipeline.build_scd_type_2_gold(clean_stu, natural_keys=["Student_ID"])
    pipeline.save_layer_data(scd2_stu_gold, os.path.join(gold_dir, "dim_student_scd2"), format_type="both")
    pipeline.save_layer_data(scd2_stu_gold, os.path.join(gold_scd2_dir, "dim_student_scd2"), format_type="both")
    print(f"[GOLD SCD2] Dim Student Records Generated: {scd2_stu_gold.count()}")

    # =========================================================================
    # STAGE 5: SILVER TO GOLD 1 CONSOLIDATED WIDE ANALYTICS TABLE
    # =========================================================================
    print_banner("Stage 5: Silver to Gold 1 Wide Consolidated Analytics Table")
    gold_wide_clean, gold_wide_quar = pipeline.build_gold_wide_table(
        silver_students=clean_stu,
        silver_enrollments=clean_enr,
        silver_institutions=clean_inst,
        silver_programs=clean_prg,
        gold_rules=GOLD_BUSINESS_RULES,
        score_calculator=score_calc
    )
    pipeline.save_layer_data(gold_wide_clean, os.path.join(gold_dir, "fact_student_enrollment_wide"), format_type="both")
    pipeline.save_layer_data(gold_wide_clean, os.path.join(gold_mart_dir, "fact_student_enrollment_wide"), format_type="both")
    print(f"[GOLD MART] Analytical Records: {gold_wide_clean.count()} | Verified & Persisted (Quarantine Storage Removed)")

    # =========================================================================
    # STAGE 6: DQ SCORING & GOVERNANCE REPORTS
    # =========================================================================
    print_banner("Stage 6: Data Quality Scoring & Governance Reports")
    summary = score_calc.calculate_scores()

    print("\n+==============================================================================+")
    print("|                         DATA QUALITY EXECUTIVE SCOREBOARD                    |")
    print("+==============================================================================+")
    print(f"  OVERALL DQ HEALTH SCORE : {summary['overall_score']}%  [ RATING: {summary['overall_grade']} ]")
    print(f"  Total Rules Checked     : {summary['total_rules_evaluated']}")
    print(f"  Clean Passing Checks    : {summary['total_checks_passed']}")
    print(f"  Quarantined / Anomaly   : {summary['total_checks_failed']}")
    print("+------------------------------------------------------------------------------+")
    print("  DIMENSION SCORES BREAKDOWN:")
    for dim, dinfo in summary["dimension_scores"].items():
        bar_len = int(dinfo["score"] / 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        print(f"    - {dim:<23}: {dinfo['score']:>6.2f}% [{bar}] ({dinfo['grade']})")
    print("+------------------------------------------------------------------------------+")
    print("  MEDALLION LAYERS BREAKDOWN:")
    for lyr, linfo in summary["layer_scores"].items():
        print(f"    - {lyr:<12} Layer      : {linfo['score']:>6.2f}% ({linfo['passed_checks']}/{linfo['total_checks']} checks clean)")
    print("+==============================================================================+")

    # Generate HTML, JSON, Markdown, and PDF Reports
    html_path = os.path.join(reports_dir, "dq_scorecard_report.html")
    json_path = os.path.join(reports_dir, "dq_summary.json")
    md_path = os.path.join(reports_dir, "dq_summary.md")
    pdf_path = os.path.join(docs_dir, "DATA_DOCUMENTATION.pdf")

    DQHtmlReporter.generate_html_report(summary, html_path)
    print(f"[✓] HTML Report generated : {html_path}")

    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[✓] JSON Summary exported : {json_path}")

    with open(md_path, "w") as f:
        f.write("# Data Quality & Governance Executive Summary\n\n")
        f.write(f"- **Overall DQ Health Score:** {summary['overall_score']}%\n")
        f.write(f"- **Rating:** {summary['overall_grade']}\n")
        f.write(f"- **Total Rules Checked:** {summary['total_rules_evaluated']}\n")
        f.write(f"- **Passing Checks:** {summary['total_checks_passed']}\n")
        f.write(f"- **Quarantined / Violations:** {summary['total_checks_failed']}\n\n")
        f.write("## Dimension Breakdown\n\n")
        for dim, dinfo in summary["dimension_scores"].items():
            f.write(f"- **{dim}:** {dinfo['score']}% ({dinfo['grade']})\n")
    print(f"[✓] Markdown Summary      : {md_path}")

    generate_pdf_documentation(pdf_path)
    print(f"[✓] Data Documentation PDF: {pdf_path}")

    # Generate Test Summary CSV
    generate_test_summary_csv()
    print(f"[✓] Test Cases Summary CSV: docs/dq_test_cases_summary.csv")
    print("+==============================================================================+\n")

    return summary


if __name__ == "__main__":
    run_medallion_pipeline()
