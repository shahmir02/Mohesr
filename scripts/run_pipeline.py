"""
Pipeline Orchestration Script for Medallion DQ Architecture.

Layer layout (optimized — no dual writes):
  data/source/<table>.csv
  data/bronze/<table>/
  data/silver/clean/<table>/
  data/silver/quarantine/<table>/
  data/gold/scd2_dimensions/dim_student_scd2/
  data/gold/analytics_marts/fact_student_enrollment_wide/
  data/gold/analytics_marts/quarantine/fact_student_enrollment_wide/
"""

import sys
import os
import json
from pyspark.sql import SparkSession

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

DATASETS = ["institution", "program", "student", "enrollment"]


def print_banner(title: str):
    print("\n" + "=" * 80)
    print(f" {title.upper()}")
    print("=" * 80)


def layer_paths(base_data_path: str) -> dict:
    """Canonical medallion paths — single location per artifact."""
    return {
        "source": os.path.join(base_data_path, "source"),
        "bronze": os.path.join(base_data_path, "bronze"),
        "silver_clean": os.path.join(base_data_path, "silver", "clean"),
        "silver_quarantine": os.path.join(base_data_path, "silver", "quarantine"),
        "gold_scd2": os.path.join(base_data_path, "gold", "scd2_dimensions", "dim_student_scd2"),
        "gold_mart": os.path.join(base_data_path, "gold", "analytics_marts", "fact_student_enrollment_wide"),
        "gold_mart_quarantine": os.path.join(
            base_data_path, "gold", "analytics_marts", "quarantine", "fact_student_enrollment_wide"
        ),
    }


def save_silver(pipeline, clean_df, quar_df, table_name: str, paths: dict):
    """Write clean + quarantine once under silver/clean and silver/quarantine."""
    pipeline.save_layer_data(clean_df, os.path.join(paths["silver_clean"], table_name), format_type="both")
    pipeline.save_layer_data(quar_df, os.path.join(paths["silver_quarantine"], table_name), format_type="both")
    print(f"[SILVER] {table_name:<12} -> Clean: {clean_df.count():<4} | Quarantine: {quar_df.count():<4}")


def run_medallion_pipeline():
    spark = (
        SparkSession.builder
        .appName("Medallion-DQ-Scoring-Pipeline")
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    pipeline = MedallionPipeline(spark)
    dq_engine = KeywordDQEngine()
    score_calc = DQScoreCalculator()

    print_banner("Medallion Architecture & Data Quality Scoring Pipeline")

    base_data_path = os.path.join(PROJECT_ROOT, "data")
    paths = layer_paths(base_data_path)
    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    docs_dir = os.path.join(PROJECT_ROOT, "docs")
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(docs_dir, exist_ok=True)

    # -------------------------------------------------------------------------
    # STAGE 1: SOURCE PROFILING
    # -------------------------------------------------------------------------
    print_banner("Stage 1: Source Data Profiling")
    profiling_reports = {}

    for ds in DATASETS:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(
            os.path.join(paths["source"], f"{ds}.csv")
        )
        prof = dq_engine.profile_data(df, dataset_name=ds.upper())
        profiling_reports[ds] = prof
        print(f"\n[PROFILE] {prof['dataset_name']}: {prof['total_records']} rows, {prof['total_columns']} cols")

    profiling_json_path = os.path.join(reports_dir, "data_profiling_summary.json")
    with open(profiling_json_path, "w") as f:
        json.dump(profiling_reports, f, indent=2)
    print(f"[✓] Profiling JSON: {profiling_json_path}")

    # -------------------------------------------------------------------------
    # STAGE 2: SOURCE → BRONZE
    # -------------------------------------------------------------------------
    print_banner("Stage 2: Source to Bronze Ingestion & 4-Way Reconciliation")
    bronze_tables = {}

    for ds in DATASETS:
        src_path = os.path.join(paths["source"], f"{ds}.csv")
        _, brz_df, recon = pipeline.ingest_source_to_bronze(src_path, table_name=ds, score_calculator=score_calc)
        bronze_tables[ds] = brz_df
        pipeline.save_layer_data(brz_df, os.path.join(paths["bronze"], ds), format_type="csv")
        print(
            f"[RECONCILIATION] {ds.upper():<12} -> "
            f"Schema: {'PASS' if recon['schema_check']['passed'] else 'FAIL'} | "
            f"Count: {'PASS' if recon['record_count_check']['passed'] else 'FAIL'} | "
            f"Checksum: {'PASS' if recon['checksum_check']['passed'] else 'FAIL'}"
        )

    # -------------------------------------------------------------------------
    # STAGE 3: BRONZE → SILVER (clean + quarantine)
    # -------------------------------------------------------------------------
    print_banner("Stage 3: Bronze to Silver Transformation & Cleansing")

    clean_inst, quar_inst = pipeline.transform_bronze_to_silver(
        bronze_tables["institution"], "institution", INSTITUTION_RULES, score_calculator=score_calc
    )
    save_silver(pipeline, clean_inst, quar_inst, "institution", paths)

    clean_prg, quar_prg = pipeline.transform_bronze_to_silver(
        bronze_tables["program"], "program", PROGRAM_RULES,
        context={"institution": clean_inst}, score_calculator=score_calc
    )
    save_silver(pipeline, clean_prg, quar_prg, "program", paths)

    clean_stu, quar_stu = pipeline.transform_bronze_to_silver(
        bronze_tables["student"], "student", STUDENT_RULES,
        context={"institution": clean_inst}, score_calculator=score_calc
    )
    save_silver(pipeline, clean_stu, quar_stu, "student", paths)

    clean_enr, quar_enr = pipeline.transform_bronze_to_silver(
        bronze_tables["enrollment"], "enrollment", ENROLLMENT_RULES,
        context={"student": clean_stu, "institution": clean_inst, "program": clean_prg},
        score_calculator=score_calc,
    )
    save_silver(pipeline, clean_enr, quar_enr, "enrollment", paths)

    # -------------------------------------------------------------------------
    # STAGE 4: GOLD SCD2
    # -------------------------------------------------------------------------
    print_banner("Stage 4: Silver to Gold SCD Type 2 Dimension")
    scd2_stu_gold = pipeline.build_scd_type_2_gold(clean_stu, natural_keys=["Student_ID"])
    pipeline.save_layer_data(scd2_stu_gold, paths["gold_scd2"], format_type="both")
    print(f"[GOLD SCD2] Dim Student Records: {scd2_stu_gold.count()}")

    # -------------------------------------------------------------------------
    # STAGE 5: GOLD WIDE MART
    # -------------------------------------------------------------------------
    print_banner("Stage 5: Gold Wide Consolidated Analytics Mart")
    gold_wide_clean, gold_wide_quar = pipeline.build_gold_wide_table(
        silver_students=clean_stu,
        silver_enrollments=clean_enr,
        silver_institutions=clean_inst,
        silver_programs=clean_prg,
        gold_rules=GOLD_BUSINESS_RULES,
        score_calculator=score_calc,
    )
    pipeline.save_layer_data(gold_wide_clean, paths["gold_mart"], format_type="both")
    pipeline.save_layer_data(gold_wide_quar, paths["gold_mart_quarantine"], format_type="both")
    print(f"[GOLD MART] Clean: {gold_wide_clean.count()} | Quarantine: {gold_wide_quar.count()}")

    # -------------------------------------------------------------------------
    # STAGE 6: DQ SCORING & REPORTS
    # -------------------------------------------------------------------------
    print_banner("Stage 6: Data Quality Scoring & Governance Reports")
    try:
        scores = score_calc.compute_scores()
        html_path = os.path.join(reports_dir, "dq_scorecard_report.html")
        DQHtmlReporter(scores).write(html_path)
        print(f"[✓] HTML scorecard: {html_path}")
    except Exception as exc:
        print(f"[!] Scorecard generation skipped/partial: {exc}")

    try:
        generate_test_summary_csv(os.path.join(docs_dir, "dq_test_cases_summary.csv"))
        print("[✓] Test summary CSV generated")
    except Exception as exc:
        print(f"[!] Test summary CSV: {exc}")

    try:
        generate_pdf_documentation()
        print("[✓] PDF documentation generated")
    except Exception as exc:
        print(f"[!] PDF generation: {exc}")

    print_banner("Pipeline Complete")
    spark.stop()


if __name__ == "__main__":
    run_medallion_pipeline()
