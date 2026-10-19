"""
Unit + Integration Test Suite for Keyword-Driven Medallion DQ Framework.

Class layout:
  - TestKeywordDQUnit          → in-memory / synthetic data (engine & pipeline logic)
  - TestActualFilesMedallionDQ → physical CSVs under data/ (source → bronze → silver → gold)

Folder contract (optimized — no dual writes):
  data/source/<table>.csv
  data/bronze/<table>/
  data/silver/clean/<table>/
  data/silver/quarantine/<table>/
  data/gold/scd2_dimensions/dim_student_scd2/
  data/gold/analytics_marts/fact_student_enrollment_wide/
  data/gold/analytics_marts/quarantine/fact_student_enrollment_wide/

CSV-driven failure contract:
  Source CSVs intentionally contain defective rows. If those defects are removed
  (or the pipeline fails to quarantine them), the actual-file tests FAIL.
"""

import csv
import os
import sys
import unittest
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.keyword_dq_engine import KeywordDQEngine, DataQualityException
from src.medallion_pipeline import MedallionPipeline
from config.rules_config import (
    STUDENT_RULES,
    ENROLLMENT_RULES,
)

try:
    import allure
except ImportError:
    class _DummyAllure:
        @staticmethod
        def epic(name): return lambda fn: fn
        @staticmethod
        def feature(name): return lambda fn: fn
        @staticmethod
        def story(name): return lambda fn: fn
        @staticmethod
        def title(name): return lambda fn: fn
        @staticmethod
        def severity(level): return lambda fn: fn
        @staticmethod
        def step(title):
            @contextmanager
            def _cm():
                yield
            return _cm()
        class severity_level:
            CRITICAL = "critical"
            NORMAL = "normal"
    allure = _DummyAllure()


DATASETS = ["institution", "program", "student", "enrollment"]


def project_data_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))


def resolve_table_path(*parts: str) -> str:
    """Return first existing path among candidates (Spark dir or flat .csv)."""
    base = os.path.join(*parts)
    if os.path.exists(base):
        return base
    csv_flat = base + ".csv"
    if os.path.exists(csv_flat):
        return csv_flat
    return base


class SparkTestMixin:
    """Shared Spark session + path helpers for both test classes."""

    spark = None
    engine = None
    pipeline = None

    @classmethod
    def _start_spark(cls, app_name: str):
        cls.spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.engine = KeywordDQEngine()
        cls.pipeline = MedallionPipeline(cls.spark)

    @classmethod
    def _stop_spark(cls):
        if cls.spark is not None:
            cls.spark.stop()
            cls.spark = None

    def read_csv(self, path: str):
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)


# =============================================================================
# UNIT TESTS — synthetic / in-memory only
# =============================================================================

@allure.epic("Data Quality Governance & Medallion Architecture")
@allure.feature("Unit — Keyword Engine & Pipeline Logic")
class TestKeywordDQUnit(SparkTestMixin, unittest.TestCase):
    """
    Pure unit tests. All data is created in-memory.
    These do NOT depend on data/ CSVs.
    """

    @classmethod
    def setUpClass(cls):
        cls._start_spark("UnitTest-KeywordDQ")

    @classmethod
    def tearDownClass(cls):
        cls._stop_spark()

    @allure.story("Unit Completeness")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Unit: Missing mandatory keys are quarantined")
    def test_unit_completeness_null_student_id_quarantined(self):
        schema = [
            "Student_ID", "Full_Name", "Emirates_ID", "Date_of_Birth",
            "Gender", "Institution_Code", "Created_Date", "Updated_Date",
        ]
        bad_data = [
            (None, "No ID", "784-2000-1111111-1", "2000-01-01", "M", "INST001", "2023-01-01", "2023-01-01"),
            ("   ", "Blank ID", "784-2000-2222222-2", "2000-01-01", "F", "INST001", "2023-01-01", "2023-01-01"),
            ("STU_OK", "Valid", "784-2000-3333333-3", "2000-01-01", "M", "INST001", "2023-01-01", "2023-01-01"),
        ]
        df = self.spark.createDataFrame(bad_data, schema)
        clean, quar = self.pipeline.transform_bronze_to_silver(df, "student_unit", STUDENT_RULES)

        self.assertEqual(quar.count() + clean.count(), df.count())
        self.assertGreater(quar.count(), 0, "Null/blank Student_ID must be quarantined")
        self.assertEqual(clean.count(), 1)
        errors = [row["dq_errors"] for row in quar.collect()]
        self.assertTrue(all(any("STU_DQ_01" in e for e in err_list) for err_list in errors))

    @allure.story("Unit Uniqueness")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Unit: Duplicate natural keys → latest kept, stale quarantined")
    def test_unit_uniqueness_duplicate_student_id(self):
        schema = [
            "Student_ID", "Full_Name", "Emirates_ID", "Date_of_Birth",
            "Gender", "Institution_Code", "Created_Date", "Updated_Date",
        ]
        dup_data = [
            ("STU001", "Student One", "784-2000-1111111-1", "2000-01-01", "M", "INST001", "2023-01-01", "2023-01-01"),
            ("STU001", "Student One Updated", "784-2000-1111111-1", "2000-01-01", "M", "INST001", "2023-01-01", "2023-02-01"),
        ]
        df = self.spark.createDataFrame(dup_data, schema)
        clean, quar = self.pipeline.transform_bronze_to_silver(df, "student_dedup_unit", STUDENT_RULES)

        self.assertEqual(clean.count() + quar.count(), df.count())
        self.assertEqual(clean.count(), 1)
        self.assertEqual(quar.count(), 1)
        self.assertEqual(clean.collect()[0]["Updated_Date"], "2023-02-01")
        self.assertTrue(any("STU_DQ_03" in e for e in quar.collect()[0]["dq_errors"]))

    @allure.story("Unit Validity")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Unit: Invalid Emirates ID and Gender quarantined")
    def test_unit_validity_format_and_domain(self):
        schema = [
            "Student_ID", "Full_Name", "Emirates_ID", "Date_of_Birth",
            "Gender", "Institution_Code", "Created_Date", "Updated_Date",
        ]
        bad_data = [
            ("STU01", "Bad EID", "INVALID-EMIRATES-ID", "2000-01-01", "M", "INST001", "2023-01-01", "2023-01-01"),
            ("STU02", "Bad Gender", "784-2000-1111111-1", "2000-01-01", "INVALID_GENDER", "INST001", "2023-01-01", "2023-01-01"),
            ("STU03", "Valid", "784-2000-2222222-2", "2000-01-01", "F", "INST001", "2023-01-01", "2023-01-01"),
        ]
        df = self.spark.createDataFrame(bad_data, schema)
        clean, quar = self.pipeline.transform_bronze_to_silver(df, "student_validity_unit", STUDENT_RULES)

        self.assertEqual(clean.count() + quar.count(), df.count())
        self.assertGreaterEqual(quar.count(), 2)
        self.assertEqual(clean.count(), 1)

    @allure.story("Unit Chronology")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Unit: Graduation before Enrollment is quarantined")
    def test_unit_chronology_graduation_before_enrollment(self):
        schema = [
            "Enrollment_ID", "Student_ID", "Program_Code", "Institution_Code",
            "Enrollment_Date", "Graduation_Date", "Academic_Year",
            "Enrollment_Status", "Created_Date", "Updated_Date",
        ]
        data = [
            ("ENR_BAD", "STU001", "PRG001", "INST001", "2023-09-01", "2022-06-01", 2023, "Enrolled", "2023-09-01", "2023-09-01"),
            ("ENR_OK", "STU001", "PRG001", "INST001", "2022-09-01", "2026-06-01", 2022, "Enrolled", "2022-09-01", "2022-09-01"),
        ]
        df = self.spark.createDataFrame(data, schema)
        clean, quar = self.pipeline.transform_bronze_to_silver(df, "enr_chrono_unit", ENROLLMENT_RULES)

        self.assertEqual(clean.count() + quar.count(), df.count())
        self.assertEqual(quar.count(), 1)
        self.assertEqual(clean.count(), 1)
        self.assertTrue(any("ENR_DQ_06" in e for e in quar.collect()[0]["dq_errors"]))

    @allure.story("Unit Timeliness")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Unit: Updated_Date < Created_Date flagged")
    def test_unit_created_updated_chronology(self):
        schema = ["Student_ID", "Full_Name", "Emirates_ID", "Created_Date", "Updated_Date"]
        data = [
            ("STU101", "Valid Equal", "784-2000-1111111-1", "2023-01-15 08:30:00", "2023-01-15 08:30:00"),
            ("STU102", "Valid Later", "784-2000-2222222-2", "2023-01-15 08:30:00", "2023-02-15 10:00:00"),
            ("STU103", "Invalid", "784-2000-3333333-3", "2023-03-01 08:00:00", "2023-01-01 08:00:00"),
        ]
        df = self.spark.createDataFrame(data, schema)
        checked = self.engine.check_date_chronology(
            df,
            start_col="Created_Date",
            end_col="Updated_Date",
            rule_id="STU_DQ_06",
        )
        invalid = checked.filter(F.size(F.col("dq_errors")) > 0).collect()
        valid = checked.filter(F.size(F.col("dq_errors")) == 0).collect()
        self.assertEqual(len(valid), 2)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["Student_ID"], "STU103")

    @allure.story("Unit Domain Case Sensitivity")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Unit: Status domain case_sensitive flag")
    def test_unit_case_sensitive_status_domain(self):
        schema = ["Code", "Institution_Status"]
        data = [
            ("INST001", "Active"),
            ("INST002", "Inactive"),
            ("INST003", "active"),
            ("INST004", "ACTIVE"),
            ("INST005", "Pending"),
            ("INST006", "UNKNOWN"),
        ]
        df = self.spark.createDataFrame(data, schema)

        strict = self.engine.check_status_domain(
            df, status_column="Institution_Status",
            allowed_values=["Active", "Inactive", "Pending"],
            case_sensitive=True, rule_id="INST_DQ_03",
        )
        self.assertEqual(strict.filter(F.size(F.col("dq_errors")) == 0).count(), 3)
        self.assertEqual(strict.filter(F.size(F.col("dq_errors")) > 0).count(), 3)

        lax = self.engine.check_status_domain(
            df, status_column="Institution_Status",
            allowed_values=["Active", "Inactive", "Pending"],
            case_sensitive=False, rule_id="INST_DQ_03_LAX",
        )
        self.assertEqual(lax.filter(F.size(F.col("dq_errors")) == 0).count(), 5)
        self.assertEqual(lax.filter(F.size(F.col("dq_errors")) > 0).count(), 1)

    @allure.story("Unit Reconciliation")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Unit: Count and checksum mismatches fail reconciliation")
    def test_unit_reconciliation_failures(self):
        schema = ["Institution_Code", "Institution_Name", "Institution_Status"]
        data = [
            ("INST001", "Uni A", "Active"),
            ("INST002", "Uni B", "Active"),
        ]
        src = self.spark.createDataFrame(data, schema)
        dropped = src.limit(1)
        altered = src.withColumn("Institution_Name", F.lit("CORRUPTED"))

        count_res = self.engine.reconcile_record_count(src, dropped, "institution")
        self.assertFalse(count_res["passed"])
        self.assertNotEqual(count_res["difference"], 0)

        chk_res = self.engine.reconcile_checksum(src, altered, table_name="institution")
        self.assertFalse(chk_res["passed"])


# =============================================================================
# ACTUAL FILE TESTS — physical CSVs under data/
# =============================================================================

@allure.epic("Data Quality Governance & Medallion Architecture")
@allure.feature("Integration — Actual Layer Files")
class TestActualFilesMedallionDQ(SparkTestMixin, unittest.TestCase):
    """
    Integration tests against physical layer CSVs.

    Source CSVs contain intentional defects. After a pipeline run:
      - silver/clean must have zero of those defects
      - silver/quarantine must contain the defective rows

    If someone removes the intentional bad rows from source CSVs, or the
    pipeline stops quarantining, these tests FAIL.
    """

    @classmethod
    def setUpClass(cls):
        cls._start_spark("IntegrationTest-ActualFiles")
        root = project_data_root()
        cls.base_path = root
        cls.src_path = os.path.join(root, "source")
        cls.brz_path = os.path.join(root, "bronze")
        cls.slv_clean = os.path.join(root, "silver", "clean")
        cls.slv_quar = os.path.join(root, "silver", "quarantine")
        cls.gold_scd2 = os.path.join(root, "gold", "scd2_dimensions", "dim_student_scd2")
        cls.gold_mart = os.path.join(root, "gold", "analytics_marts", "fact_student_enrollment_wide")
        cls.gold_mart_quar = os.path.join(
            root, "gold", "analytics_marts", "quarantine", "fact_student_enrollment_wide"
        )

    @classmethod
    def tearDownClass(cls):
        cls._stop_spark()

    def _read_layer(self, *parts: str):
        path = resolve_table_path(*parts)
        self.assertTrue(os.path.exists(path), f"Missing layer path: {path}")
        return self.read_csv(path)

    @allure.story("Source Profiling")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Actual: Profile all 4 source CSVs")
    def test_01_profile_source_files(self):
        for ds in DATASETS:
            with allure.step(f"Profile {ds}"):
                path = os.path.join(self.src_path, f"{ds}.csv")
                self.assertTrue(os.path.exists(path), f"Source missing: {path}")
                df = self.read_csv(path)
                profile = self.engine.profile_data(df, dataset_name=ds.upper())
                self.assertGreater(profile["total_records"], 0)
                self.assertGreater(profile["total_columns"], 0)
                for col_name, stats in profile["columns"].items():
                    self.assertIn("null_count", stats)
                    self.assertIn("distinct_count", stats)
                    self.assertGreaterEqual(stats["uniqueness_pct"], 0.0)
                    self.assertLessEqual(stats["uniqueness_pct"], 100.0)

    @allure.story("Bronze Reconciliation")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Source↔Bronze 4-way reconciliation")
    def test_02_bronze_reconciliation(self):
        for ds in DATASETS:
            with allure.step(f"Reconcile {ds}"):
                src = self.read_csv(os.path.join(self.src_path, f"{ds}.csv"))
                brz = self._read_layer(self.brz_path, ds)
                self.assertIn("_bronze_ingestion_ts", brz.columns)
                self.assertIn("_bronze_source_file", brz.columns)
                brz_raw = brz.drop("_bronze_ingestion_ts", "_bronze_source_file")

                schema_res = self.engine.reconcile_schema(src, brz_raw, ds)
                self.assertTrue(schema_res["passed"], f"Schema mismatch: {schema_res}")

                count_res = self.engine.reconcile_record_count(src, brz_raw, ds)
                self.assertTrue(count_res["passed"], f"Count mismatch: {count_res}")
                self.assertEqual(count_res["difference"], 0)

                chk_res = self.engine.reconcile_checksum(src, brz_raw, table_name=ds)
                self.assertTrue(chk_res["passed"], f"Checksum mismatch: {chk_res}")

    @allure.story("CSV Defect Contract")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Source student.csv retains intentional defects")
    def test_03_source_student_has_intentional_defects(self):
        """If defects are removed from student.csv, this test fails."""
        src = self.read_csv(os.path.join(self.src_path, "student.csv"))

        null_or_blank_id = src.filter(
            F.col("Student_ID").isNull() | (F.trim(F.col("Student_ID")) == "")
        ).count()
        bad_eid = src.filter(~F.col("Emirates_ID").rlike(r"^784-\d{4}-\d{7}-\d{1}$")).count()
        dup_ids = src.groupBy("Student_ID").count().filter(
            (F.col("count") > 1) & F.col("Student_ID").isNotNull() & (F.trim(F.col("Student_ID")) != "")
        ).count()
        invalid_inst = src.filter(~F.col("Institution_Code").rlike(r"^INST\d{3}$")).count()

        self.assertGreater(null_or_blank_id, 0, "Source must contain blank/null Student_ID for DQ coverage")
        self.assertGreater(bad_eid, 0, "Source must contain invalid Emirates_ID for DQ coverage")
        self.assertGreater(dup_ids, 0, "Source must contain duplicate Student_ID for DQ coverage")
        self.assertGreater(invalid_inst, 0, "Source must contain invalid Institution_Code for DQ coverage")

    @allure.story("CSV Defect Contract")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Source enrollment.csv retains intentional defects")
    def test_04_source_enrollment_has_intentional_defects(self):
        src = self.read_csv(os.path.join(self.src_path, "enrollment.csv"))

        orphan_student = src.filter(
            F.col("Student_ID").isNull()
            | (F.trim(F.col("Student_ID")) == "")
            | F.col("Student_ID").startswith("STU_NON_EXISTENT")
        ).count()
        bad_chrono = src.filter(
            F.col("Graduation_Date").isNotNull()
            & (F.col("Graduation_Date") != "")
            & (F.col("Graduation_Date") < F.col("Enrollment_Date"))
        ).count()
        bad_status = src.filter(
            ~F.col("Enrollment_Status").isin("Enrolled", "Graduated", "Suspended", "Withdrawn")
        ).count()

        self.assertGreater(orphan_student, 0, "Source must contain orphan/missing Student_ID")
        self.assertGreater(bad_chrono, 0, "Source must contain Graduation_Date < Enrollment_Date")
        self.assertGreater(bad_status, 0, "Source must contain invalid Enrollment_Status")

    @allure.story("Silver Clean + Quarantine")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Silver clean student has no null keys / invalid EIDs")
    def test_05_silver_clean_student_quality(self):
        clean = self._read_layer(self.slv_clean, "student")

        null_ids = clean.filter(
            F.col("Student_ID").isNull() | (F.trim(F.col("Student_ID")) == "")
        ).count()
        self.assertEqual(null_ids, 0, "Clean student must not contain null/blank Student_ID")

        dups = clean.groupBy("Student_ID").count().filter(F.col("count") > 1).count()
        self.assertEqual(dups, 0, "Clean student must have unique Student_IDs")

        if "Emirates_ID" in clean.columns:
            bad_eid = clean.filter(~F.col("Emirates_ID").rlike(r"^784-\d{4}-\d{7}-\d{1}$")).count()
            self.assertEqual(bad_eid, 0, "Clean student must only have valid Emirates IDs")

    @allure.story("Silver Clean + Quarantine")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Quarantine student captures source defects")
    def test_06_silver_quarantine_student_captures_defects(self):
        self.assertTrue(os.path.exists(self.slv_quar), "silver/quarantine must exist after pipeline run")
        quar = self._read_layer(self.slv_quar, "student")
        self.assertGreater(quar.count(), 0, "Student quarantine must contain rows from intentional source defects")
        if "dq_errors" in quar.columns:
            rows_with_errors = quar.filter(
                F.col("dq_errors").isNotNull() & (F.col("dq_errors") != "")
            ).count()
            self.assertGreater(rows_with_errors, 0, "Quarantined rows must carry dq_errors tags")

    @allure.story("Silver Clean + Quarantine")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Quarantine enrollment captures source defects")
    def test_07_silver_quarantine_enrollment_captures_defects(self):
        self.assertTrue(os.path.exists(self.slv_quar), "silver/quarantine must exist")
        quar = self._read_layer(self.slv_quar, "enrollment")
        self.assertGreater(quar.count(), 0, "Enrollment quarantine must capture intentional source defects")

    @allure.story("Silver Clean + Quarantine")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Clean enrollment chronology and referential integrity")
    def test_08_silver_clean_enrollment_quality(self):
        clean_enr = self._read_layer(self.slv_clean, "enrollment")
        clean_stu = self._read_layer(self.slv_clean, "student")

        null_enr = clean_enr.filter(
            F.col("Enrollment_ID").isNull() | (F.trim(F.col("Enrollment_ID")) == "")
        ).count()
        self.assertEqual(null_enr, 0)

        if "Graduation_Date" in clean_enr.columns and "Enrollment_Date" in clean_enr.columns:
            bad_dates = clean_enr.filter(
                F.col("Graduation_Date").isNotNull()
                & (F.col("Graduation_Date") < F.col("Enrollment_Date"))
            ).count()
            self.assertEqual(bad_dates, 0, "Clean enrollment must not have reverse chronology")

        orphan = clean_enr.join(clean_stu, "Student_ID", "left_anti").count()
        self.assertEqual(orphan, 0, "Clean enrollment must not reference missing students")

    @allure.story("Gold Layer")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: SCD2 dimension structure")
    def test_09_gold_scd2_structure(self):
        scd2 = self._read_layer(self.gold_scd2)
        for col in ["effective_start_date", "effective_end_date", "is_current", "version"]:
            self.assertIn(col, scd2.columns, f"SCD2 missing column: {col}")
        self.assertGreater(scd2.count(), 0)
        current = scd2.filter(
            (F.col("is_current") == True) | (F.col("is_current") == "true") | (F.col("is_current") == "True")
        )
        self.assertGreater(current.count(), 0, "SCD2 must have at least one current record")

    @allure.story("Gold Layer")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Actual: Wide mart exists and is non-empty")
    def test_10_gold_wide_mart(self):
        mart = self._read_layer(self.gold_mart)
        self.assertGreater(mart.count(), 0, "Gold wide mart must contain verified clean records")

    @allure.story("Layer Layout")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Actual: Optimized folder layout + summary CSV")
    def test_11_optimized_folder_layout_and_summary_csv(self):
        with allure.step("Summary CSV"):
            summary_csv_path = os.path.join(os.path.dirname(self.base_path), "docs", "dq_test_cases_summary.csv")
            if not os.path.exists(summary_csv_path):
                summary_csv_path = os.path.join(self.base_path, "dq_test_cases_summary.csv")
            self.assertTrue(os.path.exists(summary_csv_path), "dq_test_cases_summary.csv must exist")
            with open(summary_csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                for required in ("id", "category", "dq_dimension", "test_summary_description"):
                    self.assertIn(required, headers)
                rows = list(reader)
                self.assertGreaterEqual(len(rows), 10)

        with allure.step("Source + Bronze present"):
            for ds in DATASETS:
                self.assertTrue(os.path.exists(os.path.join(self.src_path, f"{ds}.csv")))
                self.assertTrue(
                    os.path.exists(os.path.join(self.brz_path, ds))
                    or os.path.exists(os.path.join(self.brz_path, f"{ds}.csv"))
                )

        with allure.step("Silver clean + quarantine present"):
            self.assertTrue(os.path.isdir(self.slv_clean), "silver/clean must exist")
            self.assertTrue(os.path.isdir(self.slv_quar), "silver/quarantine must exist")
            for ds in DATASETS:
                clean_p = resolve_table_path(self.slv_clean, ds)
                self.assertTrue(os.path.exists(clean_p), f"Missing silver/clean/{ds}")
            quar_stu = resolve_table_path(self.slv_quar, "student")
            self.assertTrue(os.path.exists(quar_stu), "Missing silver/quarantine/student")

        with allure.step("Gold paths present"):
            self.assertTrue(
                os.path.exists(self.gold_scd2) or os.path.exists(self.gold_scd2 + ".csv"),
                "gold/scd2_dimensions/dim_student_scd2 must exist",
            )
            self.assertTrue(
                os.path.exists(self.gold_mart) or os.path.exists(self.gold_mart + ".csv"),
                "gold/analytics_marts/fact_student_enrollment_wide must exist",
            )


if __name__ == "__main__":
    unittest.main()
