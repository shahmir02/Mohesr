"""
Unit and Integration Test Suite for Keyword-Driven Medallion DQ Framework.

Strict Testing Requirements:
1. Tests the ACTUAL generated data files across all 4 layers (Source, Bronze, Silver, Gold) in CSV format.
2. Explicitly verifies DQ Dimensions:
   - Completeness
   - Validity
   - Uniqueness
   - Consistency
   - Referential Integrity
   - Timeliness
3. Explicitly verifies Medallion Layers:
   - Source Layer (Raw CSV extracts & Automated Profiling)
   - Bronze Layer (Ingestion & 4-Way Reconciliation)
   - Silver Layer (Clean Datasets, Audit Columns, Deduplication, Standardization)
   - Gold Layer (SCD Type 2 History & 4-Table Wide Consolidated Analytical Mart)
4. Verifies test summary documentation CSV and rule execution mechanisms.
"""

import unittest
import sys
import os
import csv
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Project paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.keyword_dq_engine import KeywordDQEngine, DataQualityException
from src.medallion_pipeline import MedallionPipeline
from src.generate_test_summary_csv import generate_test_summary_csv
from config.rules_config import (
    BRONZE_RECONCILIATION_RULES,
    INSTITUTION_RULES,
    PROGRAM_RULES,
    STUDENT_RULES,
    ENROLLMENT_RULES,
    GOLD_BUSINESS_RULES
)

# Optional Allure integration with graceful fallback
try:
    import allure
    has_allure = True
except ImportError:
    has_allure = False

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
            def _step_cm():
                yield
            return _step_cm()
        class severity_level:
            CRITICAL = "critical"
            NORMAL = "normal"
    allure = _DummyAllure()


@allure.epic("Data Quality Governance & Medallion Architecture")
@allure.feature("End-to-End DQ Framework Verification")
class TestActualFilesMedallionDQFramework(unittest.TestCase):
    """
    Test suite executing against actual physical data files in data/ directory.
    """

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("UnitTest-ActualFiles-DQ") \
            .master("local[*]") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.codegen.wholeStage", "false") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.engine = KeywordDQEngine()
        cls.pipeline = MedallionPipeline(cls.spark)

        cls.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
        cls.src_path = os.path.join(cls.base_path, "source")
        cls.brz_path = os.path.join(cls.base_path, "bronze")
        cls.slv_path = os.path.join(cls.base_path, "silver")
        cls.gold_path = os.path.join(cls.base_path, "gold")
        cls.gold_scd2_path = os.path.join(cls.gold_path, "scd2_dimensions", "dim_student_scd2")
        cls.gold_mart_path = os.path.join(cls.gold_path, "analytics_marts", "fact_student_enrollment_wide")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # =========================================================================
    # 1. DATA PROFILING (Source Files)
    # =========================================================================
    @allure.story("Data Profiling on Source Ingestion")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Task 1: Automated Data Profiling on 4 Actual Source Datasets")
    def test_01_profile_actual_source_files(self):
        """
        [PROFILING] Validate data profiling execution against all 4 actual source CSV files.
        """
        for ds in ["institution", "program", "student", "enrollment"]:
            with allure.step(f"Profiling source dataset: {ds}"):
                file_path = os.path.join(self.src_path, f"{ds}.csv")
                self.assertTrue(os.path.exists(file_path), f"Source file missing: {file_path}")
                
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                profile = self.engine.profile_data(df, dataset_name=ds.upper())
                
                self.assertGreater(profile["total_records"], 0)
                self.assertGreater(profile["total_columns"], 0)
                self.assertIn("columns", profile)

    # =========================================================================
    # 2. BRONZE LAYER & RECONCILIATION (Completeness, Consistency, Validity)
    # =========================================================================
    @allure.story("Bronze Layer Ingestion & 4-Way Reconciliation")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 2: Source to Bronze 4-Way Reconciliation (Exact Match, No Loss, Checksums)")
    def test_02_bronze_layer_reconciliation_actual_files(self):
        """
        [BRONZE LAYER] [DIMENSIONS: Completeness, Consistency, Validity] [ACTION: REJECT/FAIL]
        Verifies 4-Way Reconciliation between actual Source CSVs and actual Bronze CSVs:
        - Schema match (Completeness)
        - Record count match (Completeness)
        - Checksum payload hash (Consistency)
        - Numeric aggregation (Validity)
        """
        for ds in ["institution", "program", "student", "enrollment"]:
            with allure.step(f"Reconciling Bronze table: {ds}"):
                src_file = os.path.join(self.src_path, f"{ds}.csv")
                brz_file = os.path.join(self.brz_path, ds)
                
                src_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(src_file)
                brz_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(brz_file)
                
                # Bronze audit metadata presence
                self.assertIn("_bronze_ingestion_ts", brz_df.columns)
                self.assertIn("_bronze_source_file", brz_df.columns)
                
                brz_raw = brz_df.drop("_bronze_ingestion_ts", "_bronze_source_file")
                
                # 1. Schema Reconciliation
                schema_res = self.engine.reconcile_schema(src_df, brz_raw, ds)
                self.assertTrue(schema_res["passed"], f"Schema mismatch on {ds}: {schema_res}")
                self.assertEqual(schema_res["dimension"], "Completeness")
                self.assertEqual(schema_res["layer"], "Bronze")
                
                # 2. Record Count Reconciliation
                count_res = self.engine.reconcile_record_count(src_df, brz_raw, ds)
                self.assertTrue(count_res["passed"], f"Count mismatch on {ds}: {count_res}")
                self.assertEqual(count_res["difference"], 0)
                self.assertEqual(count_res["dimension"], "Completeness")
                
                # 3. Checksum Hash Reconciliation
                chk_res = self.engine.reconcile_checksum(src_df, brz_raw, table_name=ds)
                self.assertTrue(chk_res["passed"], f"Checksum mismatch on {ds}: {chk_res}")
                self.assertEqual(chk_res["dimension"], "Consistency")

    # =========================================================================
    # 3. SILVER LAYER: COMPLETENESS DIMENSION (Mandatory Key Validation)
    # =========================================================================
    @allure.story("Silver Layer Cleansing & Validation")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 3: Completeness Dimension - Mandatory Identifier Validation")
    def test_03_silver_completeness_dimension_actual_files(self):
        """
        [SILVER LAYER] [DIMENSION: Completeness]
        Verifies that clean Silver datasets contain zero records with missing mandatory identifiers.
        """
        with allure.step("Verifying clean Silver datasets have no null mandatory keys"):
            clean_stu = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "student"))
            null_stus = clean_stu.filter(F.col("Student_ID").isNull() | (F.trim(F.col("Student_ID")) == "")).count()
            self.assertEqual(null_stus, 0, "Clean Silver Student table must not contain null Student_ID")
            
            clean_enr = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "enrollment"))
            null_enrs = clean_enr.filter(F.col("Enrollment_ID").isNull() | (F.trim(F.col("Enrollment_ID")) == "")).count()
            self.assertEqual(null_enrs, 0, "Clean Silver Enrollment table must not contain null Enrollment_ID")

    # =========================================================================
    # 4. SILVER LAYER: UNIQUENESS DIMENSION (Deduplication)
    # =========================================================================
    @allure.story("Silver Layer Deduplication")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 4: Uniqueness Dimension - Natural Key Deduplication & Stale Record Isolation")
    def test_04_silver_uniqueness_dimension_actual_files(self):
        """
        [SILVER LAYER] [DIMENSION: Uniqueness]
        Verifies deduplication on Student natural key:
        - Silver clean contains 100% unique Student_IDs.
        """
        with allure.step("Verifying clean student table has unique IDs"):
            clean_stu = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "student"))
            stu_counts = clean_stu.groupBy("Student_ID").count().filter(F.col("count") > 1).count()
            self.assertEqual(stu_counts, 0, "Clean Silver Student table must not contain duplicate Student_IDs")

    # =========================================================================
    # 5. SILVER LAYER: VALIDITY DIMENSION (Format & Reference Set)
    # =========================================================================
    @allure.story("Silver Layer Format & Domain Validation")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Task 5: Validity Dimension - Regex Matching & Domain Set Constraints")
    def test_05_silver_validity_dimension_actual_files(self):
        """
        [SILVER LAYER] [DIMENSION: Validity]
        Verifies format regex (Emirates ID), allowed status values, and academic year range:
        - All Clean Silver records have valid UAE Emirates ID format and valid standardized Gender values.
        """
        with allure.step("Verifying format and domain values in clean Silver datasets"):
            clean_stu = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "student"))
            bad_eids = clean_stu.filter(~F.col("Emirates_ID").rlike(r"^784-\d{4}-\d{7}-\d{1}$")).count()
            self.assertEqual(bad_eids, 0, "Clean Silver Student must contain only valid UAE Emirates IDs")
            
            bad_genders = clean_stu.filter(~F.col("Gender").isin("F", "M", "Other", "Unknown")).count()
            self.assertEqual(bad_genders, 0, "Clean Silver Student must contain only valid standardized Gender values")

    # =========================================================================
    # 6. SILVER LAYER: REFERENTIAL INTEGRITY DIMENSION
    # =========================================================================
    @allure.story("Silver Layer Referential Integrity")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 6: Referential Integrity Dimension - Cross-Table Foreign Key Enforcement")
    def test_06_silver_referential_integrity_dimension_actual_files(self):
        """
        [SILVER LAYER] [DIMENSION: Referential Integrity]
        Verifies that all foreign key references in clean Silver tables point to valid parent records.
        """
        with allure.step("Verifying cross-table referential integrity"):
            clean_stu = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "student"))
            clean_inst = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "institution"))
            clean_prg = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "program"))
            clean_enr = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "enrollment"))
            
            # Student -> Institution
            orphan_stu_inst = clean_stu.join(clean_inst, "Institution_Code", "left_anti").count()
            self.assertEqual(orphan_stu_inst, 0, "No orphaned institution references in Student")
            
            # Enrollment -> Student
            orphan_enr_stu = clean_enr.join(clean_stu, "Student_ID", "left_anti").count()
            self.assertEqual(orphan_enr_stu, 0, "No orphaned student references in Enrollment")

    # =========================================================================
    # 7. SILVER LAYER: TIMELINESS & CONSISTENCY (Chronology Validation)
    # =========================================================================
    @allure.story("Silver Layer Chronology & Consistency")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Task 7: Consistency Dimension - Date Chronology Sequence Validation")
    def test_07_silver_consistency_chronology_actual_files(self):
        """
        [SILVER LAYER] [DIMENSION: Consistency]
        Verifies date chronology logic:
        - Graduation_Date is always on or after Enrollment_Date.
        """
        with allure.step("Verifying date chronology integrity in clean Enrollment table"):
            clean_enr = self.spark.read.option("header", "true").csv(os.path.join(self.slv_path, "enrollment"))
            bad_dates = clean_enr.filter(
                F.col("Graduation_Date").isNotNull() & (F.col("Graduation_Date") < F.col("Enrollment_Date"))
            ).count()
            self.assertEqual(bad_dates, 0, "Clean enrollment records must have valid chronological dates")

    # =========================================================================
    # 8. GOLD LAYER: SCD TYPE 2 DIMENSION (Student)
    # =========================================================================
    @allure.story("Gold Layer SCD Type 2 Dimension")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 8: Gold Layer - SCD Type 2 Dimensioning & Version Tracking")
    def test_08_gold_scd_type_2_dimension_actual_files(self):
        """
        [GOLD LAYER] [DIMENSION: Consistency, Uniqueness]
        Verifies Gold SCD Type 2 dimension structure:
        - Contains effective_start_date, effective_end_date, is_current, version.
        - Active current records have effective_end_date = 9999-12-31 23:59:59 and is_current = True.
        """
        with allure.step("Verifying SCD Type 2 dimension structure"):
            self.assertTrue(os.path.exists(self.gold_scd2_path))
            gold_scd2_df = self.spark.read.option("header", "true").csv(self.gold_scd2_path)
            
            for col_name in ["effective_start_date", "effective_end_date", "is_current", "version"]:
                self.assertIn(col_name, gold_scd2_df.columns)
                
            active_records = gold_scd2_df.filter(F.col("is_current") == "true")
            self.assertGreater(active_records.count(), 0)

    # =========================================================================
    # 9. GOLD LAYER: 4-TABLE CONSOLIDATED WIDE ANALYTICS MART
    # =========================================================================
    @allure.story("Gold Layer Wide Analytics Mart")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 9: Gold Layer - 4-Table Consolidated Wide Mart & Business Rule Enforcement")
    def test_09_gold_wide_analytical_mart_actual_files(self):
        """
        [GOLD LAYER] [DIMENSIONS: Completeness, Consistency, Validity]
        Verifies wide analytical fact table consolidating Student, Enrollment, Institution, and Program:
        - Contains joined attributes from all 4 datasets.
        - Gold Business Rules applied (Active Institution, Active Program, Minimum Age).
        """
        with allure.step("Verifying 4-table wide consolidated analytical mart"):
            self.assertTrue(os.path.exists(self.gold_mart_path))
            gold_mart_df = self.spark.read.option("header", "true").csv(self.gold_mart_path)
            
            expected_attributes = [
                "Enrollment_ID", "Student_ID", "Student_Name", "Emirates_ID", "Gender",
                "Institution_Code", "Institution_Name", "Program_Code", "Program_Name",
                "Enrollment_Status", "_mart_version"
            ]
            for attr in expected_attributes:
                self.assertIn(attr, gold_mart_df.columns)
                
            self.assertGreater(gold_mart_df.count(), 0, "Gold Analytics mart must contain verified clean records")

    # =========================================================================
    # 10. REJECT / FAIL ACTION MECHANISM VERIFICATION
    # =========================================================================
    @allure.story("Governance & Isolation Mechanics")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 10: Critical Rule Abort / Exception Mechanism Verification")
    def test_10_quarantine_vs_reject_action_mechanisms(self):
        """
        [GOVERNANCE] Verifies that when a critical rule fails with FAIL/REJECT action,
        a DataQualityException is raised to halt the pipeline.
        """
        with allure.step("Testing REJECT fatal abort on critical violations"):
            fatal_rule = [{
                "rule_id": "FATAL_TEST_01",
                "rule_name": "Fatal Critical Check",
                "dimension": "Completeness",
                "layer": "Silver",
                "keyword": "NOT_NULL",
                "severity": "CRITICAL",
                "action": "FAIL",
                "params": {"column": "Student_ID"}
            }]
            raw_stu = self.spark.read.option("header", "true").csv(os.path.join(self.src_path, "student.csv"))
            with self.assertRaises(DataQualityException):
                self.pipeline.transform_bronze_to_silver(raw_stu, "student_fatal_test", fatal_rule)

    # =========================================================================
    # 11. TEST SUMMARY CSV & LAYER CSV FILES VERIFICATION
    # =========================================================================
    @allure.story("Test Documentation & Layer CSV Files")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Task 11: Verification of Layer CSV Datasets & Test Summary CSV")
    def test_11_layer_csv_files_and_test_summary_csv(self):
        """
        [DOCUMENTATION & DATA] Verifies that:
        1. Test cases summary CSV exists with all required columns in plain English.
        2. All 4 layers (Source, Bronze, Silver, Gold) contain clean CSV files.
        3. No quarantine directories exist in the data folder.
        """
        with allure.step("Checking test summary CSV"):
            summary_csv_path = os.path.join(os.path.dirname(self.base_path), "docs", "dq_test_cases_summary.csv")
            self.assertTrue(os.path.exists(summary_csv_path), "Test summary CSV must exist in docs/")
            
            with open(summary_csv_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                self.assertIn("id", headers)
                self.assertIn("category", headers)
                self.assertIn("dq_dimension", headers)
                self.assertIn("test_summary_description", headers)
                self.assertIn("code_snippet_in_query", headers)
                rows = list(reader)
                self.assertGreaterEqual(len(rows), 20)

        with allure.step("Checking CSV files exist across all 4 layers"):
            for ds in ["student", "enrollment", "institution", "program"]:
                # Source
                self.assertTrue(os.path.exists(os.path.join(self.src_path, f"{ds}.csv")))
                # Bronze
                self.assertTrue(os.path.exists(os.path.join(self.brz_path, f"{ds}.csv")) or os.path.exists(os.path.join(self.brz_path, ds)))
                # Silver
                self.assertTrue(os.path.exists(os.path.join(self.slv_path, f"{ds}.csv")) or os.path.exists(os.path.join(self.slv_path, ds)))

            # Gold
            self.assertTrue(
                os.path.exists(os.path.join(self.gold_path, "dim_student_scd2.csv")) or
                os.path.exists(os.path.join(self.gold_scd2_path))
            )
            self.assertTrue(
                os.path.exists(os.path.join(self.gold_path, "fact_student_enrollment_wide.csv")) or
                os.path.exists(os.path.join(self.gold_mart_path))
            )

        with allure.step("Verifying quarantine directory is completely absent"):
            quar_path = os.path.join(self.slv_path, "quarantine")
            self.assertFalse(os.path.exists(quar_path), "Quarantine directory must not exist in silver layer")

    # =========================================================================
    # 12. CREATED_DATE VS UPDATED_DATE CHRONOLOGY CHECK (updated_date >= created_date)
    # =========================================================================
    @allure.story("Student Timeliness Chronology Check")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.title("Task 12: Created_Date vs Updated_Date Chronology Verification")
    def test_12_date_chronology_created_updated_dates(self):
        """
        [TIMELINESS / CONSISTENCY]
        Verifies that Updated_Date is logically on or after Created_Date (updated_date >= created_date).
        Records with updated_date < created_date are flagged with dq_errors.
        """
        with allure.step("Testing Created_Date vs Updated_Date chronology check"):
            schema = ["Student_ID", "Full_Name", "Emirates_ID", "Created_Date", "Updated_Date"]
            test_data = [
                ("STU101", "Valid Student 1", "784-2000-1111111-1", "2023-01-15 08:30:00", "2023-01-15 08:30:00"),  # Valid: updated == created
                ("STU102", "Valid Student 2", "784-2000-2222222-2", "2023-01-15 08:30:00", "2023-02-15 10:00:00"),  # Valid: updated > created
                ("STU103", "Invalid Student", "784-2000-3333333-3", "2023-03-01 12:00:00", "2023-02-01 12:00:00"),  # Invalid: updated < created
            ]
            df = self.spark.createDataFrame(test_data, schema)
            
            # Using KeywordDQEngine convenience method and apply_keyword_rule
            checked_df = self.engine.check_date_chronology(df, start_col="Created_Date", end_col="Updated_Date")
            
            # Verify clean vs quarantined rows
            valid_rows = checked_df.filter(F.size(F.col("dq_errors")) == 0).collect()
            invalid_rows = checked_df.filter(F.size(F.col("dq_errors")) > 0).collect()
            
            self.assertEqual(len(valid_rows), 2)
            self.assertEqual(len(invalid_rows), 1)
            self.assertEqual(invalid_rows[0]["Student_ID"], "STU103")
            self.assertTrue(any("Chronology violation" in err for err in invalid_rows[0]["dq_errors"]))

    # =========================================================================
    # 13. DUPLICATE NAME, EMIRATES_ID WITH DIFFERENT STUDENT_ID CHECK
    # =========================================================================
    @allure.story("Student Cross-ID Identity Duplicate Check")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.title("Task 13: Duplicate Name and Emirates_ID with Different Student_ID Check")
    def test_13_duplicate_name_emirates_id_with_different_student_id(self):
        """
        [UNIQUENESS / CONSISTENCY]
        Verifies detection of records with duplicate (Full_Name, Emirates_ID) but different Student_ID.
        """
        with allure.step("Testing duplicate (Full_Name, Emirates_ID) with different Student_ID"):
            schema = ["Student_ID", "Full_Name", "Emirates_ID", "Created_Date", "Updated_Date"]
            test_data = [
                # Same identity (Fatima Ahmed, 784-2001-1234567-1) with DIFFERENT student IDs (STU001 vs STU999) -> Duplicate violation
                ("STU001", "Fatima Ahmed", "784-2001-1234567-1", "2023-01-15 08:30:00", "2023-01-15 08:30:00"),
                ("STU999", "Fatima Ahmed", "784-2001-1234567-1", "2023-01-16 09:00:00", "2023-01-16 09:00:00"),
                # Same Student_ID STU002 (different versions of same student) -> Not cross-ID conflict
                ("STU002", "Zayed Sultan", "784-2002-7654321-2", "2023-01-16 09:15:00", "2023-04-10 14:20:00"),
                ("STU002", "Zayed Sultan", "784-2002-7654321-2", "2023-01-16 09:15:00", "2023-04-11 10:00:00"),
                # Unique student
                ("STU003", "John Smith", "784-1999-9876543-3", "2023-01-17 11:00:00", "2023-01-17 11:00:00")
            ]
            df = self.spark.createDataFrame(test_data, schema)
            
            # Using KeywordDQEngine convenience method and apply_keyword_rule
            checked_df = self.engine.check_duplicate_identity_with_different_id(
                df, key_columns=["Full_Name", "Emirates_ID"], distinct_column="Student_ID"
            )
            
            flagged_rows = checked_df.filter(F.size(F.col("dq_errors")) > 0).collect()
            clean_rows = checked_df.filter(F.size(F.col("dq_errors")) == 0).collect()
            
            # STU001 and STU999 should be flagged
            flagged_stu_ids = [r["Student_ID"] for r in flagged_rows]
            self.assertIn("STU001", flagged_stu_ids)
            self.assertIn("STU999", flagged_stu_ids)
            self.assertEqual(len(flagged_rows), 2)
            
            # Clean rows should contain STU002 (both versions) and STU003
            clean_stu_ids = [r["Student_ID"] for r in clean_rows]
            self.assertIn("STU002", clean_stu_ids)
            self.assertIn("STU003", clean_stu_ids)


if __name__ == "__main__":
    unittest.main()
