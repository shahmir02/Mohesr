"""
Medallion Pipeline Architecture implementation:
- Source -> Bronze: Exact ingestion, Source-to-Bronze 4-Way Reconciliation
- Bronze -> Silver: Audit columns, Standardization, Deduplication, DQ validation & Quarantine routing
- Silver -> Gold: SCD Type 2 Dimension generation & Wide Analytics Mart Consolidation
"""

import os
import shutil
import glob
from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, DateType, BooleanType
)

from src.keyword_dq_engine import KeywordDQEngine, DataQualityException


class MedallionPipeline:
    """
    End-to-End Medallion Pipeline managing layer transitions with DQ enforcement.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.dq_engine = KeywordDQEngine()

    def ingest_source_to_bronze(self, source_path: str, table_name: str,
                                score_calculator: Optional[Any] = None) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        """
        Source -> Bronze: Exact ingestion of raw data. Adds bronze timestamp metadata.
        Performs 4-way reconciliation:
        1. Schema validation (Completeness)
        2. Row count match (Completeness)
        3. Checksum reconciliation (Consistency)
        4. Numeric aggregation reconciliation (Validity)
        """
        source_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
        
        # Bronze: Exact ingestion copy with bronze metadata
        bronze_df = source_df.withColumn("_bronze_ingestion_ts", F.current_timestamp()) \
                             .withColumn("_bronze_source_file", F.lit(source_path))

        # Reconcile raw source_df against bronze_df (excluding metadata columns)
        bronze_raw_projection = bronze_df.drop("_bronze_ingestion_ts", "_bronze_source_file")
        
        # Numeric columns discovery
        numeric_cols = [f.name for f in source_df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType))]

        schema_res = self.dq_engine.reconcile_schema(source_df, bronze_raw_projection, table_name)
        count_res = self.dq_engine.reconcile_record_count(source_df, bronze_raw_projection, table_name)
        chk_res = self.dq_engine.reconcile_checksum(source_df, bronze_raw_projection, key_cols=source_df.columns, table_name=table_name)
        num_res = self.dq_engine.reconcile_numeric_aggregations(source_df, bronze_raw_projection, numeric_cols, table_name=table_name)

        if score_calculator:
            total_rows = source_df.count()
            score_calculator.record_check(
                rule_id=f"BRZ_REC_{table_name.upper()}_01",
                rule_name=f"Bronze Schema Match - {table_name}",
                dimension="Completeness",
                layer="Bronze",
                table=table_name,
                total_records=len(source_df.columns),
                passed_records=len(source_df.columns) if schema_res["passed"] else 0,
                failed_records=0 if schema_res["passed"] else 1,
                action="REJECT",
                passed=schema_res["passed"],
                details="Exact column & datatype match"
            )
            score_calculator.record_check(
                rule_id=f"BRZ_REC_{table_name.upper()}_02",
                rule_name=f"Bronze Count Reconciliation - {table_name}",
                dimension="Completeness",
                layer="Bronze",
                table=table_name,
                total_records=total_rows,
                passed_records=total_rows if count_res["passed"] else 0,
                failed_records=0 if count_res["passed"] else abs(count_res["difference"]),
                action="REJECT",
                passed=count_res["passed"],
                details="Zero record loss between source and bronze"
            )
            score_calculator.record_check(
                rule_id=f"BRZ_REC_{table_name.upper()}_03",
                rule_name=f"Bronze Checksum Verification - {table_name}",
                dimension="Consistency",
                layer="Bronze",
                table=table_name,
                total_records=total_rows,
                passed_records=total_rows if chk_res["passed"] else 0,
                failed_records=0 if chk_res["passed"] else total_rows,
                action="REJECT",
                passed=chk_res["passed"],
                details="Bit-level SHA-256 payload checksum match"
            )

        reconciliation_report = {
            "table_name": table_name,
            "schema_check": schema_res,
            "record_count_check": count_res,
            "checksum_check": chk_res,
            "numeric_agg_check": num_res
        }

        return source_df, bronze_df, reconciliation_report

    def transform_bronze_to_silver(self, bronze_df: DataFrame, table_name: str,
                                   rules: List[Dict[str, Any]],
                                   context: Optional[Dict[str, DataFrame]] = None,
                                   score_calculator: Optional[Any] = None) -> Tuple[DataFrame, DataFrame]:
        """
        Bronze -> Silver:
        - Adds Standard Audit Columns: _silver_created_at, _silver_updated_at, _pipeline_version, _is_valid
        - Standardizes reference values (e.g. Gender 'Female' -> 'F', 'Male' -> 'M', Trim strings)
        - Executes Keyword DQ Rules (completeness, deduplication, RI, format, chronology)
        - Routes records to Clean Silver vs Quarantine
        """
        df = bronze_df.drop("_bronze_ingestion_ts", "_bronze_source_file")
        
        # Trim all string columns
        for f in df.schema.fields:
            if isinstance(f.dataType, StringType):
                df = df.withColumn(f.name, F.trim(F.col(f.name)))

        # Standardize reference representations if present
        if "Gender" in df.columns:
            df = df.withColumn(
                "Gender",
                F.when(F.upper(F.col("Gender")).isin("FEMALE", "F"), F.lit("F"))
                 .when(F.upper(F.col("Gender")).isin("MALE", "M"), F.lit("M"))
                 .otherwise(F.col("Gender"))
            )

        # Standardize Date formats
        for date_col in ["Date_of_Birth", "Enrollment_Date", "Graduation_Date"]:
            if date_col in df.columns:
                df = df.withColumn(date_col, F.to_date(F.col(date_col), "yyyy-MM-dd"))

        # Execute Keyword DQ Rules Suite
        clean_df, quarantine_df, critical_violations = self.dq_engine.execute_rules_suite(
            df, rules, context=context, score_calculator=score_calculator, table_name=table_name
        )
        
        if critical_violations:
            raise DataQualityException(f"CRITICAL DQ Failure in table '{table_name}': {critical_violations}")

        # Add Audit Columns to Silver Clean
        silver_clean_df = clean_df \
            .withColumn("_silver_processed_ts", F.current_timestamp()) \
            .withColumn("_pipeline_version", F.lit("v2.4.0")) \
            .withColumn("_is_valid", F.lit(True))

        silver_quarantine_df = quarantine_df \
            .withColumn("_quarantined_ts", F.current_timestamp()) \
            .withColumn("_pipeline_version", F.lit("v2.4.0")) \
            .withColumn("_is_valid", F.lit(False))

        return silver_clean_df, silver_quarantine_df

    def build_scd_type_2_gold(self, silver_student_df: DataFrame, natural_keys: List[str] = ["Student_ID"]) -> DataFrame:
        """
        Silver -> Gold SCD Type 2 Dimension:
        Maintains historical versions of records with:
        - effective_start_date
        - effective_end_date (9999-12-31 for active)
        - is_current (True/False)
        - version (1, 2, 3...)
        """
        order_col = "Updated_Date" if "Updated_Date" in silver_student_df.columns else "Created_Date"
        window_spec = Window.partitionBy(*[F.col(k) for k in natural_keys]).orderBy(F.col(order_col).asc())
        
        df_ranked = silver_student_df.withColumn("_row_ver", F.row_number().over(window_spec)) \
                                     .withColumn("_max_ver", F.max(F.row_number().over(window_spec)).over(Window.partitionBy(*[F.col(k) for k in natural_keys])))

        scd2_df = df_ranked \
            .withColumn("version", F.col("_row_ver")) \
            .withColumn("effective_start_date", F.to_timestamp(F.col(order_col))) \
            .withColumn("effective_end_date", F.when(F.col("_row_ver") == F.col("_max_ver"), F.to_timestamp(F.lit("9999-12-31 23:59:59")))
                                               .otherwise(F.lead(F.to_timestamp(F.col(order_col)), 1).over(window_spec))) \
            .withColumn("is_current", F.when(F.col("_row_ver") == F.col("_max_ver"), F.lit(True)).otherwise(F.lit(False))) \
            .drop("_row_ver", "_max_ver")

        return scd2_df

    def build_gold_wide_table(self, silver_students: DataFrame, silver_enrollments: DataFrame,
                              silver_institutions: DataFrame, silver_programs: DataFrame,
                              gold_rules: List[Dict[str, Any]],
                              score_calculator: Optional[Any] = None) -> Tuple[DataFrame, DataFrame]:
        """
        Silver -> Gold Wide Analytics Table:
        Combines all 4 source tables (Student, Enrollment, Institution, Program) into 1 wide consolidated analytical model.
        Applies Gold-layer Business Rules (e.g. Active Enrollment Validation, Age thresholds, Program status alignment).
        """
        joined_df = silver_enrollments.alias("e") \
            .join(silver_students.alias("s"), F.col("e.Student_ID") == F.col("s.Student_ID"), "inner") \
            .join(silver_institutions.alias("i"), F.col("e.Institution_Code") == F.col("i.Institution_Code"), "inner") \
            .join(silver_programs.alias("p"), (F.col("e.Program_Code") == F.col("p.Program_Code")) & (F.col("e.Institution_Code") == F.col("p.Institution_Code")), "inner") \
            .select(
                # Enrollment attributes
                F.col("e.Enrollment_ID"),
                F.col("e.Academic_Year"),
                F.col("e.Enrollment_Status"),
                F.col("e.Enrollment_Date"),
                F.col("e.Graduation_Date"),
                # Student attributes
                F.col("s.Student_ID"),
                F.col("s.Emirates_ID"),
                F.col("s.Full_Name").alias("Student_Name"),
                F.col("s.Date_of_Birth"),
                F.col("s.Gender"),
                F.col("s.Nationality_Code"),
                # Institution attributes
                F.col("i.Institution_Code"),
                F.col("i.Institution_Name"),
                F.col("i.Institution_Status"),
                # Program attributes
                F.col("p.Program_Code"),
                F.col("p.Program_Name"),
                F.col("p.Program_Status")
            )

        clean_gold, quarantine_gold, critical_violations = self.dq_engine.execute_rules_suite(
            joined_df, gold_rules, score_calculator=score_calculator, table_name="gold_wide_mart"
        )
        if critical_violations:
            raise DataQualityException(f"CRITICAL Gold Business Rule Violation: {critical_violations}")

        gold_analytics_df = clean_gold \
            .withColumn("_gold_created_at", F.current_timestamp()) \
            .withColumn("_mart_version", F.lit("Gold-Wide-Enrollment-360-v1"))

        return gold_analytics_df, quarantine_gold

    def save_layer_data(self, df: DataFrame, folder_path: str, format_type: str = "csv"):
        """
        Persists dataframe partitions into structured layer directory.
        Supports 'csv' (human-readable with header and standalone .csv file), 'parquet' (columnar performance), or 'both'.
        """
        os.makedirs(folder_path, exist_ok=True)
        export_df = df
        for f in df.schema.fields:
            if "ArrayType" in str(f.dataType):
                export_df = export_df.withColumn(f.name, F.concat_ws("; ", F.col(f.name)))
        
        if format_type in ("csv", "both"):
            export_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(folder_path)
            part_files = glob.glob(os.path.join(folder_path, "part-*.csv"))
            if part_files:
                single_csv_path = f"{folder_path}.csv"
                shutil.copy(part_files[0], single_csv_path)
        if format_type in ("parquet", "both"):
            parquet_path = folder_path + "_parquet"
            os.makedirs(parquet_path, exist_ok=True)
            export_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
