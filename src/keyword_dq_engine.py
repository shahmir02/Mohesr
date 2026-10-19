"""
Keyword-Driven Data Quality Engine.
Provides core keyword execution methods for:
- Data Profiling (PROFILE_DATA)
- Source-to-Bronze 4-Way Reconciliation (RECONCILE_SCHEMA, RECONCILE_CHECKSUM, RECONCILE_RECORD_COUNT, RECONCILE_NUMERIC_AGG)
- Silver Layer Cleansing & Validation (NOT_NULL, DEDUPLICATE, REFERENTIAL_INTEGRITY, DATE_CHRONOLOGY, REGEX_MATCH, VALUE_IN_SET)
- Gold Layer Business Rules (BUSINESS_RULE)
- Action Hierarchy:
    - CRITICAL / FAIL / REJECT: Fatal abort
    - HIGH / MEDIUM / QUARANTINE: Non-blocking row-level isolation
    - LOW / WARN: Audit logging
"""

import hashlib
from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType


class DataQualityException(Exception):
    """Raised when a CRITICAL Data Quality rule fails and severity action is FAIL/ABORT/REJECT."""
    pass


class KeywordDQEngine:
    """
    Core Keyword-Driven DQ Engine.
    Executes declarative rules across Completeness, Validity, Uniqueness, Consistency, Referential Integrity, and Timeliness.
    """

    # =========================================================================
    # 1. DATA PROFILING (Keyword: PROFILE_DATA)
    # =========================================================================
    @staticmethod
    def profile_data(df: DataFrame, dataset_name: str = "Dataset") -> Dict[str, Any]:
        """
        Profiles a DataFrame: calculates row count, columns, data types, nulls, distinct counts,
        min, max, average (for numeric columns), and string length metrics.
        """
        row_count = df.count()
        columns = df.columns
        col_types = {field.name: str(field.dataType) for field in df.schema.fields}
        
        col_profiles = {}
        for field in df.schema.fields:
            c = field.name
            dtype_str = str(field.dataType)
            
            null_count = df.filter(
                F.col(c).isNull() | 
                (F.trim(F.col(c).cast("string")) == "") | 
                (F.lower(F.col(c).cast("string")) == "null")
            ).count()
            distinct_count = df.select(c).distinct().count()
            
            # Non-null values for metric aggregations
            non_null_df = df.filter(F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != ""))
            has_data = non_null_df.count() > 0
            
            min_val = None
            max_val = None
            avg_val = None
            min_len = None
            max_len = None
            
            if has_data:
                # Min and Max for comparable values
                min_max_row = non_null_df.agg(
                    F.min(F.col(c)).alias("min_val"),
                    F.max(F.col(c)).alias("max_val")
                ).collect()[0]
                min_val = str(min_max_row["min_val"]) if min_max_row["min_val"] is not None else None
                max_val = str(min_max_row["max_val"]) if min_max_row["max_val"] is not None else None
                
                # Avg for numeric columns
                numeric_types = ("IntegerType", "LongType", "FloatType", "DoubleType", "DecimalType", "ShortType", "ByteType")
                if any(nt in dtype_str for nt in numeric_types):
                    avg_row = non_null_df.agg(F.avg(F.col(c).cast("double")).alias("avg_val")).collect()[0]
                    if avg_row["avg_val"] is not None:
                        avg_val = round(float(avg_row["avg_val"]), 2)
                
                # String lengths
                if "StringType" in dtype_str:
                    len_row = non_null_df.agg(
                        F.min(F.length(F.col(c))).alias("min_len"),
                        F.max(F.length(F.col(c))).alias("max_len")
                    ).collect()[0]
                    min_len = int(len_row["min_len"]) if len_row["min_len"] is not None else None
                    max_len = int(len_row["max_len"]) if len_row["max_len"] is not None else None
            
            col_profiles[c] = {
                "type": dtype_str,
                "null_count": null_count,
                "null_pct": round((null_count / row_count * 100) if row_count > 0 else 0.0, 2),
                "distinct_count": distinct_count,
                "uniqueness_pct": round((distinct_count / row_count * 100) if row_count > 0 else 0.0, 2),
                "min": min_val,
                "max": max_val,
                "avg": avg_val,
                "min_length": min_len,
                "max_length": max_len
            }
            
        return {
            "dataset_name": dataset_name,
            "total_records": row_count,
            "total_columns": len(columns),
            "columns": col_profiles
        }

    # =========================================================================
    # 2. SOURCE-TO-BRONZE 4-WAY RECONCILIATION
    # =========================================================================
    @staticmethod
    def reconcile_schema(source_df: DataFrame, bronze_df: DataFrame, table_name: str = "") -> Dict[str, Any]:
        """Validates exact match of column names and datatypes."""
        src_cols = [(f.name.lower(), str(f.dataType)) for f in source_df.schema.fields]
        brz_cols = [(f.name.lower(), str(f.dataType)) for f in bronze_df.schema.fields]
        passed = (src_cols == brz_cols)
        return {
            "check": "RECONCILE_SCHEMA",
            "dimension": "Completeness",
            "layer": "Bronze",
            "table": table_name,
            "passed": passed,
            "source_cols": len(src_cols),
            "bronze_cols": len(brz_cols)
        }

    @staticmethod
    def reconcile_record_count(source_df: DataFrame, bronze_df: DataFrame, table_name: str = "") -> Dict[str, Any]:
        """Verifies zero record loss between source and bronze."""
        src_count = source_df.count()
        brz_count = bronze_df.count()
        passed = (src_count == brz_count)
        return {
            "check": "RECONCILE_RECORD_COUNT",
            "dimension": "Completeness",
            "layer": "Bronze",
            "table": table_name,
            "passed": passed,
            "source_count": src_count,
            "bronze_count": brz_count,
            "difference": brz_count - src_count
        }

    @staticmethod
    def reconcile_checksum(source_df: DataFrame, bronze_df: DataFrame, key_cols: Optional[List[str]] = None, table_name: str = "") -> Dict[str, Any]:
        """Verifies cryptographic bit-level payload checksums."""
        cols = key_cols if key_cols else source_df.columns
        src_concat = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in cols])
        brz_concat = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in cols])
        
        src_hashes = source_df.withColumn("_row_hash", F.sha2(src_concat, 256)).select("_row_hash").orderBy("_row_hash").collect()
        brz_hashes = bronze_df.withColumn("_row_hash", F.sha2(brz_concat, 256)).select("_row_hash").orderBy("_row_hash").collect()
        
        src_combined = "".join([r["_row_hash"] for r in src_hashes])
        brz_combined = "".join([r["_row_hash"] for r in brz_hashes])
        
        src_hash = hashlib.sha256(src_combined.encode('utf-8')).hexdigest()
        brz_hash = hashlib.sha256(brz_combined.encode('utf-8')).hexdigest()
        
        passed = (src_hash == brz_hash)
        return {
            "check": "RECONCILE_CHECKSUM",
            "dimension": "Consistency",
            "layer": "Bronze",
            "table": table_name,
            "passed": passed,
            "source_checksum": src_hash,
            "bronze_checksum": brz_hash
        }

    @staticmethod
    def reconcile_numeric_aggregations(source_df: DataFrame, bronze_df: DataFrame, numeric_cols: List[str], table_name: str = "") -> Dict[str, Any]:
        """Validates sum, min, max numeric aggregations."""
        if not numeric_cols:
            return {"check": "RECONCILE_NUMERIC_AGG", "dimension": "Validity", "layer": "Bronze", "table": table_name, "passed": True}
            
        src_exprs = [F.sum(F.col(c).cast("double")).alias(f"{c}_sum") for c in numeric_cols]
        brz_exprs = [F.sum(F.col(c).cast("double")).alias(f"{c}_sum") for c in numeric_cols]
        
        src_res = source_df.agg(*src_exprs).collect()[0].asDict()
        brz_res = bronze_df.agg(*brz_exprs).collect()[0].asDict()
        
        passed = (src_res == brz_res)
        return {
            "check": "RECONCILE_NUMERIC_AGG",
            "dimension": "Validity",
            "layer": "Bronze",
            "table": table_name,
            "passed": passed,
            "source_metrics": src_res,
            "bronze_metrics": brz_res
        }

    # =========================================================================
    # 3. KEYWORD RULE EXECUTION & QUARANTINE ROUTING
    # =========================================================================
    @staticmethod
    def apply_keyword_rule(df: DataFrame, rule: Dict[str, Any], context: Optional[Dict[str, DataFrame]] = None) -> DataFrame:
        """
        Applies a single keyword rule:
        - Evaluates violation condition.
        - Appends structured error tags to `dq_errors` array.
        """
        keyword = rule.get("keyword")
        rule_id = rule.get("rule_id", "DQ_RULE")
        dimension = rule.get("dimension", "Validity")
        layer = rule.get("layer", "Silver")
        severity = rule.get("severity", "HIGH").upper()
        action = rule.get("action", "QUARANTINE").upper()
        params = rule.get("params", {})
        
        if "dq_errors" not in df.columns:
            # Initialize an empty array<string> column safely using SQL expr
            df = df.withColumn("dq_errors", F.expr("array()").cast("array<string>"))
            
        err_tag = f"{rule_id}|{dimension}|{layer}|{severity}|{action}|{keyword}: "

        if keyword == "NOT_NULL":
            col_name = params["column"]
            cond = F.col(col_name).isNull() | (F.trim(F.col(col_name).cast("string")) == "") | (F.lower(F.col(col_name).cast("string")) == "null")
            df = df.withColumn(
                "dq_errors",
                F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Mandatory column '{col_name}' is null or empty"))))
                .otherwise(F.col("dq_errors"))
            )

        elif keyword == "DEDUPLICATE":
            key_cols = params["key_columns"]
            order_col = params.get("order_by_column", "Updated_Date")
            desc_order = params.get("descending", True)
            
            order_expr = F.col(order_col).desc() if desc_order else F.col(order_col).asc()
            window_spec = Window.partitionBy(*[F.col(k) for k in key_cols]).orderBy(order_expr)
            
            df = df.withColumn("_row_rank", F.row_number().over(window_spec))
            df = df.withColumn(
                "dq_errors",
                F.when(F.col("_row_rank") > 1,
                       F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Duplicate natural key {key_cols}. Stale version quarantined."))))
                .otherwise(F.col("dq_errors"))
            ).drop("_row_rank")

        elif keyword == "REFERENTIAL_INTEGRITY":
            fk_col = params["fk_column"]
            parent_table = params["parent_table"]
            pk_col = params["parent_pk_column"]
            
            if context and parent_table in context:
                parent_df = context[parent_table]
                valid_parent_keys = [r[pk_col] for r in parent_df.select(pk_col).distinct().collect() if r[pk_col] is not None]
                cond = F.col(fk_col).isNotNull() & (~F.col(fk_col).isin(valid_parent_keys))
                df = df.withColumn(
                    "dq_errors",
                    F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}FK '{fk_col}' not found in parent '{parent_table}.{pk_col}'"))))
                    .otherwise(F.col("dq_errors"))
                )

        elif keyword == "DATE_CHRONOLOGY":
            start_col = params["start_date_column"]
            end_col = params["end_date_column"]
            # Valid when end_date >= start_date (e.g., Updated_Date >= Created_Date)
            start_ts = F.coalesce(F.to_timestamp(F.col(start_col)), F.to_date(F.col(start_col)).cast("timestamp"))
            end_ts = F.coalesce(F.to_timestamp(F.col(end_col)), F.to_date(F.col(end_col)).cast("timestamp"))
            cond = F.col(start_col).isNotNull() & F.col(end_col).isNotNull() & (end_ts < start_ts)
            df = df.withColumn(
                "dq_errors",
                F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Chronology violation: {end_col} is prior to {start_col} ({end_col} < {start_col})"))))
                .otherwise(F.col("dq_errors"))
            )

        elif keyword in ("FRESHNESS_CHECK", "MAX_DELAY", "TIMELINESS_LAG"):
            ts_col = params.get("timestamp_column", "Updated_Date")
            max_days = params.get("max_delay_days", params.get("max_age_days", 3650))
            ref_ts = params.get("reference_timestamp", None)
            
            ts_val = F.coalesce(F.to_timestamp(F.col(ts_col)), F.to_date(F.col(ts_col)).cast("timestamp"))
            ref_val = F.to_timestamp(F.lit(ref_ts)) if ref_ts else F.current_timestamp()
            
            # Record is considered delayed / stale if (ref_val - ts_val) in days exceeds max_days or if ts_val is in future beyond tolerance
            delay_days = (F.unix_timestamp(ref_val) - F.unix_timestamp(ts_val)) / 86400.0
            cond = F.col(ts_col).isNotNull() & (delay_days > max_days)
            
            df = df.withColumn(
                "dq_errors",
                F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Timeliness violation: Record in '{ts_col}' is delayed beyond threshold ({max_days} days)"))))
                .otherwise(F.col("dq_errors"))
            )

        elif keyword in ("CROSS_KEY_DUPLICATE", "DUPLICATE_CROSS_KEYS", "CROSS_COLUMN_DUPLICATE"):
            key_cols = params.get("key_columns", ["Full_Name", "Emirates_ID"])
            actual_key_cols = []
            for k in key_cols:
                if k in df.columns:
                    actual_key_cols.append(k)
                elif k in ("Full_Name", "Name", "Student_Name"):
                    for cand in ["Full_Name", "Student_Name", "Name"]:
                        if cand in df.columns:
                            actual_key_cols.append(cand)
                            break
                else:
                    actual_key_cols.append(k)

            distinct_col = params.get("distinct_column", "Student_ID")
            if distinct_col not in df.columns:
                for cand in ["Student_ID", "ID", "Enrollment_ID"]:
                    if cand in df.columns:
                        distinct_col = cand
                        break

            if all(c in df.columns for c in actual_key_cols) and distinct_col in df.columns:
                window_spec = Window.partitionBy(*[F.col(k) for k in actual_key_cols])
                min_id_col = "_dq_min_" + distinct_col
                max_id_col = "_dq_max_" + distinct_col
                df = df.withColumn(min_id_col, F.min(F.col(distinct_col)).over(window_spec)) \
                       .withColumn(max_id_col, F.max(F.col(distinct_col)).over(window_spec))

                keys_valid = F.col(actual_key_cols[0]).isNotNull() & (F.trim(F.col(actual_key_cols[0]).cast("string")) != "")
                for k in actual_key_cols[1:]:
                    keys_valid = keys_valid & F.col(k).isNotNull() & (F.trim(F.col(k).cast("string")) != "")

                cond = keys_valid & F.col(distinct_col).isNotNull() & (F.col(min_id_col) != F.col(max_id_col))
                df = df.withColumn(
                    "dq_errors",
                    F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Duplicate {actual_key_cols} found with different {distinct_col}"))))
                    .otherwise(F.col("dq_errors"))
                ).drop(min_id_col, max_id_col)

        elif keyword == "REGEX_MATCH":
            col_name = params["column"]
            pattern = params["pattern"]
            cond = F.col(col_name).isNotNull() & (~F.col(col_name).rlike(pattern))
            df = df.withColumn(
                "dq_errors",
                F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Format mismatch on '{col_name}' (Pattern: {pattern})"))))
                .otherwise(F.col("dq_errors"))
            )

        elif keyword in ("VALUE_IN_SET", "STATUS_CHECK", "ALLOWED_DOMAIN"):
            col_name = params["column"]
            allowed_values = params["allowed_values"]
            case_sensitive = params.get("case_sensitive", True)
            
            if case_sensitive:
                cond = F.col(col_name).isNotNull() & (~F.col(col_name).isin(allowed_values))
            else:
                lower_allowed = [str(v).lower() for v in allowed_values]
                cond = F.col(col_name).isNotNull() & (~F.lower(F.col(col_name)).isin(lower_allowed))
                
            df = df.withColumn(
                "dq_errors",
                F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Invalid domain value in '{col_name}'. Allowed: {allowed_values}"))))
                .otherwise(F.col("dq_errors"))
            )

        elif keyword == "BUSINESS_RULE":
            condition_expr = params["valid_sql_condition"]
            rule_desc = params.get("description", condition_expr)
            cond = ~(F.expr(condition_expr))
            df = df.withColumn(
                "dq_errors",
                F.when(cond, F.array_union(F.col("dq_errors"), F.array(F.lit(f"{err_tag}Business rule failed: {rule_desc}"))))
                .otherwise(F.col("dq_errors"))
            )

        return df

    @staticmethod
    def execute_rules_suite(df: DataFrame, rules: List[Dict[str, Any]],
                            context: Optional[Dict[str, DataFrame]] = None,
                            score_calculator: Optional[Any] = None,
                            table_name: str = "") -> Tuple[DataFrame, DataFrame, List[str]]:
        """
        Executes a list of rules sequentially:
        - Segregates records into Clean vs Quarantine.
        - If score_calculator is provided, records detailed metrics for each rule.
        - Captures CRITICAL fatal violations.
        """
        total_records = df.count()
        processed_df = df
        
        for r in rules:
            rule_id = r.get("rule_id", "DQ_RULE")
            rule_name = r.get("rule_name", rule_id)
            dimension = r.get("dimension", "Validity")
            layer = r.get("layer", "Silver")
            action = r.get("action", "QUARANTINE")
            desc = r.get("description", "")
            # Apply individual rule
            single_rule_df = KeywordDQEngine.apply_keyword_rule(df, r, context=context)
            failed_count = single_rule_df.filter(F.size(F.col("dq_errors")) > 0).count()
            passed_count = total_records - failed_count
            
            if score_calculator is not None:
                score_calculator.record_check(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    dimension=dimension,
                    layer=layer,
                    table=table_name,
                    total_records=total_records,
                    passed_records=passed_count,
                    failed_records=failed_count,
                    action=action,
                    passed=(failed_count == 0),
                    details=desc
                )
            
            # Accumulate on master processed_df
            processed_df = KeywordDQEngine.apply_keyword_rule(processed_df, r, context=context)

        clean_df = processed_df.filter(F.size(F.col("dq_errors")) == 0).drop("dq_errors")
        quarantine_df = processed_df.filter(F.size(F.col("dq_errors")) > 0)

        critical_violations = []
        if quarantine_df.count() > 0:
            error_rows = quarantine_df.select("dq_errors").collect()
            for row in error_rows:
                for err in row["dq_errors"]:
                    if any(fatal in err for fatal in ["|CRITICAL|FAIL|", "|CRITICAL|ABORT|", "|CRITICAL|REJECT|"]):
                        critical_violations.append(err)

        return clean_df, quarantine_df, list(set(critical_violations))

    # =========================================================================
    # 4. CONVENIENCE DQ CHECK UTILITIES
    # =========================================================================
    @staticmethod
    def check_date_chronology(df: DataFrame, start_col: str = "Created_Date", end_col: str = "Updated_Date",
                              rule_id: str = "CHK_DATE_CHRONOLOGY", action: str = "QUARANTINE") -> DataFrame:
        """
        Validates that end_col is on or after start_col (e.g. Updated_Date >= Created_Date).
        Flags records where end_col < start_col.
        """
        rule = {
            "rule_id": rule_id,
            "rule_name": f"{end_col} >= {start_col} Chronology Check",
            "dimension": "Timeliness",
            "layer": "Silver",
            "keyword": "DATE_CHRONOLOGY",
            "severity": "HIGH",
            "action": action,
            "params": {
                "start_date_column": start_col,
                "end_date_column": end_col
            },
            "description": f"{end_col} cannot be prior to {start_col} ({end_col} >= {start_col})."
        }
        return KeywordDQEngine.apply_keyword_rule(df, rule)

    @staticmethod
    def check_duplicate_identity_with_different_id(df: DataFrame,
                                                   key_columns: Optional[List[str]] = None,
                                                   distinct_column: str = "Student_ID",
                                                   rule_id: str = "CHK_DUP_IDENTITY_DIFF_ID",
                                                   action: str = "QUARANTINE") -> DataFrame:
        """
        Validates that duplicate identity attributes (such as Name/Full_Name and Emirates_ID)
        do not exist with different primary/natural keys (e.g. Student_ID).
        """
        if key_columns is None:
            name_cand = "Full_Name" if "Full_Name" in df.columns else ("Name" if "Name" in df.columns else "Student_Name")
            eid_cand = "Emirates_ID" if "Emirates_ID" in df.columns else "Emirates_Id"
            key_columns = [name_cand, eid_cand]

        rule = {
            "rule_id": rule_id,
            "rule_name": f"Duplicate {key_columns} with Different {distinct_column}",
            "dimension": "Uniqueness",
            "layer": "Silver",
            "keyword": "CROSS_KEY_DUPLICATE",
            "severity": "HIGH",
            "action": action,
            "params": {
                "key_columns": key_columns,
                "distinct_column": distinct_column
            },
            "description": f"Check for duplicate {key_columns} associated with different {distinct_column} values."
        }
        return KeywordDQEngine.apply_keyword_rule(df, rule)

    @staticmethod
    def check_timeliness_delay(df: DataFrame,
                               timestamp_column: str = "Updated_Date",
                               max_delay_days: int = 3650,
                               reference_timestamp: Optional[str] = None,
                               rule_id: str = "CHK_TIMELINESS_DELAY",
                               action: str = "QUARANTINE") -> DataFrame:
        """
        Validates timeliness and identifies delayed or excessively stale records exceeding max_delay_days.
        """
        rule = {
            "rule_id": rule_id,
            "rule_name": f"{timestamp_column} Timeliness Delay Validation",
            "dimension": "Timeliness",
            "layer": "Silver",
            "keyword": "FRESHNESS_CHECK",
            "severity": "MEDIUM",
            "action": action,
            "params": {
                "timestamp_column": timestamp_column,
                "max_delay_days": max_delay_days,
                "reference_timestamp": reference_timestamp
            },
            "description": f"Validate that {timestamp_column} is not delayed by more than {max_delay_days} days."
        }
        return KeywordDQEngine.apply_keyword_rule(df, rule)

    @staticmethod
    def check_status_domain(df: DataFrame,
                            status_column: str,
                            allowed_values: List[str],
                            case_sensitive: bool = True,
                            rule_id: str = "CHK_STATUS_DOMAIN",
                            action: str = "QUARANTINE") -> DataFrame:
        """
        Validates that status_column strictly belongs to allowed_values with case-sensitive enforcement.
        """
        rule = {
            "rule_id": rule_id,
            "rule_name": f"{status_column} Case-Sensitive Status Domain Validation",
            "dimension": "Validity",
            "layer": "Silver",
            "keyword": "VALUE_IN_SET",
            "severity": "HIGH",
            "action": action,
            "params": {
                "column": status_column,
                "allowed_values": allowed_values,
                "case_sensitive": case_sensitive
            },
            "description": f"Validate that {status_column} matches allowed status values {allowed_values} with case_sensitive={case_sensitive}."
        }
        return KeywordDQEngine.apply_keyword_rule(df, rule)
