# Case Study Assignment Solution: Senior Data Quality Engineer
**Enterprise Data Platform — Medallion Data Quality Architecture & Control Framework**

---

## Executive Summary & System Architecture

This solution establishes an end-to-end Data Quality (DQ) engineering framework for higher-education student enrollment data within an Enterprise Medallion Architecture (**Bronze → Silver → Gold**). The architecture utilizes a **keyword-driven, reusable configuration engine** in PySpark to enforce comprehensive data profiling, multi-dimensional reconciliation, automated cleansing, deduplication, referential integrity verification, quarantine isolation, SCD Type 2 dimensioning, and wide analytical mart consolidation.

```
+---------------------------------------------------------------------------------------------------------+
|                                     MEDALLION DATA QUALITY ARCHITECTURE                                 |
+---------------------------------------------------------------------------------------------------------+
                                                                                                           
 [ Higher-Education ]       +---------------------------------------------------------------------------+ 
 [ Institutions     ] ===>  | 1. RAW INGESTION & DATA PROFILING (Keyword: PROFILE_DATA)                  | 
 [ CSV Source Feeds ]       |    - Cardinality, null rates, uniqueness %, datatype inference, patterns  | 
                            +---------------------------------------------------------------------------+ 
                                                                |                                         
                                                                v                                         
                            +---------------------------------------------------------------------------+ 
                            | 2. BRONZE LAYER (Exact Ingestion & 4-Way Reconciliation)                  | 
                            |    - RECONCILE_SCHEMA (Column names & types match)                        | 
                            |    - RECONCILE_RECORD_COUNT (Source vs Bronze delta = 0)                  | 
                            |    - RECONCILE_CHECKSUM (Bit-level MD5 payload verification)              | 
                            |    - RECONCILE_NUMERIC_AGG (Sum, Min, Max consistency)                    | 
                            |    - Enriches with metadata: _bronze_ingestion_ts, _bronze_source_file    | 
                            +---------------------------------------------------------------------------+ 
                                                                |                                         
                                                                v                                         
                            +---------------------------------------------------------------------------+ 
                            | 3. SILVER LAYER (Cleansing, Standardization & Rule Enforcement)           | 
                            |    - Standardization: String trimming, ISO-8601 dates, Gender domain (F/M)| 
                            |    - NOT_NULL (Completeness)                                              | 
                            |    - DEDUPLICATE (Uniqueness - latest Updated_Date retained)              | 
                            |    - REGEX_MATCH (Validity - Emirates ID 784-YYYY-XXXXXXX-Z)             | 
                            |    - REFERENTIAL_INTEGRITY (Parent/Child FK validation against masters)   | 
                            |    - DATE_CHRONOLOGY (Consistency - Enrollment_Date <= Graduation_Date)   | 
                            |    - Audit columns: _silver_processed_ts, _pipeline_version, _is_valid    | 
                            +---------------------------------------------------------------------------+ 
                                            |                                           |                 
                        [ Passed All Rules ]|                                           |[ Failed Rules ] 
                                            v                                           v                 
                    +-------------------------------+                   +-------------------------------+ 
                    | SILVER CLEAN STORAGE          |                   | SILVER QUARANTINE STORAGE     | 
                    | (data/silver/clean/)          |                   | (data/silver/quarantine/)     | 
                    | - Clean Institution           |                   | - Isolated Anomaly Records    | 
                    | - Clean Program               |                   | - Enriched dq_errors metadata | 
                    | - Clean Student               |                   | - Steward Remediation Queue   | 
                    | - Clean Enrollment            |                   +-------------------------------+ 
                    +-------------------------------+                                                     
                                    |                                                                     
                    +---------------+---------------+                                                     
                    |                               |                                                     
                    v                               v                                                     
+---------------------------------------+   +-----------------------------------------------------------+ 
| 4. GOLD SCD TYPE 2 DIMENSIONS         |   | 5. GOLD WIDE ANALYTICAL MART (fact_student_enrollment)    | 
| (data/gold/scd2_dimensions/)          |   | (data/gold/analytics_marts/)                              | 
| - dim_student_scd2                    |   | - 4-Table Consolidated Analytical View                    | 
| - version, effective_start_date,      |   | - BUSINESS_RULE (Active Inst, Active Program, Age >= 16)  | 
|   effective_end_date, is_current      |   | - Gold Quarantine: Quarantine_Wide Mart                   | 
+---------------------------------------+   +-----------------------------------------------------------+ 
```

---

## Task 1: Data Profiling

### Profiling Strategy
Before defining rules, automated statistical and structural profiling is conducted on raw ingestion payloads. This prevents false assumptions about incoming feeds and identifies schema drift, missing value concentrations, and domain variances across institutions.

### Main Checks, Statistics & Metrics Captured
1. **Structural Metrics:**
   - Column names, count of columns, inferred Spark datatypes, and schema alignment.
2. **Completeness & Nullability:**
   - Absolute null count, empty string count, string literal `"null"` occurrences, and `null_percentage` per column.
3. **Cardinality & Uniqueness:**
   - Distinct count per column, `uniqueness_ratio = (distinct_count / total_rows) * 100`.
   - Identification of primary key candidates and natural key candidates.
4. **Value Distribution & Domain Frequency:**
   - Frequency tables for low-cardinality categorical fields (`Gender`, `Enrollment_Status`, `Institution_Status`, `Program_Status`).
5. **Pattern & Format Analysis:**
   - Regex compliance rates for formatted codes (`Emirates_ID`, `Institution_Code`, `Program_Code`).
6. **Chronological & Numerical Distributions:**
   - Min, max, mean, standard deviation, and quantile distribution for numerical metrics (`Academic_Year`) and dates (`Date_of_Birth`, `Enrollment_Date`, `Graduation_Date`, `Created_Date`, `Updated_Date`).

---

## Task 2: Data Quality Rules Matrix

The framework implements 19 comprehensive rules across 6 core DQ dimensions:

| Rule ID | Rule Name | DQ Dimension | Layer | Severity | Expected Action | Description & Condition |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **BRZ_REC_01** | Schema Exact Match | Completeness | Bronze | CRITICAL | REJECT / FAIL | Column count, names, and datatypes match source specification. |
| **BRZ_REC_02** | Row Count Reconciliation | Completeness | Bronze | CRITICAL | REJECT / FAIL | Target Bronze row count == Source row count (Delta = 0). |
| **BRZ_REC_03** | Checksum Hash Integrity | Consistency | Bronze | CRITICAL | REJECT / FAIL | Aggregated MD5 bit-level hash matches source payload. |
| **BRZ_REC_04** | Numeric Aggregation Match | Validity | Bronze | HIGH | REJECT / FAIL | SUM, MIN, MAX match across numeric attributes. |
| **INST_DQ_01** | Institution Code Not Null | Completeness | Silver | CRITICAL | QUARANTINE | `Institution_Code IS NOT NULL AND trim(Institution_Code) != ''`. |
| **INST_DQ_02** | Institution Code Format | Validity | Silver | HIGH | QUARANTINE | `Institution_Code RLIKE '^INST\d{3}$'`. |
| **INST_DQ_03** | Institution Status Domain | Validity | Silver | HIGH | QUARANTINE | `Institution_Status IN ('Active', 'Inactive', 'Pending')` (case-sensitive). |
| **PRG_DQ_01** | Program Code Not Null | Completeness | Silver | CRITICAL | QUARANTINE | `Program_Code IS NOT NULL AND trim(Program_Code) != ''`. |
| **PRG_DQ_02** | Program Status Domain | Validity | Silver | HIGH | QUARANTINE | `Program_Status IN ('Active', 'Suspended', 'Discontinued')` (case-sensitive). |
| **PRG_DQ_03** | Program Inst Referential Int | Referential Integrity | Silver | HIGH | QUARANTINE | `Program.Institution_Code` exists in clean `Institution` master. |
| **STU_DQ_01** | Student ID Not Null | Completeness | Silver | CRITICAL | QUARANTINE | `Student_ID IS NOT NULL AND trim(Student_ID) != ''`. |
| **STU_DQ_02** | Emirates ID Format | Validity | Silver | HIGH | QUARANTINE | `Emirates_ID RLIKE '^784-\d{4}-\d{7}-\d{1}$'`. |
| **STU_DQ_03** | Student Deduplication | Uniqueness | Silver | HIGH | QUARANTINE | Deduplicate by `Student_ID`, keep latest `Updated_Date`, quarantine stale. |
| **STU_DQ_04** | Student Inst Referential Int | Referential Integrity | Silver | HIGH | QUARANTINE | `Student.Institution_Code` exists in clean `Institution` master. |
| **STU_DQ_05** | Gender Standard Domain | Validity | Silver | MEDIUM | QUARANTINE | `Gender IN ('F', 'M', 'Other', 'Unknown')`. |
| **STU_DQ_06** | Student Update Timeliness | Timeliness | Silver | HIGH | QUARANTINE | `Updated_Date >= Created_Date`. |
| **STU_DQ_07** | Student Identity Cross-Key Deduplication | Uniqueness | Silver | HIGH | QUARANTINE | Check for duplicate `Full_Name`/`Name` and `Emirates_ID` associated with different `Student_ID` values (`COUNT(DISTINCT Student_ID) > 1`). |
| **STU_DQ_08** | Student Record Freshness & Delay Validation | Timeliness | Silver | MEDIUM | QUARANTINE | Verify record timestamps are within allowable SLA delay threshold (`DATEDIFF(CURRENT_TIMESTAMP, Updated_Date) <= max_days`). |
| **ENR_DQ_01** | Enrollment ID Not Null | Completeness | Silver | CRITICAL | QUARANTINE | `Enrollment_ID IS NOT NULL AND trim(Enrollment_ID) != ''`. |
| **ENR_DQ_02** | Enrollment Student ID Not Null| Completeness | Silver | CRITICAL | QUARANTINE | Foreign key `Student_ID` is not null. |
| **ENR_DQ_03** | Enrollment Student Ref Int | Referential Integrity | Silver | HIGH | QUARANTINE | `Enrollment.Student_ID` exists in clean `Student` master. |
| **ENR_DQ_04** | Enrollment Program Ref Int | Referential Integrity | Silver | HIGH | QUARANTINE | `Enrollment.Program_Code` exists in clean `Program` master. |
| **ENR_DQ_05** | Enrollment Inst Ref Int | Referential Integrity | Silver | HIGH | QUARANTINE | `Enrollment.Institution_Code` exists in clean `Institution` master. |
| **ENR_DQ_06** | Date Chronology Check | Consistency | Silver | HIGH | QUARANTINE | `Graduation_Date IS NULL OR Graduation_Date >= Enrollment_Date`. |
| **ENR_DQ_07** | Enrollment Status Domain | Validity | Silver | MEDIUM | QUARANTINE | `Enrollment_Status IN ('Enrolled', 'Graduated', 'Suspended', 'Withdrawn')` (case-sensitive). |
| **ENR_DQ_08** | Academic Year Range | Validity | Silver | HIGH | QUARANTINE | `Academic_Year BETWEEN 2000 AND 2030`. |
| **GOLD_DQ_01** | Active Institution Rule | Consistency | Gold | HIGH | QUARANTINE | In Gold Mart: `Institution_Status = 'Active'`. |
| **GOLD_DQ_02** | Active Program Rule | Consistency | Gold | HIGH | QUARANTINE | In Gold Mart: `Program_Status = 'Active'`. |
| **GOLD_DQ_03** | Student Minimum Age Rule | Validity | Gold | MEDIUM | QUARANTINE | In Gold Mart: `year(current_date()) - year(Date_of_Birth) >= 16`. |

---

## Task 3: Medallion Architecture Implementation

### 1. Bronze Layer (Raw Ingestion & Source Verification)
- **Controls Implemented:** Exact 1:1 ingestion, source schema reconciliation, row count reconciliation, checksum hash verification, numeric reconciliation, and audit enrichment (`_bronze_ingestion_ts`, `_bronze_source_file`).
- **Why:** The Bronze layer preserves raw history in its immutable state for forensic auditability and replayability without destructive parsing.

### 2. Silver Layer (Standardization, Cleansing & Integrity Isolation)
- **Controls Implemented:**
  - Standardized representations (e.g., `'Female'` / `'FEMALE'` → `'F'`, string whitespace stripping, ISO date parsing).
  - Row-level validation across Completeness, Uniqueness, Validity, Referential Integrity, Consistency, and Timeliness.
  - Automatic split into **Silver Clean** (`data/silver/clean/`) and **Silver Quarantine** (`data/silver/quarantine/`).
  - Audit enrichment: `_silver_processed_ts`, `_pipeline_version`, `_is_valid`.
- **Why:** Silver represents the single source of truth of validated, enterprise-conformed data. Anomalous records are quarantined with failure reasons without halting batch operations.

### 3. Gold Layer (SCD Type 2 Dimensions & Wide Analytical Mart)
- **Controls Implemented:**
  - **SCD Type 2 History (`dim_student_scd2`):** Natural key partitioning with `version`, `effective_start_date`, `effective_end_date` (`9999-12-31 23:59:59` for active), and `is_current` flag.
  - **Consolidated 1 Wide Analytical Table (`fact_student_enrollment_wide`):** 4-way denormalized join of Student, Enrollment, Institution, and Program.
  - **Business Rules Enforcement:** Active institution/program requirements, minimum student age filters, routing invalid business combinations to Gold Quarantine.
- **Why:** Gold caters directly to analytical consumers, BI tools, and executive dashboards requiring conformed dimensional models and high-performance denormalized fact tables.

---

## Task 4: Technical Implementation (Reusable PySpark & SQL)

### 1. Reusable Deduplication Control (Uniqueness Dimension)
```python
# Configuration:
# {"keyword": "DEDUPLICATE", "params": {"key_columns": ["Student_ID"], "order_by_column": "Updated_Date", "descending": True}}

from pyspark.sql.window import Window
import pyspark.sql.functions as F

def execute_deduplication(df, key_cols, order_col="Updated_Date", descending=True):
    order_expr = F.col(order_col).desc() if descending else F.col(order_col).asc()
    window_spec = Window.partitionBy(*[F.col(k) for k in key_cols]).orderBy(order_expr)
    
    ranked_df = df.withColumn("_row_rank", F.row_number().over(window_spec))
    
    clean_df = ranked_df.filter(F.col("_row_rank") == 1).drop("_row_rank")
    quarantine_df = ranked_df.filter(F.col("_row_rank") > 1).drop("_row_rank") \
        .withColumn("dq_errors", F.array(F.lit("STU_DQ_03|Uniqueness|Silver|HIGH|QUARANTINE: Stale duplicate version")))
    return clean_df, quarantine_df
```

Equivalent ANSI SQL:
```sql
WITH RankedStudents AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Student_ID ORDER BY Updated_Date DESC) AS row_rank
    FROM bronze_student
)
-- Clean output:
SELECT * FROM RankedStudents WHERE row_rank = 1;

-- Quarantine output:
SELECT *, 'STU_DQ_03: Duplicate record quarantined' AS dq_error 
FROM RankedStudents WHERE row_rank > 1;
```

### 2. Reusable Referential Integrity Control (Referential Integrity Dimension)
```python
# Configuration:
# {"keyword": "REFERENTIAL_INTEGRITY", "params": {"fk_column": "Student_ID", "parent_table": "student", "parent_pk_column": "Student_ID"}}

def execute_referential_integrity(child_df, parent_df, fk_col, parent_pk_col):
    valid_keys = [r[parent_pk_col] for r in parent_df.select(parent_pk_col).distinct().collect() if r[parent_pk_col] is not None]
    
    invalid_cond = F.col(fk_col).isNotNull() & (~F.col(fk_col).isin(valid_keys))
    
    quarantine_df = child_df.filter(invalid_cond).withColumn(
        "dq_errors", F.array(F.lit(f"RI_VIOLATION|Referential Integrity|Silver|HIGH|QUARANTINE: FK {fk_col} missing in parent"))
    )
    clean_df = child_df.filter(~invalid_cond)
    return clean_df, quarantine_df
```

Equivalent ANSI SQL:
```sql
-- Valid Clean Enrollments:
SELECT e.* 
FROM silver_enrollment e
INNER JOIN silver_student s ON e.Student_ID = s.Student_ID;

-- Quarantined Orphan Enrollments:
SELECT e.*, 'ENR_DQ_03: Student_ID not found in Student master' AS dq_error
FROM silver_enrollment e
LEFT JOIN silver_student s ON e.Student_ID = s.Student_ID
WHERE s.Student_ID IS NULL AND e.Student_ID IS NOT NULL;
```

### 3. Additional Reusable Business Rule (Date Chronology & Logic)
```python
# Configuration:
# {"keyword": "DATE_CHRONOLOGY", "params": {"start_date_column": "Enrollment_Date", "end_date_column": "Graduation_Date"}}

def execute_date_chronology(df, start_col, end_col):
    invalid_cond = F.col(start_col).isNotNull() & F.col(end_col).isNotNull() & (F.to_date(F.col(end_col)) < F.to_date(F.col(start_col)))
    
    clean_df = df.filter(~invalid_cond)
    quarantine_df = df.filter(invalid_cond).withColumn(
        "dq_errors", F.array(F.lit(f"ENR_DQ_06|Consistency|Silver|HIGH|QUARANTINE: {end_col} prior to {start_col}"))
    )
    return clean_df, quarantine_df
```

### 4. Cross-Key Identity Deduplication Control (Uniqueness & Identity Integrity)
```python
# Configuration:
# {"keyword": "CROSS_KEY_DUPLICATE", "params": {"key_columns": ["Full_Name", "Emirates_ID"], "distinct_column": "Student_ID"}}

def execute_cross_key_duplicate_check(df, key_cols=["Full_Name", "Emirates_ID"], distinct_col="Student_ID"):
    # Window to count distinct student IDs per identity combination (Full_Name + Emirates_ID)
    window_spec = Window.partitionBy(*[F.col(k) for k in key_cols])
    
    # Calculate count of distinct IDs per natural identity
    id_counts = df.groupBy(*key_cols).agg(F.countDistinct(distinct_col).alias("_distinct_id_count"))
    joined_df = df.join(id_counts, on=key_cols, how="left")
    
    invalid_cond = F.col("_distinct_id_count") > 1
    
    clean_df = joined_df.filter(~invalid_cond).drop("_distinct_id_count")
    quarantine_df = joined_df.filter(invalid_cond).drop("_distinct_id_count").withColumn(
        "dq_errors", F.array(F.lit("STU_DQ_07|Uniqueness|Silver|HIGH|QUARANTINE|CROSS_KEY_DUPLICATE: Duplicate Emirates_ID & Full_Name across multiple Student_IDs"))
    )
    return clean_df, quarantine_df
```

Equivalent ANSI SQL:
```sql
-- Find conflicting identities sharing same Emirates_ID and Full_Name under different Student_IDs:
WITH ConflictingIdentities AS (
    SELECT Full_Name, Emirates_ID
    FROM bronze_student
    WHERE Emirates_ID IS NOT NULL AND Full_Name IS NOT NULL
    GROUP BY Full_Name, Emirates_ID
    HAVING COUNT(DISTINCT Student_ID) > 1
)
-- Quarantine conflicting records:
SELECT s.*, 'STU_DQ_07: Duplicate Emirates_ID & Full_Name with different Student_ID' AS dq_error
FROM bronze_student s
INNER JOIN ConflictingIdentities c 
    ON s.Full_Name = c.Full_Name AND s.Emirates_ID = c.Emirates_ID;
```

### 4. Reusable Case-Sensitive Status Domain Control (Validity Dimension)
```python
# Configuration:
# {"keyword": "VALUE_IN_SET", "params": {"column": "Institution_Status", "allowed_values": ["Active", "Inactive", "Pending"], "case_sensitive": True}}

def execute_status_domain_check(df, status_col="Institution_Status", allowed_values=None, case_sensitive=True):
    if allowed_values is None:
        allowed_values = ["Active", "Inactive", "Pending"]
    
    if case_sensitive:
        invalid_cond = F.col(status_col).isNotNull() & (~F.col(status_col).isin(allowed_values))
    else:
        lower_allowed = [v.lower() for v in allowed_values]
        invalid_cond = F.col(status_col).isNotNull() & (~F.lower(F.col(status_col)).isin(lower_allowed))
        
    quarantine_df = df.filter(invalid_cond).withColumn(
        "dq_errors", F.array(F.lit(f"STATUS_DQ_ERROR|Validity|Silver|HIGH|QUARANTINE: Invalid case or domain in {status_col}"))
    )
    clean_df = df.filter(~invalid_cond)
    return clean_df, quarantine_df
```

Equivalent ANSI SQL:
```sql
-- Case-sensitive status validation (exact casing):
SELECT * 
FROM bronze_institution
WHERE Institution_Status IS NOT NULL 
  AND Institution_Status NOT IN ('Active', 'Inactive', 'Pending');
```

---

## Task 5: Exception Handling & Remediation

### 1. Reject vs. Quarantine Distinction
- **Reject / Fatal Abort (`REJECT` / `FAIL`):**
  - **When Used:** Catastrophic file corruption, unreadable schema, missing essential batch headers, or byte-level checksum mismatch during Bronze ingestion.
  - **Action:** Halts the pipeline immediately with a `DataQualityException`, triggers incident logging, and rolls back the batch.
- **Quarantine (`QUARANTINE`):**
  - **When Used:** Row-level data quality violations (e.g., missing FK, regex format error, duplicate student key, date chronology conflict, out-of-range value).
  - **Action:** The invalid row is isolated into the corresponding `data/silver/quarantine/` or `data/gold/analytics_marts/quarantine_wide` storage location. Valid rows continue through the pipeline without disruption.

### 2. Capturing Failed Records & Failure Reasons
Each quarantined row preserves the full raw attribute payload and appends an audit column `dq_errors` containing an array of structured error tags:
`RULE_ID|DIMENSION|LAYER|SEVERITY|ACTION|KEYWORD: Descriptive Reason`
Example:
`STU_DQ_02|Validity|Silver|HIGH|QUARANTINE|REGEX_MATCH: Format violation on 'Emirates_ID': Must match pattern ^784-\d{4}-\d{7}-\d{1}$`

### 3. Reprocessing & Remediation Workflow
1. **Quarantine Storage:** Records reside in queryable Delta/Parquet/CSV quarantine tables partitioned by ingestion date and institution.
2. **Data Steward Review:** Domain stewards correct missing student profiles or invalid status codes via a remediation UI or corrective batch upload.
3. **Re-ingestion & Replay:** Corrected records are tagged with `_is_reprocessed = TRUE` and re-injected into the Silver validation stage. When validated, they flow into Silver Clean and Gold dimensions.

---

## Task 6: Comprehensive Reconciliation Framework

Reconciliation between external source systems and the enterprise platform goes beyond gross record counts:

```
+---------------------------------------------------------------------------------------------------------+
|                                  5-POINT SOURCE-TO-PLATFORM RECONCILIATION                              |
+---------------------------------------------------------------------------------------------------------+
| 1. Schema Check:         Column count, ordinal positioning, case-insensitive names, datatype cast match |
| 2. Row Count:            Source Row Count == Target Bronze Row Count (Delta == 0)                       |
| 3. Checksum Hash:        SUM(MD5(concat_ws('||', sorted_columns))) bit-level payload verification       |
| 4. Numeric Aggregation:  SUM(Score/Year), MIN(), MAX(), AVG() consistency                              |
| 5. Categorical Subtotal: Group-by counts per Institution_Code & Academic_Year matched across systems    |
+---------------------------------------------------------------------------------------------------------+
```

---

## Task 7: Data Quality Monitoring Framework

*(Note: Alerts and notification dispatchers omitted as requested; monitoring architecture focuses on metrics collection and quality reporting).*

### 1. Key Metrics & Calculations
- **Overall DQ Score:**
  $$\text{DQ Score} = \left(\frac{\text{Total Records Passing 100\% of Rules}}{\text{Total Input Records}}\right) \times 100$$
- **DQ Score by Dimension:**
  Individual compliance ratios for **Completeness**, **Validity**, **Uniqueness**, **Consistency**, **Referential Integrity**, and **Timeliness**.
- **Rule Failure Breakdown:**
  Count and percentage of failures grouped by `rule_id`, `dimension`, and `source_institution`.
- **Quarantine Volume & Latency:**
  Volume of unresolved quarantined rows and Mean Time to Remediation (MTTR).

### 2. Trend Tracking & Quality Drift
- Historical snapshot table updated at the end of each batch run to monitor quality trends over time and identify degrading source systems.

---

## Task 8: Root-Cause Analysis (Diagnostic Investigation)

### 1. Scenario Overview
- **Yesterday:** Source contained 1,000,000 enrollment records.
- **Today:**
  - **Bronze:** 1,000,000 records
  - **Silver:** 970,000 records (30,000 drop from Bronze)
  - **Gold:** 850,000 records (120,000 drop from Silver)
  - **Total Pipeline Attrition:** 150,000 records from Source to Gold Wide Mart

### 2. Diagnostic Investigation Workflow Diagram

```
+---------------------------------------------------------------------------------------------------------+
| STEP 1: BRONZE VERIFICATION (Source -> Bronze: 1,000,000 -> 1,000,000)                                  |
| * Outcome: Checksum, row count, and schema match 100%. Ingestion pipeline is intact. Zero drop.         |
+---------------------------------------------------------------------------------------------------------+
                                                    |
                                                    v
+---------------------------------------------------------------------------------------------------------+
| STEP 2: SILVER QUARANTINE BREAKDOWN (Bronze -> Silver: 1,000,000 -> 970,000 = 30,000 Records)            |
| Execute query on Silver Quarantine grouped by rule_id and dimension:                                    |
|                                                                                                         |
| SELECT explode(dq_errors) AS rule_error, count(1) AS failure_count                                      |
| FROM silver_quarantine_enrollment                                                                       |
| GROUP BY 1 ORDER BY 2 DESC;                                                                             |
|                                                                                                         |
| Findings:                                                                                               |
| 1. STU_DQ_03 (Deduplication): 15,000 records -> EXPECTED BUSINESS FILTERING (stale duplicate versions)  |
| 2. ENR_DQ_03 / ENR_DQ_04 (Broken FKs): 8,000 records -> DATA QUALITY DEFECT (orphan student/program IDs)|
| 3. ENR_DQ_01 / ENR_DQ_02 (Missing IDs): 4,000 records -> DATA QUALITY DEFECT (Null Student/Enrollment)  |
| 4. ENR_DQ_06 (Date Chronology): 3,000 records -> DATA QUALITY DEFECT (Graduation before Enrollment)    |
|                                                                                                         |
| Diagnosis: 15,000 represents valid deduplication; 15,000 represents legitimate source DQ errors.       |
+---------------------------------------------------------------------------------------------------------+
                                                    |
                                                    v
+---------------------------------------------------------------------------------------------------------+
| STEP 3: GOLD ANALYTICAL MART REDUCTION (Silver -> Gold: 970,000 -> 850,000 = 120,000 Records)           |
| Execute query comparing Silver Clean against Gold Mart:                                                 |
|                                                                                                         |
| SELECT s.Enrollment_Status, i.Institution_Status, p.Program_Status, count(1) AS cnt                     |
| FROM silver_clean_enrollment e                                                                          |
| LEFT JOIN silver_clean_institution i ON e.Institution_Code = i.Institution_Code                         |
| LEFT JOIN silver_clean_program p ON e.Program_Code = p.Program_Code                                     |
| WHERE e.Enrollment_ID NOT IN (SELECT Enrollment_ID FROM gold_fact_enrollment)                           |
| GROUP BY 1, 2, 3;                                                                                       |
|                                                                                                         |
| Findings:                                                                                               |
| 1. GOLD_DQ_01 & GOLD_DQ_02: 70,000 records filtered due to Inactive Institution or Suspended Program    |
|    -> EXPECTED BUSINESS FILTERING (Gold Mart scopes active institutions/programs only).                 |
| 2. Non-Active Enrollment Statuses: 35,000 Graduated/Withdrawn records routed to historical marts        |
|    -> EXPECTED BUSINESS FILTERING (Fact wide mart scopes active academic registrations).                |
| 3. GOLD_DQ_03: 15,000 records failed student minimum age rule (< 16 years old)                          |
|    -> DATA QUALITY DEFECT (Invalid Date_of_Birth values in student master).                             |
+---------------------------------------------------------------------------------------------------------+
                                                    |
                                                    v
+---------------------------------------------------------------------------------------------------------+
| STEP 4: FINAL VERDICT & ACTION PLAN                                                                     |
| * Expected Business Filtering: 120,000 records (15,000 duplicates + 105,000 inactive/historical scoping)|
| * Actual Data Quality Defects: 30,000 records (15,000 in Silver Quarantine + 15,000 in Gold Quarantine)|
| * Remediation Action: Export 30,000 defective records to institution stewards for profile correction.   |
+---------------------------------------------------------------------------------------------------------+
```

### 3. Comprehensive Diagnostic Investigation Methodology

#### Step 1: Bronze Ingestion Integrity Verification (Source → Bronze)
- **Objective:** Establish whether the ingestion pipeline experienced transport data loss, network drops, or truncation.
- **Methodology:** Run cryptographic checksum reconciliation (`SUM(MD5)` across serialized row contents) and compare raw source file line counts against Bronze table record counts.
- **Outcome:** Exact count match (`1,000,000 = 1,000,000`) and matching schema confirm zero transport data loss.

#### Step 2: Silver Quarantine Breakdown (Bronze → Silver Drop: 30,000 Records)
- **Objective:** Classify the 30,000 quarantined records into expected pipeline operations vs. true source anomalies.
- **Methodology:** Unnest `dq_errors` in `silver_quarantine_enrollment` to categorize anomalies by rule identifier and failure reason.
- **Detailed Findings:**
  1. **Deduplication / Version Resolution (`STU_DQ_03` - 15,000 records):** *Expected Business Filtering*. Multi-version student updates where only the latest `Updated_Date` is promoted to clean storage.
  2. **Referential Integrity Defects (`ENR_DQ_03`, `ENR_DQ_04` - 8,000 records):** *Data Quality Defect*. Orphan foreign keys referencing non-existent students or academic programs.
  3. **Mandatory Identifier Completeness (`ENR_DQ_01`, `ENR_DQ_02` - 4,000 records):** *Data Quality Defect*. Missing/null primary keys (`Enrollment_ID` / `Student_ID`).
  4. **Date Chronology (`ENR_DQ_06` - 3,000 records):** *Data Quality Defect*. Chronological paradox where `Graduation_Date < Enrollment_Date`.

#### Step 3: Gold Mart Scoping Analysis (Silver → Gold Drop: 120,000 Records)
- **Objective:** Trace why 120,000 Silver Clean records were omitted from `gold_fact_enrollment`.
- **Methodology:** Anti-join `silver_clean_enrollment` against `gold_fact_enrollment` evaluating business predicates and dimensional filter conditions.
- **Detailed Findings:**
  1. **Inactive Institution / Suspended Program Scoping (`GOLD_DQ_01`, `GOLD_DQ_02` - 70,000 records):** *Expected Business Filtering*. Analytical mart specifically scopes active educational entities.
  2. **Non-Active Enrollment Status Scoping (35,000 records):** *Expected Business Filtering*. Graduated and withdrawn student records routed to specialized historical marts.
  3. **Student Minimum Age Rule (`GOLD_DQ_03` - 15,000 records):** *Data Quality Defect*. Invalid dates of birth resulting in calculated student age `< 16 years`.

### 4. Variance Reconciliation & Synthesis

| Category | Record Count | Contributing Factors | Assessment |
| :--- | :--- | :--- | :--- |
| **Expected Business Filtering** | **120,000** | 15,000 (Deduplicated stale versions) + 70,000 (Inactive entities) + 35,000 (Historical lifecycles) | Valid platform behavior adhering to mart domain boundaries. |
| **True Data Quality Defects** | **30,000** | 15,000 (Silver Quarantine: Broken FKs, Null IDs, Date logic) + 15,000 (Gold Quarantine: Invalid DOB) | Actionable errors requiring data steward remediation. |
| **Total Reconciled Drop** | **150,000** | Source (1,000,000) minus Gold Mart (850,000) | **100% Accounted For** |

### 5. Remediation & Action Plan
1. **Quarantine Export & Steward Notification:** Segregate and export the 30,000 true defect records to regional data stewards with attached error codes.
2. **Upstream Source Gatekeeping:** Coordinate with source application teams to add client-side validations for mandatory fields, valid DOBs, and logical date ranges.
3. **Reprocessing Pipeline:** Once stewards submit corrected records, process them through Silver with `_is_reprocessed = TRUE` to flow into Gold marts.

---

## Assumptions & Design Principles
1. **Immutability of Bronze:** Raw ingestion files are never modified in-place; all transformations and cleansing occur in Silver.
2. **Non-Blocking Quarantine:** High and Medium severity anomalies never abort pipeline execution, guaranteeing downstream availability for clean data.
3. **Cryptographic Payload Matching:** Checksum reconciliation uses MD5 hash over string-serialized column concatenations to identify any bit-level truncation or encoding artifacts.
4. **Natural Key Deduplication:** In accordance with university registration patterns, multiple records with the same `Student_ID` are resolved by `Updated_Date` descending.
