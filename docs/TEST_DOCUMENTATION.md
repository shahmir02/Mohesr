# Medallion Architecture Data Quality & Testing Documentation

## 1. Architecture & Folder Structure Overview

The framework organizes the enterprise data platform following the standard Medallion Architecture pattern with dedicated folder separation for data layers, code components, configurations, and tests:

```
├── config/
│   ├── __init__.py
│   └── rules_config.py             <- Keyword-driven rule suites with Dimensions & Layers
├── data/
│   ├── source/                     <- Raw incoming CSV extracts (Student, Enrollment, Institution, Program)
│   ├── bronze/                     <- Exact ingestion layer + ingestion metadata & reconciliation
│   ├── silver/                     <- Standardized, deduplicated, cleansed layer
│   │   ├── clean/                  <- Validated records ready for analytics/reporting
│   │   └── quarantine/             <- Intercepted anomalies with detailed DQ error tags
│   └── gold/                       <- Consumption & analytical mart layer
│       ├── scd2_dimensions/        <- Slowly Changing Dimension Type 2 (dim_student_scd2)
│       └── analytics_marts/        <- 4-Table Consolidated Wide Mart & Mart Quarantine
├── docs/
│   └── TEST_DOCUMENTATION.md       <- Full Test Plan, Rules Matrix, and DQ Strategy
├── src/
│   ├── __init__.py
│   ├── keyword_dq_engine.py        <- Core Keyword Data Quality validation engine
│   └── medallion_pipeline.py       <- Layer-by-layer ingestion, ETL, and SCD2 pipeline
├── tests/
│   ├── __init__.py
│   └── test_keyword_framework.py   <- Comprehensive unit and integration test suite (File-based)
├── CASE_STUDY_SOLUTION.md          <- Complete Senior DQ Engineer Case Study Solution Document
├── run_pipeline.py                 <- Main pipeline orchestration runner script
└── README.md                       <- Quickstart guide
```

---

## 2. Layer-by-Layer Data Quality Strategy & Dimensions

### Layer 1: Source Layer (Raw Data Ingestion)
- **Objective:** Ingest raw CSV feeds from external higher-education institutions without alteration.
- **Data Quality Check:** `PROFILE_DATA`
  - Calculates column datatypes, null counts, null percentages, distinct values, and uniqueness ratios across all attributes.

### Layer 2: Bronze Layer (Exact Ingestion & 4-Way Reconciliation)
- **Objective:** Exact 1:1 raw copy preserving original source state, enriched with ingestion audit metadata (`_bronze_ingestion_ts`, `_bronze_source_file`).
- **Data Quality Checks (4-Way Reconciliation):**
  1. `RECONCILE_SCHEMA` (*Completeness*): Verifies exact match of column names and structural datatypes.
  2. `RECONCILE_RECORD_COUNT` (*Completeness*): Confirms row counts match between Source and Bronze (zero delta).
  3. `RECONCILE_CHECKSUM` (*Consistency*): MD5 bit-level payload hash aggregation to guarantee zero data corruption.
  4. `RECONCILE_NUMERIC_AGG` (*Validity*): Verifies SUM, MIN, MAX aggregations across numeric columns.

### Layer 3: Silver Layer (Standardization, Deduplication & Validation)
- **Objective:** Data cleansing, reference value standardization, deduplication, audit column enrichment (`_silver_processed_ts`, `_pipeline_version`, `_is_valid`), and anomaly quarantine routing.
- **Data Quality Checks & Dimensions:**
  - `NOT_NULL` (*Completeness*): Mandatory primary and foreign key completeness check (e.g., `Student_ID`, `Enrollment_ID`).
  - `DEDUPLICATE` (*Uniqueness*): Resolves multiple versions using window partitioning by natural keys and ordering by `Updated_Date`, keeping latest profile and quarantining stale duplicates.
  - `CROSS_KEY_DUPLICATE` (*Uniqueness / Identity Integrity*): Identifies collision anomalies where natural identities (`Full_Name` + `Emirates_ID`) are assigned across conflicting `Student_ID` keys (`STU_DQ_07`).
  - `REGEX_MATCH` (*Validity*): Standard format validation (e.g., Emirates ID `^784-\d{4}-\d{7}-\d{1}$`, Institution Code `^INST\d{3}$`).
  - `VALUE_IN_SET` (*Validity*): Reference domain integrity (e.g., Gender `['F', 'M', 'Other', 'Unknown']`, Enrollment Status `['Enrolled', 'Graduated', 'Suspended', 'Withdrawn']`).
  - `REFERENTIAL_INTEGRITY` (*Referential Integrity*): Cross-table foreign key validation:
    - Enrollment `Student_ID` -> Student Master
    - Enrollment `Program_Code` -> Program Master
    - Enrollment `Institution_Code` -> Institution Master
    - Program `Institution_Code` -> Institution Master
    - Student `Institution_Code` -> Institution Master
  - `DATE_CHRONOLOGY` (*Consistency / Timeliness*): Validates timeline dependencies (e.g., `Graduation_Date >= Enrollment_Date`, `Updated_Date >= Created_Date`).

### Layer 4: Gold Layer (SCD Type 2 & Analytical Marts)
- **Objective:** Business-ready dimensional models and consolidated analytical wide marts.
- **Transformations & Data Quality:**
  1. **SCD Type 2 Dimensioning (`dim_student_scd2`):**
     - Tracks historical record changes with `version`, `effective_start_date`, `effective_end_date` (defaulting to `9999-12-31 23:59:59` for active), and `is_current` boolean flag.
  2. **1 Wide Consolidated Analytical Mart (`fact_student_enrollment_wide`):**
     - Consolidates Student, Enrollment, Institution, and Program into a single denormalized table.
  3. `BUSINESS_RULE` (*Consistency & Validity*): Gold layer business validations:
     - `Institution_Status = 'Active'`
     - `Program_Status = 'Active'`
     - `Student Minimum Age >= 16 years`

---

## 3. Severity Matrix & Action Routing (Quarantine vs Reject)

| Severity Level | Action | Behavior & Routing | Example Rules |
| :--- | :--- | :--- | :--- |
| **CRITICAL** | `REJECT` / `FAIL` | Aborts the pipeline immediately with a `DataQualityException` when payload or structural integrity is fatally compromised. | Total source schema corruption, missing batch headers, checksum failure. |
| **CRITICAL** | `QUARANTINE` | Blocks records missing vital identifiers (e.g. `Student_ID` IS NULL) from entering clean Silver/Gold tables, isolating them in `data/silver/quarantine/`. | `STU_DQ_01`, `ENR_DQ_01`, `INST_DQ_01`, `PRG_DQ_01`. |
| **HIGH** | `QUARANTINE` | Intercepts invalid records (duplicate versions, broken foreign keys, date conflicts) and routes them to quarantine with error trace tags. | `DEDUPLICATE`, `REFERENTIAL_INTEGRITY`, `DATE_CHRONOLOGY`, `REGEX_MATCH`. |
| **MEDIUM** | `QUARANTINE` | Flags business anomaly values or domain discrepancies into quarantine for data steward review. | `VALUE_IN_SET`, `Academic_Year Logical Range`, `Student Minimum Age`. |
| **LOW** | `WARN` / `LOG` | Emits pipeline warnings for informational metrics while allowing records to pass through. | Data profiling skew, non-standard naming casing. |

---

## 4. Comprehensive Test Suite & Automation Tasks

The test suite in `tests/test_keyword_framework.py` covers 13 core test tasks testing physical files and validation logic:

| Task # | Test Name | Dimension | Rule Reference | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Task 01** | `test_01_source_to_bronze_reconciliation` | Completeness & Consistency | `BRZ_REC_01` to `04` | 4-Way Reconciliation: Schema exact match, row count delta = 0, SHA-256/MD5 payload checksum, numeric aggregations. |
| **Task 02** | `test_02_silver_mandatory_key_completeness` | Completeness | `STU_DQ_01`, `ENR_DQ_01`, `INST_DQ_01`, `PRG_DQ_01` | Mandatory primary key not null / not blank validation. |
| **Task 03** | `test_03_silver_regex_validity` | Validity | `STU_DQ_02`, `INST_DQ_02` | Format regex validation (Emirates ID `^784-\d{4}-\d{7}-\d{1}$`, Institution Code `^INST\d{3}$`). |
| **Task 04** | `test_04_silver_value_in_set_validity` | Validity | `STU_DQ_05`, `ENR_DQ_07`, `INST_DQ_03`, `PRG_DQ_02` | Reference domain sets (`Gender`, `Enrollment_Status`, `Institution_Status`, `Program_Status`). |
| **Task 05** | `test_05_silver_referential_integrity` | Referential Integrity | `ENR_DQ_03`, `ENR_DQ_04`, `ENR_DQ_05`, `PRG_DQ_03`, `STU_DQ_04` | Cross-entity FK relationship checks against parent Silver master tables. |
| **Task 06** | `test_06_silver_date_chronology` | Consistency & Timeliness | `ENR_DQ_06` | Chronological sequencing (`Graduation_Date >= Enrollment_Date`). |
| **Task 07** | `test_07_silver_academic_year_range` | Validity | `ENR_DQ_08` | Logical operational year range check (`2000 <= Academic_Year <= 2030`). |
| **Task 08** | `test_08_silver_quarantine_error_tagging` | Consistency | Quarantine Engine | Structured error tag validation on quarantined records (`dq_errors` array). |
| **Task 09** | `test_09_gold_scd2_dimension_validation` | Consistency | `GOLD_SCD2_01` | SCD Type 2 temporal integrity (`effective_start_date`, `effective_end_date`, `is_current`, `version`). |
| **Task 10** | `test_10_gold_wide_mart_and_business_rules` | Completeness & Validity | `GOLD_DQ_01`, `02`, `03` | 18-column analytical mart consolidation and business rules (`Active` status, student age >= 16). |
| **Task 11** | `test_11_test_summary_csv_and_file_presence` | Completeness | Test Artifacts | Verification of all layer physical CSV files and `dq_test_cases_summary.csv`. |
| **Task 12** | `test_12_date_chronology_created_updated_dates` | Timeliness | `STU_DQ_06` | Verification that `Updated_Date >= Created_Date` in student master. |
| **Task 13** | `test_13_duplicate_name_emirates_id_with_different_student_id` | Uniqueness & Consistency | `STU_DQ_07` / `CROSS_KEY_DUPLICATE` | Detection and quarantine of identity collisions sharing identical (`Full_Name`, `Emirates_ID`) under different `Student_ID` values. |
| **Task 14** | `test_14_timeliness_delayed_records_validation` | Timeliness | `STU_DQ_08` / `FRESHNESS_CHECK` | Verification and detection of delayed or excessively stale records exceeding ingestion SLA threshold windows. |
| **Task 15** | `test_15_case_sensitive_status_validation` | Validity & Consistency | `INST_DQ_03`, `PRG_DQ_02`, `ENR_DQ_07` / `VALUE_IN_SET` | Case-sensitive domain validation for `Institution_Status`, `Program_Status`, and `Enrollment_Status`. |

---

## 5. Automated Test Suite Execution (Testing Actual Files)

The automated test suite in `tests/test_keyword_framework.py` strictly tests the **actual physical data files and generated layers** without creating inline synthetic test data in code:

```bash
# Run complete test suite
/Users/mirs/PyCharmMiscProject/.venv/bin/python3 -m unittest discover -s tests -p "test_*.py"

# Run end-to-end Medallion pipeline
/Users/mirs/PyCharmMiscProject/.venv/bin/python3 run_pipeline.py
```
