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

## 4. Automated Test Suite Execution (Testing Actual Files)

The automated test suite in `tests/test_keyword_framework.py` strictly tests the **actual physical data files and generated layers** without creating inline synthetic test data in code:

```bash
# Run complete test suite
/Users/mirs/PyCharmMiscProject/.venv/bin/python3 -m unittest discover -s tests -p "test_*.py"

# Run end-to-end Medallion pipeline
/Users/mirs/PyCharmMiscProject/.venv/bin/python3 run_pipeline.py
```
