# Comprehensive Data Pipeline, Architecture & Process Documentation
**Author:** mirs  
**Version:** 1.2.0  
**Domain:** Higher Education / UAE Ministry of Education Data Platform  
**Architecture:** Medallion Architecture (Bronze → Silver → Gold) with Keyword Data Quality Engine

---

## 1. Executive Overview & System Architecture

This platform provides an enterprise-grade Data Quality (DQ) and Data Engineering framework built in PySpark for ingesting, validating, cleansing, dimensionally modeling, and reporting on higher education student enrollment data.

The system is architected around the **Medallion Architecture pattern** with an integrated, declarative **Keyword-Driven Data Quality Engine**.

```
+---------------------------------------------------------------------------------------------------------------+
|                                      END-TO-END DATA FLOW ARCHITECTURE                                        |
+---------------------------------------------------------------------------------------------------------------+

  [ 1. INPUT LAYER ]           [ 2. MIDDLE PROCESS: MEDALLION PIPELINE ]                [ 3. OUTPUT LAYER ]
  
  +------------------+         +---------------------------------------+                +-------------------------+
  | Raw Source Files |         | BRONZE LAYER                          |                | GOLD ANALYTICS MARTS    |
  | (data/source/)   | =====>  | (data/bronze/)                        | =====+         | (data/gold/)            |
  | - student.csv    |         | - 1:1 Ingestion                       |      |         | - fact_student_enrollment|
  | - institution.csv|         | - 4-Way Reconciliation                |      |         |   _wide (18-column 360) |
  | - program.csv    |         | - Audit Metadata                      |      |         | - dim_student_scd2      |
  | - enrollment.csv |         +---------------------------------------+      |         |   (SCD Type 2 History)  |
  +------------------+                             |                          |         +-------------------------+
                                                   v                          |                      ^
                               +---------------------------------------+      |                      |
                               | SILVER LAYER                          |      |                      |
                               | (data/silver/)                        | =====+                      |
                               | - Standardization & Trimming          |                             |
                               | - Keyword DQ Rules Engine             |                             |
                               |   (19 Rules across 6 Dimensions)      |                             |
                               +---------------------------------------+                             |
                                       /                       \                                     |
                         [Clean Data] /                         \ [Quarantined Data]                 |
                                     v                           v                                   |
                       +-------------------------+   +-------------------------+                     |
                       | SILVER CLEAN            |   | SILVER QUARANTINE       |                     |
                       | (data/silver/clean/)    |   | (data/silver/quarantine)|                     |
                       | - Validated Records     |   | - Isolated Anomalies    |                     |
                       | - Fed into Gold Marts   |   | - Enriched `dq_errors`  |                     |
                       +-------------------------+   +-------------------------+                     |
                                    |                                                                |
                                    +----------------------------------------------------------------+
                                                                     |
                                                                     v
                                                     +---------------------------------+
                                                     | GOVERNANCE & REPORTING OUTPUTS  |
                                                     | (reports/ & docs/)              |
                                                     | - HTML DQ Scorecard Dashboard   |
                                                     | - JSON & Markdown Metric Summaries|
                                                     | - Test Summary Matrix (CSV)     |
                                                     | - Allure Test Automation Suite  |
                                                     +---------------------------------+
```

---

## 2. Input Layer Detailed Specification

The pipeline consumes four raw relational CSV files representing the core higher-education ecosystem:

### 2.1. Source Datasets & Schemas

#### A. Student Master (`data/source/student.csv`)
Captures student demographic and biographical information.
- `Student_ID` (*String*): Unique student identifier (e.g. `STU001`). Primary key.
- `Emirates_ID` (*String*): UAE National Identification number in standard format `784-YYYY-XXXXXXX-Z`.
- `Full_Name` (*String*): Student full name.
- `Date_of_Birth` (*Date YYYY-MM-DD*): Birth date.
- `Gender` (*String*): Gender indicator (`M`, `F`, `Male`, `Female`). Standardized in Silver to `M`/`F`.
- `Nationality_Code` (*String*): ISO-3166 3-letter country code (e.g., `ARE`, `GBR`, `OMN`, `USA`, `IND`).
- `Institution_Code` (*String*): Foreign key referencing the Institution master.
- `Created_Date` (*Timestamp*): Record creation timestamp.
- `Updated_Date` (*Timestamp*): Record modification timestamp. Must satisfy `Updated_Date >= Created_Date`.

#### B. Institution Master (`data/source/institution.csv`)
Captures accredited higher-education entities in the UAE.
- `Institution_Code` (*String*): Unique institution code matching pattern `^INST\d{3}$` (e.g., `INST001`). Primary key.
- `Institution_Name` (*String*): Legal name of university / institute.
- `Institution_Status` (*String*): Operational status (`Active`, `Inactive`, `Pending`).

#### C. Program Master (`data/source/program.csv`)
Captures academic degrees and diplomas offered across institutions.
- `Program_Code` (*String*): Unique program code (e.g., `PRG_CS`, `PRG_MED`). Primary key.
- `Institution_Code` (*String*): Foreign key linking the program to its parent institution.
- `Program_Name` (*String*): Degree title (e.g., *BSc Computer Science*, *Doctor of Medicine*).
- `Program_Status` (*String*): Academic standing (`Active`, `Suspended`, `Discontinued`).

#### D. Enrollment Transaction Feed (`data/source/enrollment.csv`)
Captures individual academic enrollment events and progression.
- `Enrollment_ID` (*String*): Unique enrollment transaction identifier (e.g., `ENR001`). Primary key.
- `Student_ID` (*String*): Foreign key referencing `student.csv`.
- `Institution_Code` (*String*): Foreign key referencing `institution.csv`.
- `Program_Code` (*String*): Foreign key referencing `program.csv`.
- `Academic_Year` (*Integer*): 4-digit academic intake year (e.g., `2023`). Allowed range `[2000, 2030]`.
- `Enrollment_Status` (*String*): Lifecycle state (`Enrolled`, `Graduated`, `Suspended`, `Withdrawn`, `Transferred`).
- `Enrollment_Date` (*Date YYYY-MM-DD*): Date of admission.
- `Graduation_Date` (*Date YYYY-MM-DD*, Optional): Date of degree conferral. Must satisfy `Graduation_Date >= Enrollment_Date`.

---

## 3. Middle Process: Medallion Pipeline & DQ Engine

The middle processing layer transforms raw, untrusted source data into analytics-ready models through rigorous validation, reconciliation, cleansing, and dimensional modeling.

### 3.1. Pre-Ingestion Data Profiling
- **Engine Component:** `KeywordDQEngine.profile_dataframe()`
- **Capabilities:**
  - Evaluates row counts, column types, and schema completeness.
  - Computes null counts, null percentages, and cardinality ratios for every column.
  - Calculates distribution frequencies for low-cardinality categorical fields (`Gender`, `Enrollment_Status`, `Institution_Status`).
  - Validates code structure and regex compliance before data enters pipeline stages.

### 3.2. Bronze Layer: Raw Ingestion & 4-Way Reconciliation
The Bronze layer guarantees **exact 1:1 raw capture** with zero data loss or silent type coercion.

- **Storage Location:** `data/bronze/<table_name>/` (Stored as CSV with headers for direct inspection + Parquet).
- **Audit Metadata Injected:**
  - `_bronze_ingestion_ts`: Ingestion timestamp (UTC).
  - `_bronze_source_file`: Originating file name/path.
  - `_bronze_record_hash`: Row-level MD5 payload hash.
- **4-Way Reconciliation Protocol:**
  1. **Schema Exact Match (`BRZ_REC_01`):** Compares column names and datatypes against source specs.
  2. **Row Count Reconciliation (`BRZ_REC_02`):** Asserts `Count(Bronze) - Count(Source) == 0`.
  3. **Payload Checksum Integrity (`BRZ_REC_03`):** Computes cryptographic bit-level SHA-256 payload checksums across entire datasets to detect byte corruption.
  4. **Numeric Aggregation Check (`BRZ_REC_04`):** Verifies sums, mins, and maxs of key numerical values across layers.

### 3.3. Silver Layer: Cleansing, Standardization & Keyword DQ Engine
The Silver layer applies data standardization and executes declarative quality checks.

- **Standardization Operations:**
  - Strips leading/trailing whitespace across all string columns.
  - Normalizes case formatting (uppercasing codes, standardizing name casings).
  - Maps gender aliases (`Male`, `female`, `M`, `F`) to standardized single-letter domains (`M`, `F`).
  - Standardizes ISO-8601 date and timestamp formatting.
- **Declarative Keyword DQ Validation (`config/rules_config.py`):**
  - **Completeness:** `NOT_NULL` on mandatory primary/foreign keys (`Student_ID`, `Institution_Code`, `Program_Code`, `Enrollment_ID`).
  - **Validity:** `REGEX_MATCH` (Emirates ID `^784-\d{4}-\d{7}-\d{1}$`, Institution Code `^INST\d{3}$`).
  - **Allowed Values:** `VALUE_IN_SET` (`Enrollment_Status`, `Institution_Status`, `Program_Status`, `Gender`).
  - **Range Checks:** `RANGE_CHECK` (Academic Year between `2000` and `2030`).
  - **Referential Integrity:** `REFERENTIAL_INTEGRITY` verifying child foreign keys against validated Silver master tables.
  - **Date Chronology:** `DATE_CHRONOLOGY` ensuring logical sequencing:
    - `Enrollment.Graduation_Date >= Enrollment.Enrollment_Date`
    - `Student.Updated_Date >= Student.Created_Date`
  - **Cross-Key Deduplication:** `CROSS_KEY_DUPLICATE` detecting duplicate identities (`Full_Name` + `Emirates_ID`) assigned to conflicting `Student_ID` keys.
- **Automated Quarantine Routing:**
  - Clean rows (zero DQ violations) → `data/silver/clean/<table_name>/` (`_is_valid = True`).
  - Defective rows → `data/silver/quarantine/<table_name>/` (`_is_valid = False`, `dq_errors = [...]`).

### 3.4. Gold Layer: Dimensional Modeling & Analytics Marts
The Gold layer models business analytics datasets from clean Silver inputs.

#### A. Slowly Changing Dimension Type 2 (`dim_student_scd2`)
- **Storage:** `data/gold/scd2_dimensions/dim_student_scd2/`
- **Methodology:**
  - Tracks historical profile changes for each `Student_ID`.
  - Generates surrogate keys: `student_sk = MD5(Student_ID + Updated_Date)`.
  - Determines `effective_start_date`, `effective_end_date` (`9999-12-31 23:59:59` for active), `is_current` flag (`True`/`False`), and continuous `version` numbering.

#### B. Fact Student Enrollment Wide Mart (`fact_student_enrollment_wide`)
- **Storage:** `data/gold/analytics_marts/fact_student_enrollment_wide/`
- **Structure:** 18-column denormalized 360-degree analytical model joining `Enrollment` with current `Student`, `Institution`, and `Program`.
- **Business Rule Enforcement:**
  - Filters and flags records where `Institution_Status != 'Active'` or `Program_Status != 'Active'`.
  - Enforces minimum legal admission age (`Age_At_Enrollment >= 16`).

---

## 4. Output Layer Detailed Specification

The pipeline outputs structured datasets, analytical models, and governance dashboards:

### 4.1. Core Data Deliverables
1. **Gold Wide Fact Table (`data/gold/analytics_marts/fact_student_enrollment_wide/`):**
   - Consolidated 18-column analytical mart ready for PowerBI / Tableau / SQL analytics.
2. **Gold SCD2 Dimension Table (`data/gold/scd2_dimensions/dim_student_scd2/`):**
   - Complete historical versioning of student records with temporal validity ranges.
3. **Silver Clean Datasets (`data/silver/clean/`):**
   - Cleaned, validated, and normalized master and transactional datasets.
4. **Silver Quarantine Datasets (`data/silver/quarantine/`):**
   - Isolated records failing DQ rules, enriched with specific error messages in `dq_errors`.

### 4.2. Governance, Monitoring & Scorecard Outputs
1. **Interactive HTML DQ Scorecard (`reports/dq_scorecard_report.html`):**
   - Executive dashboard presenting overall data health (97.78%), layer pass rates, dimension health, quarantine summaries, and individual rule breakdown.
2. **Machine-Readable DQ Summaries (`reports/dq_summary.json`, `reports/dq_summary.md`):**
   - Structured JSON and Markdown metrics for CI/CD gates and automated alerting.
3. **Plain-English Test Cases Summary (`data/dq_test_cases_summary.csv` & `docs/`):**
   - Full test matrix detailing rule IDs, categories, DQ dimensions, plain English descriptions, and equivalent SQL validation queries.
4. **Allure Test Execution Suite (`reports/allure-results/`):**
   - Comprehensive test execution evidence spanning unit, integration, reconciliation, and physical file assertions.

---

## 5. Script Catalog: Continuous Pipeline vs. One-Time Scripts

To provide clear operational boundaries, scripts are categorized into **core pipeline execution scripts** and **one-time administrative / utility scripts**.

### 5.1. Continuous / Pipeline Execution Scripts
These scripts form the daily/scheduled production pipeline:
- `scripts/run_pipeline.py`: Orchestrates the full Medallion ETL, reconciliation, DQ rule execution, SCD2 modeling, and Gold mart generation.
- `src/medallion_pipeline.py`: Core PySpark pipeline implementation for Bronze, Silver, and Gold transformations.
- `src/keyword_dq_engine.py`: Declarative engine executing keyword rules, profiling, reconciliation, and quarantine routing.
- `src/dq_score.py`: Computes weighted Data Quality scores across dimensions and compiles the interactive HTML governance scorecard.

### 5.2. One-Time & Standalone Utility Scripts
These scripts are administrative or documentation utilities, not standard automated pipeline outputs:

1. **`scripts/generate_pdf_docs.py` / `src/pdf_generator.py`:**
   - **Purpose:** Standalone administrative utility to generate high-resolution executive PDF documentation (`docs/DATA_DOCUMENTATION.pdf`).
   - **Clarification:** The PDF file is **NOT a recurring data pipeline output**. It is an on-demand documentation generator used for executive presentation and technical handovers.
2. **`src/generate_test_summary_csv.py`:**
   - **Purpose:** One-time / on-demand utility to export the 19 Data Quality test cases into CSV matrices (`docs/dq_test_cases_summary.csv`, `reports/`, `data/`).
   - **Usage:** Run when test matrices are updated or when auditing data quality controls for regulatory compliance.
3. **`run_all.py` / `run.sh`:**
   - **Purpose:** Master wrapper orchestrating the end-to-end suite: pipeline run + scoring + test summary export + PDF generation + PyTest suite execution.

---

## 6. Synthetic Test Data Generation Methodology

The synthetic test dataset was specifically designed to rigorously evaluate every layer of the Medallion Architecture, validate all 6 Data Quality dimensions, and test the SCD Type 2 dimension engine.

### 6.1. Generation Strategy & Intentional Anomaly Injection

Synthetic records were crafted to represent realistic higher-education data patterns in the UAE while injecting controlled edge cases and data defects:

| Target Rule / Dimension | Injected Anomaly Scenario | Test Record Example | Expected Pipeline Action |
| :--- | :--- | :--- | :--- |
| **Completeness (`NOT_NULL`)** | Missing mandatory `Student_ID` in student master | `,784-2000-5555555-5,Omar Hassan Al-Ali...` | Intercepted in Silver; routed to `silver/quarantine/student` |
| **Completeness (`NOT_NULL`)** | Missing mandatory `Student_ID` in enrollment feed | `ENR013,,INST004,PRG_AI,2023,Enrolled...` | Intercepted in Silver; routed to `silver/quarantine/enrollment` |
| **Validity (`REGEX_MATCH`)** | Malformed Emirates ID format | `STU005,INVALID_EID_FORMAT_999,Mariam Khalil...` | Intercepted; flagged with regex failure |
| **Validity (`REGEX_MATCH`)** | Non-standard Institution Code pattern | `STU012,784-2000-3030303-3,Chen Wei,...,INST_INVALID_CODE` | Intercepted; flagged with regex pattern error |
| **Validity (`VALUE_IN_SET`)** | Unrecognized Gender value | `STU013,784-2002-4040404-4,Khadija Bilal,...,UNKNOWN_GENDER` | Quarantined; does not map to allowed domain `('M', 'F')` |
| **Validity (`VALUE_IN_SET`)** | Invalid enrollment lifecycle status | `ENR017,STU009,INST005,PRG_AV,2023,INVALID_STATUS...` | Quarantined; does not match permitted lifecycle states |
| **Validity (`RANGE_CHECK`)** | Out-of-bounds Academic Intake Year | `ENR016,STU008,INST004,PRG_CYBER,1990,Enrolled...` | Quarantined; year `1990` falls outside valid range `[2000, 2030]` |
| **Referential Integrity** | Child enrollment referencing non-existent student | `ENR005,STU_NON_EXISTENT_999,INST001,PRG_CS...` | Quarantined; foreign key failed validation against clean student master |
| **Referential Integrity** | Child program referencing non-existent institution | `STU006,...,INST_NON_EXISTENT_999...` | Quarantined; foreign key failed validation against clean institution master |
| **Timeliness / Chronology** | Graduation date prior to Enrollment date | `ENR006,STU001,INST001,PRG_CS,2023,Graduated,2023-08-15,2022-06-30` | Quarantined; `Graduation_Date < Enrollment_Date` violates chronology |
| **Timeliness / Chronology** | Updated date prior to initial Created date | `STU103,...,Created: 2023-03-01, Updated: 2023-02-01` | Flagged; `Updated_Date < Created_Date` violates chronology |
| **Uniqueness (Cross-Key)** | Duplicate Name & Emirates ID with different Student ID | `STU001` & `STU999` both with `Fatima Ahmed`, `784-2001-1234567-1` | Intercepted in Silver; flagged for steward remediation |
| **SCD Type 2 History** | Multiple temporal updates for same student identity | `STU002` (v1: 2023-04-10, v2: 2023-04-11) | Processed into `dim_student_scd2` with v1 (`is_current=False`) and v2 (`is_current=True`) |

### 6.2. SCD Type 2 Data Modeling Verification
To verify the Slowly Changing Dimension Type 2 tracking, multiple state changes were generated for `STU002` (Zayed Sultan Al-Nahyan):
1. **Initial State (Version 1):** Ingested with `Updated_Date = 2023-04-10 14:20:00`.
2. **Updated State (Version 2):** Name revised to `Zayed S. Al-Nahyan` with `Updated_Date = 2023-04-11 10:00:00`.
3. **Pipeline Outcome:**
   - Version 1 is assigned `effective_start_date = 2023-04-10`, `effective_end_date = 2023-04-11`, and `is_current = False`.
   - Version 2 is assigned `effective_start_date = 2023-04-11`, `effective_end_date = 9999-12-31 23:59:59`, and `is_current = True`.

---

## 7. Execution & Verification Guide

To run the complete data quality framework, verify tests, and inspect outputs:

```bash
# 1. Ensure Homebrew Git & Python environment
export PATH="/opt/homebrew/bin:$PATH"

# 2. Run master end-to-end workflow
python run_all.py

# 3. Inspect generated deliverables
open reports/dq_scorecard_report.html
open docs/DATA_DOCUMENTATION.pdf
```
