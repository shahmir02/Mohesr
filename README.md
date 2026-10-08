# Keyword-Driven Data Quality Framework & Medallion Pipeline
**Author:** mirs  
**Version:** 1.2.0  

A production-ready PySpark framework implementing the Medallion Architecture (`Source` → `Bronze` → `Silver` → `Gold`) with keyword-driven data quality checks, 4-way source-to-bronze reconciliation, automated quarantine isolation, Slowly Changing Dimension Type 2 (SCD2) history tracking, executive HTML/PDF governance scorecards, and Allure test reports.

---

## 📖 Key Documentation
- **[Data Pipeline Architecture & Data Flow Guide](docs/DATA_FLOW_AND_PIPELINE_DOCUMENTATION.md)**: Full breakdown of Inputs, Middle Processing (Bronze/Silver/Gold), Outputs, One-Time Scripts, and Test Data Generation Methodology.
- **[Case Study Solution Document](CASE_STUDY_SOLUTION.md)**: In-depth Senior DQ Engineer technical assignment design.
- **[Test Documentation & Rules Matrix](docs/TEST_DOCUMENTATION.md)**: Detailed DQ test cases across all 6 dimensions.
- **[Explanation & Presentation Guide](EXPLANATION_GUIDE.md)**: Architectural talking points and walkthrough guide.

---

## 📁 Project Structure

```
├── config/
│   ├── __init__.py
│   └── rules_config.py             <- Declarative keyword rules across 6 DQ dimensions & layers
├── data/
│   ├── source/                     <- Raw incoming CSV extracts (Student, Enrollment, Institution, Program)
│   ├── bronze/                     <- CSV (Exact 1:1 ingestion copy with metadata headers for inspection)
│   ├── silver/                     <- Standardized, cleansed, and validated layer
│   │   ├── clean/                  <- CSV & Parquet: Valid records passing all DQ checks
│   │   └── quarantine/             <- CSV & Parquet: Intercepted anomalies with dq_errors arrays
│   └── gold/                       <- High-value analytical models & consumption layer
│       ├── scd2_dimensions/        <- CSV & Parquet: SCD Type 2 history (dim_student_scd2)
│       └── analytics_marts/        <- CSV & Parquet: Consolidated 18-column analytical 360 mart
├── docs/
│   ├── DATA_DOCUMENTATION.pdf      <- Comprehensive executive PDF data documentation
│   └── TEST_DOCUMENTATION.md       <- Full DQ test plan, rules matrix, and architecture specifications
├── reports/
│   ├── dq_scorecard_report.html    <- Interactive executive DQ Scorecard & KPI dashboard
│   ├── dq_summary.json             <- Machine-readable JSON summary of DQ health scores
│   ├── dq_summary.md               <- Markdown scorecard summary
│   └── allure-results/             <- Allure test report artifacts
├── scripts/
│   ├── run_pipeline.py             <- Medallion pipeline & DQ scoring orchestration
│   └── generate_pdf_docs.py        <- Standalone PDF documentation generator
├── src/
│   ├── __init__.py
│   ├── dq_score.py                 <- DQ score calculator and HTML scorecard generator
│   ├── keyword_dq_engine.py        <- Core keyword-driven validation engine
│   ├── medallion_pipeline.py       <- Layer ETL, reconciliation, quarantine & SCD2 pipeline
│   └── pdf_generator.py            <- High-resolution PDF data documentation builder
├── tests/
│   ├── __init__.py
│   └── test_keyword_framework.py   <- Comprehensive pytest / unittest suite testing physical files
├── CASE_STUDY_SOLUTION.md          <- Complete Senior DQ Engineer Case Study Solution Document
├── EXPLANATION_GUIDE.md            <- Interview & stakeholder presentation guide
├── run_all.py                      <- Master execution script (Pipeline + Scores + PDF + Pytest)
├── run.sh                          <- Shell runner script
└── README.md
```

---

## 📊 Medallion Layer Storage Formats

| Layer | Format | Path | Purpose & Audit Metadata |
| :--- | :--- | :--- | :--- |
| **Source** | CSV | `data/source/` | Raw unmodified external feeds. |
| **Bronze** | **CSV** (with headers) | `data/bronze/<table_name>/` | Exact 1:1 ingestion copy with `_bronze_ingestion_ts` and `_bronze_source_file`. Kept as CSV for direct visual inspection in Excel/text editors. |
| **Silver Clean** | **CSV & Parquet** | `data/silver/clean/<table_name>/` | Cleansed, standardized, deduplicated records. Adds `_silver_processed_ts`, `_pipeline_version`, and `_is_valid=True`. |
| **Silver Quarantine** | **CSV & Parquet** | `data/silver/quarantine/<table_name>/` | Isolated anomaly rows blocked from analytics. Adds `dq_errors` diagnostic arrays and `_is_valid=False`. |
| **Gold SCD2** | **CSV & Parquet** | `data/gold/scd2_dimensions/dim_student_scd2/` | Historical version tracking with `effective_start_date`, `effective_end_date`, `is_current`, and `version`. |
| **Gold Wide Mart** | **CSV & Parquet** | `data/gold/analytics_marts/fact_student_enrollment_wide/` | Denormalized 18-column analytical mart consolidating all 4 source domains with business rule enforcement. |

---

## 🚀 Quick Start

### 1. Execute Master Pipeline, Tests & Generate All Reports (Recommended)
```bash
python run_all.py
# or
./run.sh
```
This single command runs:
1. **Source Data Profiling** (`PROFILE_DATA`)
2. **Bronze Ingestion & 4-Way Reconciliation** (Schema, Record Count, SHA-256 Checksum, Numeric Aggregations)
3. **Silver Cleansing & Quarantine Routing** (Completeness, Deduplication, Cross-Key Identity Deduplication, Referential Integrity, Format Regex, Chronology)
4. **Gold SCD Type 2 Dimension Processing** (`dim_student_scd2`)
5. **Gold Wide Analytical Mart Consolidation** (`fact_student_enrollment_wide`)
6. **Data Quality Scoring Engine** (Overall Health: **97.78% EXCELLENT**)
7. **Generates All Governance Artifacts:**
   - Interactive HTML Scorecard: `reports/dq_scorecard_report.html`
   - Data Documentation PDF: `docs/DATA_DOCUMENTATION.pdf`
   - Plain-English DQ Test Summary Matrix: `docs/dq_test_cases_summary.csv`
   - JSON & Markdown Summaries: `reports/dq_summary.json`, `reports/dq_summary.md`
8. **Runs 100% of Automated Test Suite with Allure Reports** (`reports/allure-results/`)

---

### 2. Run Pipeline Individually
```bash
python scripts/run_pipeline.py
```

### 3. Generate Data Documentation PDF Standalone
```bash
python scripts/generate_pdf_docs.py
```

### 4. Run Automated Test Suite
```bash
pytest tests/test_keyword_framework.py -v
```
# Mohesr
# Mohesr
