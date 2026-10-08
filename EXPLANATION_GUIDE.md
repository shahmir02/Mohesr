# Presentation & Interview Explanation Guide

This guide gives you an intuitive, step-by-step way to explain the architecture and codebase to technical interviewers and stakeholders in simple terms.

---

## 1. Executive Summary (The 60-Second Pitch)
> *"I designed a **Keyword-Driven Data Quality Framework** implemented in **PySpark** on a **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**.*
>
> *Instead of hardcoding checks into ETL scripts, all quality checks are declared as configuration rules with explicit **DQ dimensions** (Completeness, Validity, Uniqueness, Consistency, Referential Integrity, Timeliness) and explicit **actions** (`QUARANTINE` for bad rows vs `REJECT` for fatal errors).*
>
> *The framework profiles raw data, runs a 4-way Source-to-Bronze reconciliation, isolates anomalies in Silver Quarantine tables with error tags, constructs Gold SCD Type 2 dimensions and wide analytical marts, and automatically calculates **DQ health scores** with interactive HTML and Allure reporting."*

---

## 2. Architecture in 3 Simple Layers

```
Source Data (CSV)
       │
       ▼
[ BRONZE LAYER ] ──► Exact raw ingestion + 4-Way Reconciliation (Schema, Count, Checksum, Numeric)
       │
       ▼
[ SILVER LAYER ] ──► Standardization, Deduplication & Keyword Rule Enforcement
       ├──────────────► Clean Silver Tables (Passed records + audit metadata)
       └──────────────► Quarantine Tables (Isolated records + `dq_errors` tags)
       │
       ▼
[  GOLD LAYER  ] ──► SCD Type 2 History (`dim_student_scd2`) + Wide Analytics Mart (`fact_student_enrollment_wide`)
```

---

## 3. How the Keyword Engine Works (Simple Explanation)

Rules are declared in Python dictionaries (`config/rules_config.py`):
```python
{
    "rule_id": "STU_DQ_02",
    "rule_name": "Student Emirates ID Format Validation",
    "dimension": "Validity",
    "layer": "Silver",
    "keyword": "REGEX_MATCH",
    "severity": "HIGH",
    "action": "QUARANTINE",
    "params": {"column": "Emirates_ID", "pattern": r"^784-\d{4}-\d{7}-\d{1}$"}
}
```

The Engine (`KeywordDQEngine`) interprets the keyword and executes PySpark column expressions:
1. **`NOT_NULL`**: Checks for null, empty strings, and `"null"` literals.
2. **`DEDUPLICATE`**: Windows over natural keys ordered by timestamp to isolate stale duplicates.
3. **`REFERENTIAL_INTEGRITY`**: Validates foreign keys against parent dimension tables.
4. **`REGEX_MATCH`**: Tests formatting rules using PySpark regex matching (`rlike`).
5. **`VALUE_IN_SET`**: Checks if categorical values belong to allowed domain reference sets.
6. **`DATE_CHRONOLOGY`**: Ensures end dates (e.g. Graduation) do not precede start dates (Enrollment).
7. **`BUSINESS_RULE`**: Executes complex cross-column SQL boolean expressions.

---

## 4. How Quarantine vs Reject Works

- **`QUARANTINE` (Non-blocking row-level isolation)**:
  - If a row fails a rule, it is **not deleted or dropped silently**.
  - It is tagged with error metadata (`dq_errors: ["STU_DQ_02|Validity|Silver|HIGH|QUARANTINE|REGEX_MATCH: ..."]`) and routed to `data/silver/quarantine/`.
  - The pipeline **continues processing** healthy rows to clean Silver and Gold.
- **`REJECT / FAIL` (Fatal pipeline abort)**:
  - Used for fatal structural integrity failures (such as Bronze checksum corruption or critical schema mismatches).
  - Halts the pipeline and raises a `DataQualityException`.

---

## 5. How the Data Quality Score is Calculated

The Data Quality Health Score is calculated across **35 automated checks**:

$$\text{Rule Pass Rate} = \frac{\text{Passed Records}}{\text{Total Records}} \times 100\%$$

$$\text{Dimension Score} = \frac{1}{K} \sum_{i=1}^{K} \text{Rule Pass Rate}_i$$

$$\text{Overall DQ Score} = \frac{1}{N} \sum_{j=1}^{N} \text{All Rule Pass Rates}_j$$

### Live Scorecard Summary:
- **Overall DQ Health Score**: **97.71% (EXCELLENT)**
- **Completeness Score**: **99.12%**
- **Consistency Score**: **99.21%**
- **Validity Score**: **97.14%**
- **Referential Integrity Score**: **94.31%**
- **Uniqueness Score**: **88.24%**
- **Timeliness Score**: **100.00%**
- **Bronze Layer**: **100.00%** | **Silver Layer**: **96.00%** | **Gold Layer**: **100.00%**

---

## 6. How to Run & Showcase the Outputs

### One-Click Execution Command:
```bash
./run.sh
# or:
python run_all.py
```

### Generated Artifacts to Show Stakeholders:
1. **Interactive HTML Scorecard Report**: Open `reports/dq_scorecard_report.html` in any web browser to showcase dimension progress bars, pass/quarantine KPI cards, and detailed rule breakdowns.
2. **Allure Test Results**: Generated in `reports/allure-results/`.
3. **Structured JSON Summary**: `reports/dq_summary.json`.
4. **Clean & Quarantined Data**: Inspect `data/silver/clean/` vs `data/silver/quarantine/`.
