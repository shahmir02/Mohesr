"""
Keyword-Driven Data Quality Rule Configurations for Medallion Architecture.
Defines rules for Bronze, Silver, and Gold layers across Student, Enrollment, Institution, and Program datasets.

Dimensions Covered:
- Completeness
- Validity
- Uniqueness
- Consistency
- Referential Integrity
- Timeliness

Severity & Action Hierarchy:
- CRITICAL / FAIL or REJECT: Aborts execution or rejects batch when fatal structural integrity fails.
- HIGH / QUARANTINE: Isolates invalid records to quarantine layer with detailed error tags without aborting pipeline.
- MEDIUM / QUARANTINE: Isolates domain/business anomalies for data steward remediation.
- LOW / WARN: Logs informational warning, allows record to pass through.
"""

# ==============================================================================
# 1. BRONZE RECONCILIATION RULES (Bronze Layer)
# ==============================================================================
BRONZE_RECONCILIATION_RULES = [
    {
        "rule_id": "BRZ_REC_01",
        "rule_name": "Source to Bronze Schema Exact Match",
        "dimension": "Completeness",
        "layer": "Bronze",
        "keyword": "RECONCILE_SCHEMA",
        "severity": "CRITICAL",
        "action": "REJECT",
        "params": {},
        "description": "Validates source column names and datatypes match bronze schema definition exactly."
    },
    {
        "rule_id": "BRZ_REC_02",
        "rule_name": "Source to Bronze Record Count Reconciliation",
        "dimension": "Completeness",
        "layer": "Bronze",
        "keyword": "RECONCILE_RECORD_COUNT",
        "severity": "CRITICAL",
        "action": "REJECT",
        "params": {},
        "description": "Verifies zero record loss between source and bronze ingestion."
    },
    {
        "rule_id": "BRZ_REC_03",
        "rule_name": "Source to Bronze Checksum Hash Verification",
        "dimension": "Consistency",
        "layer": "Bronze",
        "keyword": "RECONCILE_CHECKSUM",
        "severity": "CRITICAL",
        "action": "REJECT",
        "params": {},
        "description": "Calculates row-level cryptographic payload checksum to guarantee zero byte corruption."
    },
    {
        "rule_id": "BRZ_REC_04",
        "rule_name": "Source to Bronze Numeric Metric Reconciliation",
        "dimension": "Validity",
        "layer": "Bronze",
        "keyword": "RECONCILE_NUMERIC_AGG",
        "severity": "HIGH",
        "action": "REJECT",
        "params": {},
        "description": "Validates sum, min, max numeric aggregations match between source and bronze."
    }
]

# ==============================================================================
# 2. INSTITUTION RULES (Silver Layer)
# ==============================================================================
INSTITUTION_RULES = [
    {
        "rule_id": "INST_DQ_01",
        "rule_name": "Institution Code Mandatory",
        "dimension": "Completeness",
        "layer": "Silver",
        "keyword": "NOT_NULL",
        "severity": "CRITICAL",
        "action": "QUARANTINE",
        "params": {"column": "Institution_Code"},
        "description": "Primary key Institution_Code must not be null or empty."
    },
    {
        "rule_id": "INST_DQ_02",
        "rule_name": "Institution Code Format Validation",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "REGEX_MATCH",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {"column": "Institution_Code", "pattern": r"^INST\d{3}$"},
        "description": "Institution_Code must match format 'INST' followed by 3 digits."
    },
    {
        "rule_id": "INST_DQ_03",
        "rule_name": "Institution Status Allowed Domain",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "VALUE_IN_SET",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {"column": "Institution_Status", "allowed_values": ["Active", "Inactive", "Pending"], "case_sensitive": True},
        "description": "Institution_Status must be Active, Inactive, or Pending (case-sensitive)."
    }
]

# ==============================================================================
# 3. PROGRAM RULES (Silver Layer)
# ==============================================================================
PROGRAM_RULES = [
    {
        "rule_id": "PRG_DQ_01",
        "rule_name": "Program Code Mandatory",
        "dimension": "Completeness",
        "layer": "Silver",
        "keyword": "NOT_NULL",
        "severity": "CRITICAL",
        "action": "QUARANTINE",
        "params": {"column": "Program_Code"},
        "description": "Primary key Program_Code must not be null or empty."
    },
    {
        "rule_id": "PRG_DQ_02",
        "rule_name": "Program Status Allowed Domain",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "VALUE_IN_SET",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {"column": "Program_Status", "allowed_values": ["Active", "Suspended", "Discontinued"], "case_sensitive": True},
        "description": "Program_Status must be Active, Suspended, or Discontinued (case-sensitive)."
    },
    {
        "rule_id": "PRG_DQ_03",
        "rule_name": "Program to Institution Referential Integrity",
        "dimension": "Referential Integrity",
        "layer": "Silver",
        "keyword": "REFERENTIAL_INTEGRITY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "fk_column": "Institution_Code",
            "parent_table": "institution",
            "parent_pk_column": "Institution_Code"
        },
        "description": "Institution_Code in Program must exist in clean Silver Institution master."
    }
]

# ==============================================================================
# 4. STUDENT RULES (Silver Layer)
# ==============================================================================
STUDENT_RULES = [
    {
        "rule_id": "STU_DQ_01",
        "rule_name": "Student ID Mandatory",
        "dimension": "Completeness",
        "layer": "Silver",
        "keyword": "NOT_NULL",
        "severity": "CRITICAL",
        "action": "QUARANTINE",
        "params": {"column": "Student_ID"},
        "description": "Primary key Student_ID cannot be null or empty."
    },
    {
        "rule_id": "STU_DQ_02",
        "rule_name": "Student Emirates ID Format Validation",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "REGEX_MATCH",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {"column": "Emirates_ID", "pattern": r"^784-\d{4}-\d{7}-\d{1}$"},
        "description": "Emirates ID must strictly follow UAE standard format: 784-YYYY-XXXXXXX-Z."
    },
    {
        "rule_id": "STU_DQ_03",
        "rule_name": "Student Natural Key Deduplication",
        "dimension": "Uniqueness",
        "layer": "Silver",
        "keyword": "DEDUPLICATE",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "key_columns": ["Student_ID"],
            "order_by_column": "Updated_Date",
            "descending": True
        },
        "description": "Identifies duplicate student records and quarantines stale/superseded versions."
    },
    {
        "rule_id": "STU_DQ_04",
        "rule_name": "Student to Institution Referential Integrity",
        "dimension": "Referential Integrity",
        "layer": "Silver",
        "keyword": "REFERENTIAL_INTEGRITY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "fk_column": "Institution_Code",
            "parent_table": "institution",
            "parent_pk_column": "Institution_Code"
        },
        "description": "Student Institution_Code must exist in clean Silver Institution master."
    },
    {
        "rule_id": "STU_DQ_05",
        "rule_name": "Student Gender Allowed Domain",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "VALUE_IN_SET",
        "severity": "MEDIUM",
        "action": "QUARANTINE",
        "params": {"column": "Gender", "allowed_values": ["F", "M", "Other", "Unknown"]},
        "description": "Standardized Gender must belong to allowed reference set (F, M, Other, Unknown)."
    },
    {
        "rule_id": "STU_DQ_06",
        "rule_name": "Student Timeliness Chronology",
        "dimension": "Timeliness",
        "layer": "Silver",
        "keyword": "DATE_CHRONOLOGY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "start_date_column": "Created_Date",
            "end_date_column": "Updated_Date"
        },
        "description": "Updated_Date cannot be prior to Created_Date (Updated_Date >= Created_Date)."
    },
    {
        "rule_id": "STU_DQ_07",
        "rule_name": "Student Identity Cross-Key Deduplication",
        "dimension": "Uniqueness",
        "layer": "Silver",
        "keyword": "CROSS_KEY_DUPLICATE",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "key_columns": ["Full_Name", "Emirates_ID"],
            "distinct_column": "Student_ID"
        },
        "description": "Check for duplicate Name/Full_Name and Emirates_ID associated with different Student_ID values."
    }
]

# ==============================================================================
# 5. ENROLLMENT RULES (Silver Layer)
# ==============================================================================
ENROLLMENT_RULES = [
    {
        "rule_id": "ENR_DQ_01",
        "rule_name": "Enrollment ID Mandatory",
        "dimension": "Completeness",
        "layer": "Silver",
        "keyword": "NOT_NULL",
        "severity": "CRITICAL",
        "action": "QUARANTINE",
        "params": {"column": "Enrollment_ID"},
        "description": "Primary key Enrollment_ID cannot be null or empty."
    },
    {
        "rule_id": "ENR_DQ_02",
        "rule_name": "Enrollment Student ID Mandatory",
        "dimension": "Completeness",
        "layer": "Silver",
        "keyword": "NOT_NULL",
        "severity": "CRITICAL",
        "action": "QUARANTINE",
        "params": {"column": "Student_ID"},
        "description": "Foreign key Student_ID must be present."
    },
    {
        "rule_id": "ENR_DQ_03",
        "rule_name": "Enrollment to Student Referential Integrity",
        "dimension": "Referential Integrity",
        "layer": "Silver",
        "keyword": "REFERENTIAL_INTEGRITY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "fk_column": "Student_ID",
            "parent_table": "student",
            "parent_pk_column": "Student_ID"
        },
        "description": "Student_ID must exist in clean Silver Student master."
    },
    {
        "rule_id": "ENR_DQ_04",
        "rule_name": "Enrollment to Program Referential Integrity",
        "dimension": "Referential Integrity",
        "layer": "Silver",
        "keyword": "REFERENTIAL_INTEGRITY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "fk_column": "Program_Code",
            "parent_table": "program",
            "parent_pk_column": "Program_Code"
        },
        "description": "Program_Code must exist in clean Silver Program master."
    },
    {
        "rule_id": "ENR_DQ_05",
        "rule_name": "Enrollment to Institution Referential Integrity",
        "dimension": "Referential Integrity",
        "layer": "Silver",
        "keyword": "REFERENTIAL_INTEGRITY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "fk_column": "Institution_Code",
            "parent_table": "institution",
            "parent_pk_column": "Institution_Code"
        },
        "description": "Institution_Code must exist in clean Silver Institution master."
    },
    {
        "rule_id": "ENR_DQ_06",
        "rule_name": "Enrollment vs Graduation Date Chronology",
        "dimension": "Consistency",
        "layer": "Silver",
        "keyword": "DATE_CHRONOLOGY",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "start_date_column": "Enrollment_Date",
            "end_date_column": "Graduation_Date"
        },
        "description": "Graduation_Date must be on or after Enrollment_Date."
    },
    {
        "rule_id": "ENR_DQ_07",
        "rule_name": "Enrollment Status Allowed Domain",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "VALUE_IN_SET",
        "severity": "MEDIUM",
        "action": "QUARANTINE",
        "params": {"column": "Enrollment_Status", "allowed_values": ["Enrolled", "Graduated", "Suspended", "Withdrawn"], "case_sensitive": True},
        "description": "Enrollment_Status must be Enrolled, Graduated, Suspended, or Withdrawn (case-sensitive)."
    },
    {
        "rule_id": "ENR_DQ_08",
        "rule_name": "Academic Year Logical Range",
        "dimension": "Validity",
        "layer": "Silver",
        "keyword": "BUSINESS_RULE",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "valid_sql_condition": "Academic_Year >= 2000 AND Academic_Year <= 2030",
            "description": "Academic_Year must be between 2000 and 2030"
        },
        "description": "Academic_Year must fall within valid operational window (2000-2030)."
    }
]

# ==============================================================================
# 6. GOLD BUSINESS RULES (Gold Layer - Analytical Mart)
# ==============================================================================
GOLD_BUSINESS_RULES = [
    {
        "rule_id": "GOLD_DQ_01",
        "rule_name": "Active Institution Validation",
        "dimension": "Consistency",
        "layer": "Gold",
        "keyword": "BUSINESS_RULE",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "valid_sql_condition": "Institution_Status = 'Active'",
            "description": "Active enrollments must belong to an Active institution"
        },
        "description": "Institutions associated with current analytical enrollments must be Active."
    },
    {
        "rule_id": "GOLD_DQ_02",
        "rule_name": "Active Program Validation",
        "dimension": "Consistency",
        "layer": "Gold",
        "keyword": "BUSINESS_RULE",
        "severity": "HIGH",
        "action": "QUARANTINE",
        "params": {
            "valid_sql_condition": "Program_Status = 'Active'",
            "description": "Programs must have Active status in analytical mart"
        },
        "description": "Programs in the analytical mart must be Active."
    },
    {
        "rule_id": "GOLD_DQ_03",
        "rule_name": "Student Minimum Age Requirement",
        "dimension": "Validity",
        "layer": "Gold",
        "keyword": "BUSINESS_RULE",
        "severity": "MEDIUM",
        "action": "QUARANTINE",
        "params": {
            "valid_sql_condition": "Date_of_Birth IS NULL OR (year(current_date()) - year(Date_of_Birth)) >= 16",
            "description": "Student must be at least 16 years of age"
        },
        "description": "Enrolled higher-education students must be at least 16 years old."
    }
]
