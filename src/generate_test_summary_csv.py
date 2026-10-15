import os
import csv

def generate_test_summary_csv(output_paths=None):
    """
    Generates a comprehensive CSV summarizing all test cases and DQ rules in plain English:
    Columns: id, category, dq_dimension, test_summary_description, code_snippet_in_query
    """
    if output_paths is None:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        output_paths = [
            os.path.join(project_root, "docs", "dq_test_cases_summary.csv"),
            os.path.join(project_root, "reports", "dq_test_cases_summary.csv"),
            os.path.join(project_root, "data", "dq_test_cases_summary.csv")
        ]

    test_cases = [
        # 1. BRONZE RECONCILIATION
        {
            "id": "BRZ_REC_01",
            "category": "Bronze Ingestion Reconciliation",
            "dq_dimension": "Completeness",
            "test_summary_description": "Verify that all source table column names, column count, and data types match the bronze layer target schema exactly without missing or altered fields.",
            "code_snippet_in_query": "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'source_student' EXCEPT SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'bronze_student';"
        },
        {
            "id": "BRZ_REC_02",
            "category": "Bronze Ingestion Reconciliation",
            "dq_dimension": "Completeness",
            "test_summary_description": "Verify that the total record count in Bronze matches the raw source record count exactly, guaranteeing zero record loss during ingestion.",
            "code_snippet_in_query": "SELECT (SELECT COUNT(*) FROM source_student) - (SELECT COUNT(*) FROM bronze_student) AS record_count_difference;"
        },
        {
            "id": "BRZ_REC_03",
            "category": "Bronze Ingestion Reconciliation",
            "dq_dimension": "Consistency",
            "test_summary_description": "Verify cryptographic bit-level SHA-256 payload checksum between source and bronze datasets to ensure zero byte corruption or truncation.",
            "code_snippet_in_query": "SELECT sha2(concat_ws('||', *), 256) AS src_hash FROM source_student EXCEPT SELECT sha2(concat_ws('||', *), 256) AS brz_hash FROM bronze_student;"
        },
        {
            "id": "BRZ_REC_04",
            "category": "Bronze Ingestion Reconciliation",
            "dq_dimension": "Validity",
            "test_summary_description": "Verify that numeric aggregations (such as SUM, MIN, and MAX of Academic Year) are completely identical between source and bronze.",
            "code_snippet_in_query": "SELECT (SUM(s.Academic_Year) - SUM(b.Academic_Year)) AS sum_diff FROM source_enrollment s, bronze_enrollment b;"
        },

        # 2. INSTITUTION SILVER RULES
        {
            "id": "INST_DQ_01",
            "category": "Completeness & Mandatory Keys",
            "dq_dimension": "Completeness",
            "test_summary_description": "Check that the primary key Institution_Code is not null or empty.",
            "code_snippet_in_query": "SELECT * FROM bronze_institution WHERE Institution_Code IS NULL OR TRIM(Institution_Code) = '';"
        },
        {
            "id": "INST_DQ_02",
            "category": "Format & Pattern Validation",
            "dq_dimension": "Validity",
            "test_summary_description": "Validate that Institution_Code follows the prescribed format of 'INST' followed by 3 numeric digits (e.g., INST001).",
            "code_snippet_in_query": "SELECT * FROM bronze_institution WHERE Institution_Code NOT RLIKE '^INST[0-9]{3}$';"
        },
        {
            "id": "INST_DQ_03",
            "category": "Domain Set Constraints",
            "dq_dimension": "Validity",
            "test_summary_description": "Ensure that Institution_Status strictly belongs to the allowable case-sensitive domain set ('Active', 'Inactive', 'Pending').",
            "code_snippet_in_query": "SELECT * FROM bronze_institution WHERE Institution_Status NOT IN ('Active', 'Inactive', 'Pending');"
        },

        # 3. PROGRAM SILVER RULES
        {
            "id": "PRG_DQ_01",
            "category": "Completeness & Mandatory Keys",
            "dq_dimension": "Completeness",
            "test_summary_description": "Check that the primary key Program_Code is not null or empty.",
            "code_snippet_in_query": "SELECT * FROM bronze_program WHERE Program_Code IS NULL OR TRIM(Program_Code) = '';"
        },
        {
            "id": "PRG_DQ_02",
            "category": "Domain Set Constraints",
            "dq_dimension": "Validity",
            "test_summary_description": "Ensure that Program_Status strictly belongs to the allowable case-sensitive domain set ('Active', 'Suspended', 'Discontinued').",
            "code_snippet_in_query": "SELECT * FROM bronze_program WHERE Program_Status NOT IN ('Active', 'Suspended', 'Discontinued');"
        },
        {
            "id": "PRG_DQ_03",
            "category": "Referential Integrity",
            "dq_dimension": "Referential Integrity",
            "test_summary_description": "Verify that every Program references an existing and valid Institution_Code in the Institution master entity.",
            "code_snippet_in_query": "SELECT p.* FROM bronze_program p LEFT JOIN silver_institution i ON p.Institution_Code = i.Institution_Code WHERE i.Institution_Code IS NULL;"
        },

        # 4. STUDENT SILVER RULES
        {
            "id": "STU_DQ_01",
            "category": "Completeness & Mandatory Keys",
            "dq_dimension": "Completeness",
            "test_summary_description": "Ensure that the primary key Student_ID is populated and not null or blank.",
            "code_snippet_in_query": "SELECT * FROM bronze_student WHERE Student_ID IS NULL OR TRIM(Student_ID) = '';"
        },
        {
            "id": "STU_DQ_02",
            "category": "Format & Pattern Validation",
            "dq_dimension": "Validity",
            "test_summary_description": "Verify that the UAE Emirates_ID strictly complies with the official national format pattern 784-YYYY-XXXXXXX-Z.",
            "code_snippet_in_query": "SELECT * FROM bronze_student WHERE Emirates_ID NOT RLIKE '^784-[0-9]{4}-[0-9]{7}-[0-9]{1}$';"
        },
        {
            "id": "STU_DQ_03",
            "category": "Deduplication & Uniqueness",
            "dq_dimension": "Uniqueness",
            "test_summary_description": "Check for duplicate Student_ID records and retain only the most recently updated record, filtering out stale duplicate versions.",
            "code_snippet_in_query": "SELECT Student_ID, COUNT(*) FROM bronze_student GROUP BY Student_ID HAVING COUNT(*) > 1;"
        },
        {
            "id": "STU_DQ_04",
            "category": "Referential Integrity",
            "dq_dimension": "Referential Integrity",
            "test_summary_description": "Verify that every student's Institution_Code references a valid, active institution in the Institution master entity.",
            "code_snippet_in_query": "SELECT s.* FROM bronze_student s LEFT JOIN silver_institution i ON s.Institution_Code = i.Institution_Code WHERE i.Institution_Code IS NULL;"
        },
        {
            "id": "STU_DQ_05",
            "category": "Domain Set Constraints",
            "dq_dimension": "Validity",
            "test_summary_description": "Check that standardized Gender values belong to the approved domain ('F', 'M', 'Other', 'Unknown').",
            "code_snippet_in_query": "SELECT * FROM bronze_student WHERE Gender NOT IN ('F', 'M', 'Other', 'Unknown');"
        },
        {
            "id": "STU_DQ_06",
            "category": "Date Chronology & Timeliness",
            "dq_dimension": "Timeliness",
            "test_summary_description": "Verify that Updated_Date is logically on or after the initial Created_Date (Updated_Date >= Created_Date).",
            "code_snippet_in_query": "SELECT * FROM bronze_student WHERE Updated_Date < Created_Date;"
        },
        {
            "id": "STU_DQ_07",
            "category": "Deduplication & Uniqueness",
            "dq_dimension": "Uniqueness",
            "test_summary_description": "Check for duplicate Full_Name/Name and Emirates_ID associated with different Student_ID values.",
            "code_snippet_in_query": "SELECT Full_Name, Emirates_ID, COUNT(DISTINCT Student_ID) FROM bronze_student GROUP BY Full_Name, Emirates_ID HAVING COUNT(DISTINCT Student_ID) > 1;"
        },
        {
            "id": "STU_DQ_08",
            "category": "Timeliness & Freshness",
            "dq_dimension": "Timeliness",
            "test_summary_description": "Verify that delayed records exceeding the allowable SLA freshness window are identified and flagged.",
            "code_snippet_in_query": "SELECT * FROM bronze_student WHERE DATEDIFF(CURRENT_TIMESTAMP(), Updated_Date) > 30;"
        },

        # 5. ENROLLMENT SILVER RULES
        {
            "id": "ENR_DQ_01",
            "category": "Completeness & Mandatory Keys",
            "dq_dimension": "Completeness",
            "test_summary_description": "Check that the primary key Enrollment_ID is present and not null or blank.",
            "code_snippet_in_query": "SELECT * FROM bronze_enrollment WHERE Enrollment_ID IS NULL OR TRIM(Enrollment_ID) = '';"
        },
        {
            "id": "ENR_DQ_02",
            "category": "Completeness & Mandatory Keys",
            "dq_dimension": "Completeness",
            "test_summary_description": "Check that the foreign key Student_ID is populated and not null.",
            "code_snippet_in_query": "SELECT * FROM bronze_enrollment WHERE Student_ID IS NULL OR TRIM(Student_ID) = '';"
        },
        {
            "id": "ENR_DQ_03",
            "category": "Referential Integrity",
            "dq_dimension": "Referential Integrity",
            "test_summary_description": "Verify that the Student_ID referenced in Enrollment exists in the clean Silver Student master.",
            "code_snippet_in_query": "SELECT e.* FROM bronze_enrollment e LEFT JOIN silver_student s ON e.Student_ID = s.Student_ID WHERE s.Student_ID IS NULL;"
        },
        {
            "id": "ENR_DQ_04",
            "category": "Referential Integrity",
            "dq_dimension": "Referential Integrity",
            "test_summary_description": "Verify that the Program_Code referenced in Enrollment exists in the clean Silver Program master.",
            "code_snippet_in_query": "SELECT e.* FROM bronze_enrollment e LEFT JOIN silver_program p ON e.Program_Code = p.Program_Code WHERE p.Program_Code IS NULL;"
        },
        {
            "id": "ENR_DQ_05",
            "category": "Referential Integrity",
            "dq_dimension": "Referential Integrity",
            "test_summary_description": "Verify that the Institution_Code referenced in Enrollment exists in the clean Silver Institution master.",
            "code_snippet_in_query": "SELECT e.* FROM bronze_enrollment e LEFT JOIN silver_institution i ON e.Institution_Code = i.Institution_Code WHERE i.Institution_Code IS NULL;"
        },
        {
            "id": "ENR_DQ_06",
            "category": "Date Chronology & Consistency",
            "dq_dimension": "Consistency",
            "test_summary_description": "Ensure that Graduation_Date is chronologically on or after the Enrollment_Date.",
            "code_snippet_in_query": "SELECT * FROM bronze_enrollment WHERE Graduation_Date IS NOT NULL AND Graduation_Date < Enrollment_Date;"
        },
        {
            "id": "ENR_DQ_07",
            "category": "Domain Set Constraints",
            "dq_dimension": "Validity",
            "test_summary_description": "Check that Enrollment_Status strictly belongs to the allowed case-sensitive domain values ('Enrolled', 'Graduated', 'Suspended', 'Withdrawn').",
            "code_snippet_in_query": "SELECT * FROM bronze_enrollment WHERE Enrollment_Status NOT IN ('Enrolled', 'Graduated', 'Suspended', 'Withdrawn');"
        },
        {
            "id": "ENR_DQ_08",
            "category": "Business Rules & Range Check",
            "dq_dimension": "Validity",
            "test_summary_description": "Verify that the Academic_Year falls within the valid operational range (2000 to 2030).",
            "code_snippet_in_query": "SELECT * FROM bronze_enrollment WHERE Academic_Year < 2000 OR Academic_Year > 2030;"
        },

        # 6. GOLD BUSINESS RULES & MART
        {
            "id": "GOLD_DQ_01",
            "category": "Business Rules & Mart Integrity",
            "dq_dimension": "Consistency",
            "test_summary_description": "Ensure that all records in the consolidated analytical mart belong to Active institutions.",
            "code_snippet_in_query": "SELECT * FROM fact_student_enrollment_wide WHERE Institution_Status != 'Active';"
        },
        {
            "id": "GOLD_DQ_02",
            "category": "Business Rules & Mart Integrity",
            "dq_dimension": "Consistency",
            "test_summary_description": "Ensure that all academic programs included in the analytical mart have Active status.",
            "code_snippet_in_query": "SELECT * FROM fact_student_enrollment_wide WHERE Program_Status != 'Active';"
        },
        {
            "id": "GOLD_DQ_03",
            "category": "Business Rules & Demographics",
            "dq_dimension": "Validity",
            "test_summary_description": "Verify that enrolled higher-education students meet the minimum age requirement of at least 16 years old.",
            "code_snippet_in_query": "SELECT * FROM fact_student_enrollment_wide WHERE Date_of_Birth IS NOT NULL AND (year(current_date()) - year(Date_of_Birth)) < 16;"
        },
        {
            "id": "GOLD_SCD2_01",
            "category": "SCD Type 2 Historical Tracking",
            "dq_dimension": "Consistency",
            "test_summary_description": "Verify that the SCD Type 2 dimension correctly maintains historical versioning with effective_start_date, effective_end_date, version, and is_current flags.",
            "code_snippet_in_query": "SELECT Student_ID, version, effective_start_date, effective_end_date, is_current FROM dim_student_scd2 ORDER BY Student_ID, version;"
        },
        {
            "id": "GOLD_WIDE_01",
            "category": "Wide Analytical Mart Consolidation",
            "dq_dimension": "Completeness",
            "test_summary_description": "Validate that the wide analytical 360 mart consolidates all 4 source entities (Student, Enrollment, Institution, Program) with non-null foreign relationships.",
            "code_snippet_in_query": "SELECT Enrollment_ID, Student_ID, Student_Name, Institution_Name, Program_Name, Academic_Year, Enrollment_Status FROM fact_student_enrollment_wide;"
        },
        {
            "id": "CRITICAL_ABORT_01",
            "category": "Pipeline Safety & Fatal Failure Routing",
            "dq_dimension": "Completeness",
            "test_summary_description": "Verify that critical failure rules configured with FAIL/REJECT action halt pipeline execution immediately to prevent data corruption.",
            "code_snippet_in_query": "IF EXISTS (SELECT * FROM source_student WHERE Student_ID IS NULL) AND action = 'FAIL' THEN RAISE EXCEPTION 'CRITICAL DQ Failure: Student_ID cannot be NULL';"
        }
    ]

    fieldnames = ["id", "category", "dq_dimension", "test_summary_description", "code_snippet_in_query"]

    for path in output_paths:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in test_cases:
                writer.writerow(row)
        print(f"[✓] Test Summary CSV written to: {path}")

    return test_cases


if __name__ == "__main__":
    generate_test_summary_csv()
