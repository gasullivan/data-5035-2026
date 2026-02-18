-- ============================================================================
-- FILE: exercise06.sql
-- ASSIGNMENT: Data Engineering - Assignment 06 - Testing
-- AUTHOR: Greg Sullivan
-- DATE: February 22, 2026
-- COURSE: DATA 5035 - Data Engineering
-- PROFESSOR: Paul Boal
-- ============================================================================
-- 
-- DESCRIPTION:
-- Unit tests for data quality checks originally developed in Exercise 02.
-- This test suite validates 5 DQ checks with 4 test cases each (20 total
-- per CSV row). With 3 rows in the CSV, we run 60 tests total. In the CSV
-- rows are my test input values so I'm not hardcoding any values in the
-- Notebook. In this way, one could add as many test values into the rows
-- as desired in order to run a variety of tests. I can even imagine a
-- program that creates the test values in some automated manner.
--
-- ARCHITECTURE:
-- Test data is externalized to a CSV file (exercise06_test_cases.csv) rather
-- than hardcoded in the SQL. Each row in the CSV represents a complete set
-- of 20 test cases. Multiple rows allow testing different scenarios without
-- code changes. The stored procedure iterates through all rows, running all
-- 20 checks per row and reporting pass/fail results.
--
-- DQ CHECKS TESTED (from Exercise 02):
--   1. dq_reversed_name - Detects "Last, First" name format (contains comma)
--   2. dq_invalid_phone_format - Validates 10 digits, or 11 starting with 1
--   3. dq_incomplete_zip - Validates exactly 5 digits or 9 digits (ZIP+4)
--   4. dq_placeholder_category - Flags "Unknown" or "N/A" values
--   5. dq_outlier_amount - Flags donations over $1,000,000
--
-- CSV STRUCTURE (exercise06_test_cases.csv):
--   40 columns total (20 input values + 20 expected results)
--   Each row = one complete test run of all 20 checks
--   Columns: name_1, name_1_exp, name_2, name_2_exp, ... amt_4, amt_4_exp
--
-- TEST RESULTS INTERPRETATION:
--   PASS = DQ check logic worked correctly (actual matched expected)
--   FAIL = DQ check logic has a bug (actual did not match expected)
--
-- ============================================================================

-- Set context
USE DATABASE SNOWBEARAIR_DB;
USE SCHEMA GSULLIVAN;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

-- ============================================================================
-- STEP 1: Create file format and stage for CSV import
-- ============================================================================
-- Using IF NOT EXISTS to preserve uploaded CSV file on subsequent runs

CREATE FILE FORMAT IF NOT EXISTS dq_test_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '');

CREATE STAGE IF NOT EXISTS dq_test_stage
    FILE_FORMAT = dq_test_csv_format;

-- ============================================================================
-- STEP 2: Create table to hold test cases
-- ============================================================================
-- Using IF NOT EXISTS to preserve structure

CREATE TABLE IF NOT EXISTS DQ_TEST_CASES (
    test_row_id INT AUTOINCREMENT,
    -- Name tests (4 tests)
    name_1 VARCHAR, name_1_exp INT,
    name_2 VARCHAR, name_2_exp INT,
    name_3 VARCHAR, name_3_exp INT,
    name_4 VARCHAR, name_4_exp INT,
    -- Phone tests (4 tests)
    phone_1 VARCHAR, phone_1_exp INT,
    phone_2 VARCHAR, phone_2_exp INT,
    phone_3 VARCHAR, phone_3_exp INT,
    phone_4 VARCHAR, phone_4_exp INT,
    -- ZIP tests (4 tests)
    zip_1 VARCHAR, zip_1_exp INT,
    zip_2 VARCHAR, zip_2_exp INT,
    zip_3 VARCHAR, zip_3_exp INT,
    zip_4 VARCHAR, zip_4_exp INT,
    -- Category tests (4 tests)
    cat_1 VARCHAR, cat_1_exp INT,
    cat_2 VARCHAR, cat_2_exp INT,
    cat_3 VARCHAR, cat_3_exp INT,
    cat_4 VARCHAR, cat_4_exp INT,
    -- Amount tests (4 tests)
    amt_1 NUMBER, amt_1_exp INT,
    amt_2 NUMBER, amt_2_exp INT,
    amt_3 NUMBER, amt_3_exp INT,
    amt_4 NUMBER, amt_4_exp INT
);

-- Clear table before loading (allows re-running)
TRUNCATE TABLE IF EXISTS DQ_TEST_CASES;

-- ============================================================================
-- STEP 3: Load test cases from CSV
-- ============================================================================
-- Test data is externalized to exercise06_test_cases.csv
-- Each row in the CSV represents a complete set of 20 test cases
-- Multiple rows allow testing different scenarios without code changes
--
-- PREREQUISITE: Upload the CSV file to the stage first (one time only)
--   1. In Snowflake UI: Data > Databases > SNOWBEARAIR_DB > GSULLIVAN > Stages
--   2. Click on DQ_TEST_STAGE
--   3. Click "+ Files" and upload exercise06_test_cases.csv
-- ============================================================================

COPY INTO DQ_TEST_CASES (
    name_1, name_1_exp, name_2, name_2_exp, name_3, name_3_exp, name_4, name_4_exp,
    phone_1, phone_1_exp, phone_2, phone_2_exp, phone_3, phone_3_exp, phone_4, phone_4_exp,
    zip_1, zip_1_exp, zip_2, zip_2_exp, zip_3, zip_3_exp, zip_4, zip_4_exp,
    cat_1, cat_1_exp, cat_2, cat_2_exp, cat_3, cat_3_exp, cat_4, cat_4_exp,
    amt_1, amt_1_exp, amt_2, amt_2_exp, amt_3, amt_3_exp, amt_4, amt_4_exp
)
FROM @dq_test_stage/exercise06_test_cases.csv
FORCE = TRUE;

-- ============================================================================
-- STEP 4: Create stored procedure for running DQ tests
-- ============================================================================
-- Returns a TABLE with all test results for immediate display
-- Also saves results to OUTPUT_TABLE for additional queries

CREATE OR REPLACE PROCEDURE RUN_DQ_TESTS(
    TEST_TABLE VARCHAR,
    OUTPUT_TABLE VARCHAR
)
RETURNS TABLE (
    TEST_ROW_ID INT,
    DQ_CHECK VARCHAR,
    TEST_ID VARCHAR,
    INPUT_VALUE VARCHAR,
    EXPECTED INT,
    ACTUAL INT,
    RESULT VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
def run(session, test_table: str, output_table: str):
    """
    Run all 20 DQ tests against each row in the test table.
    Returns a Snowpark DataFrame with all test results.
    
    DQ Check Logic:
    1. dq_reversed_name: Flag if name contains comma
    2. dq_invalid_phone_format: Valid = 10 digits OR 11 digits starting with 1
    3. dq_incomplete_zip: Valid = exactly 5 digits OR exactly 9 digits
    4. dq_placeholder_category: Flag if category is "Unknown" or "N/A"
    5. dq_outlier_amount: Flag if amount > 1,000,000
    """
    
    # Read test cases
    test_df = session.table(test_table)
    
    results = []
    
    # Process each row from the CSV
    for row in test_df.collect():
        row_id = row["TEST_ROW_ID"]
        
        # ================================================================
        # DQ CHECK 1: dq_reversed_name (4 tests per row)
        # Logic: Flag if name contains comma
        # ================================================================
        for i in range(1, 5):
            name_val = row[f"NAME_{i}"]
            expected = row[f"NAME_{i}_EXP"]
            actual = 1 if name_val and "," in str(name_val) else 0
            match = actual == expected
            results.append({
                "TEST_ROW_ID": row_id,
                "DQ_CHECK": "dq_reversed_name",
                "TEST_ID": f"name_{i}",
                "INPUT_VALUE": str(name_val) if name_val else "NULL",
                "EXPECTED": expected,
                "ACTUAL": actual,
                "RESULT": "PASS" if match else "** FAIL **"
            })
        
        # ================================================================
        # DQ CHECK 2: dq_invalid_phone_format (4 tests per row)
        # Logic: Valid = exactly 10 digits OR 11 digits starting with 1
        # ================================================================
        for i in range(1, 5):
            phone_val = row[f"PHONE_{i}"]
            expected = row[f"PHONE_{i}_EXP"]
            
            if phone_val:
                digits_only = "".join(c for c in str(phone_val) if c.isdigit())
                digit_count = len(digits_only)
                if digit_count == 10:
                    actual = 0  # Valid
                elif digit_count == 11 and digits_only.startswith("1"):
                    actual = 0  # Valid (US country code)
                else:
                    actual = 1  # Invalid
            else:
                actual = 1  # Empty/NULL is invalid
            
            match = actual == expected
            results.append({
                "TEST_ROW_ID": row_id,
                "DQ_CHECK": "dq_invalid_phone_format",
                "TEST_ID": f"phone_{i}",
                "INPUT_VALUE": str(phone_val) if phone_val else "NULL",
                "EXPECTED": expected,
                "ACTUAL": actual,
                "RESULT": "PASS" if match else "** FAIL **"
            })
        
        # ================================================================
        # DQ CHECK 3: dq_incomplete_zip (4 tests per row)
        # Logic: Valid = exactly 5 digits OR exactly 9 digits (ZIP+4)
        # ================================================================
        for i in range(1, 5):
            zip_val = row[f"ZIP_{i}"]
            expected = row[f"ZIP_{i}_EXP"]
            
            if zip_val:
                digits_only = "".join(c for c in str(zip_val) if c.isdigit())
                digit_count = len(digits_only)
                if digit_count == 5 or digit_count == 9:
                    actual = 0  # Valid
                else:
                    actual = 1  # Invalid
            else:
                actual = 1  # Empty/NULL is invalid
            
            match = actual == expected
            results.append({
                "TEST_ROW_ID": row_id,
                "DQ_CHECK": "dq_incomplete_zip",
                "TEST_ID": f"zip_{i}",
                "INPUT_VALUE": str(zip_val) if zip_val else "NULL",
                "EXPECTED": expected,
                "ACTUAL": actual,
                "RESULT": "PASS" if match else "** FAIL **"
            })
        
        # ================================================================
        # DQ CHECK 4: dq_placeholder_category (4 tests per row)
        # Logic: Flag if category is exactly "Unknown" or "N/A"
        # ================================================================
        for i in range(1, 5):
            cat_val = row[f"CAT_{i}"]
            expected = row[f"CAT_{i}_EXP"]
            
            if cat_val and str(cat_val) in ("Unknown", "N/A"):
                actual = 1  # Placeholder found
            else:
                actual = 0  # Valid (including NULL, empty, lowercase)
            
            match = actual == expected
            results.append({
                "TEST_ROW_ID": row_id,
                "DQ_CHECK": "dq_placeholder_category",
                "TEST_ID": f"cat_{i}",
                "INPUT_VALUE": str(cat_val) if cat_val else "NULL",
                "EXPECTED": expected,
                "ACTUAL": actual,
                "RESULT": "PASS" if match else "** FAIL **"
            })
        
        # ================================================================
        # DQ CHECK 5: dq_outlier_amount (4 tests per row)
        # Logic: Flag if amount > 1,000,000
        # ================================================================
        for i in range(1, 5):
            amt_val = row[f"AMT_{i}"]
            expected = row[f"AMT_{i}_EXP"]
            
            try:
                amount = float(amt_val) if amt_val is not None else 0
                actual = 1 if amount > 1000000 else 0
            except:
                actual = 0  # Conversion error = treat as valid
            
            match = actual == expected
            results.append({
                "TEST_ROW_ID": row_id,
                "DQ_CHECK": "dq_outlier_amount",
                "TEST_ID": f"amt_{i}",
                "INPUT_VALUE": str(amt_val) if amt_val is not None else "NULL",
                "EXPECTED": expected,
                "ACTUAL": actual,
                "RESULT": "PASS" if match else "** FAIL **"
            })
    
    # Create Snowpark DataFrame from results
    result_df = session.create_dataframe(results)
    
    # Save to output table for additional queries
    result_df.write.mode("overwrite").save_as_table(output_table)
    
    # Return DataFrame for immediate display
    return result_df
';

-- ============================================================================
-- STEP 5: Execute the tests
-- ============================================================================

CALL RUN_DQ_TESTS(
    'SNOWBEARAIR_DB.GSULLIVAN.DQ_TEST_CASES',
    'SNOWBEARAIR_DB.GSULLIVAN.DQ_TEST_RESULTS'
);

-- ============================================================================
-- STEP 6: View Combined Results (Details + Summaries)
-- ============================================================================
-- Single query using UNION ALL to show:
--   Section 1: All 60 test details
--   Section 2: Summary by DQ check (5 rows)
--   Section 3: Summary by CSV row (3 rows)
--   Section 4: Grand total (1 row)
--   Section 5: Any failures (for debugging)

SELECT * FROM (
    -- Section 1: All test details
    SELECT 
        '1-DETAIL' AS section,
        dq_check,
        test_row_id AS csv_row,
        test_id,
        input_value,
        expected AS exp,
        actual AS act,
        result
    FROM DQ_TEST_RESULTS
    
    UNION ALL
    
    -- Section 2: Summary by DQ check
    SELECT 
        '2-SUMMARY BY CHECK' AS section,
        dq_check,
        NULL,
        NULL,
        tests || ' tests',
        passed,
        failed,
        pass_rate
    FROM (
        SELECT 
            dq_check,
            COUNT(*) AS tests,
            SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN result != 'PASS' THEN 1 ELSE 0 END) AS failed,
            ROUND(SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) || '%' AS pass_rate
        FROM DQ_TEST_RESULTS
        GROUP BY dq_check
    )
    
    UNION ALL
    
    -- Section 3: Summary by CSV row
    SELECT 
        '3-SUMMARY BY ROW' AS section,
        'csv_row_' || csv_row,
        NULL,
        NULL,
        tests || ' tests',
        passed,
        failed,
        pass_rate
    FROM (
        SELECT 
            test_row_id AS csv_row,
            COUNT(*) AS tests,
            SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN result != 'PASS' THEN 1 ELSE 0 END) AS failed,
            ROUND(SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) || '%' AS pass_rate
        FROM DQ_TEST_RESULTS
        GROUP BY test_row_id
    )
    
    UNION ALL
    
    -- Section 4: Grand total
    SELECT 
        '4-TOTAL' AS section,
        'ALL TESTS',
        NULL,
        NULL,
        COUNT(*) || ' tests',
        SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END),
        SUM(CASE WHEN result != 'PASS' THEN 1 ELSE 0 END),
        ROUND(SUM(CASE WHEN result = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) || '%'
    FROM DQ_TEST_RESULTS
    
    UNION ALL
    
    -- Section 5: Failures only (for debugging)
    SELECT 
        '5-FAILURES' AS section,
        dq_check,
        test_row_id,
        test_id,
        input_value,
        expected,
        actual,
        result
    FROM DQ_TEST_RESULTS
    WHERE result != 'PASS'
)
ORDER BY section, dq_check, csv_row, test_id;