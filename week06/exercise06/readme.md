# Data Engineering - Assignment 06
## Unit Testing Data Quality Checks

**Author:** Greg Sullivan  
**Date:** February 2026  
**Course:** DATA 5035 - Data Engineering  
**Professor:** Paul Boal

---

## Overview

This assignment implements unit tests for 5 data quality checks originally developed in Exercise 02. The test framework validates that each DQ check correctly identifies data quality issues.

---

## ⚠️ IMPORTANT: Setup Before Running

**You must upload the test data CSV file before running the SQL script.**

### Step-by-Step Setup:

1. **Navigate to the Stage in Snowflake:**
   - Data → Databases → SNOWBEARAIR_DB → GSULLIVAN → Stages → DQ_TEST_STAGE

2. **Upload the CSV file:**
   - Click **"+ Files"**
   - Select **`exercise06_test_cases.csv`**
   - Click **Upload**

3. **Run the SQL script:**
   - Open **`exercise06.sql`**
   - Click **Run All**

---

## Architecture

### Design Principle: No Hardcoded Test Values

Following software engineering best practices, all test data is externalized to a CSV file rather than hardcoded in the SQL. This approach:

- Allows adding new test cases without code changes
- Keeps test data separate from test logic
- Enables version control of test data
- Supports automated test case generation

### File Structure

| File | Description |
|------|-------------|
| `exercise06.sql` | Test framework (stored procedure + queries) |
| `exercise06_test_cases.csv` | Test data (3 rows × 20 tests = 60 total) |
| `README.md` | This documentation |

---

## Data Quality Checks Tested

These checks were originally developed in Exercise 02 for the DONATIONS table:

| # | Check | Logic | Purpose |
|---|-------|-------|---------|
| 1 | `dq_reversed_name` | Contains comma? | Detect "Last, First" format |
| 2 | `dq_invalid_phone_format` | Exactly 10 digits, or 11 starting with 1 | Validate US phone numbers |
| 3 | `dq_incomplete_zip` | Exactly 5 or 9 digits | Validate ZIP / ZIP+4 format |
| 4 | `dq_placeholder_category` | Equals "Unknown" or "N/A" | Flag placeholder values |
| 5 | `dq_outlier_amount` | Greater than $1,000,000 | Flag suspicious donations |

---

## CSV Structure

The test data CSV has 40 columns (20 input values + 20 expected results):

```
name_1, name_1_exp, name_2, name_2_exp, ... amt_4, amt_4_exp
```

Each row represents a complete set of 20 test cases. The CSV includes 3 rows:

| Row | Description |
|-----|-------------|
| 1 | Baseline test cases |
| 2 | Alternate valid cases |
| 3 | Edge cases (special characters, boundaries) |

**Total: 60 tests (20 checks × 3 rows)**

---

## Test Results Interpretation

| Result | Meaning |
|--------|---------|
| **PASS** | DQ check logic worked correctly (actual = expected) |
| **FAIL** | DQ check logic has a bug (actual ≠ expected) |

A **PASS** means the DQ check correctly identified (or correctly passed) the test input. For example:

- Input: `"Boal, Paul"` → Expected: `1` → Actual: `1` → **PASS** ✓
- Input: `"Paul Boal"` → Expected: `0` → Actual: `0` → **PASS** ✓

---

## Output

The script produces a combined result set with 5 sections:

| Section | Description |
|---------|-------------|
| 1-DETAIL | All 60 test results with input/expected/actual |
| 2-SUMMARY BY CHECK | Pass rate per DQ check (5 rows) |
| 3-SUMMARY BY ROW | Pass rate per CSV row (3 rows) |
| 4-TOTAL | Grand total pass rate |
| 5-FAILURES | Any failed tests (for debugging) |

---

## Expected Results

- **Total Tests:** 60
- **Expected Pass Rate:** 96.7% (58/60)
- **Known Failures:** 2 edge cases demonstrating test coverage

---

## Technology

- **Platform:** Snowflake
- **Language:** SQL + Python (Snowpark)
- **Pattern:** Stored procedure returning TABLE (similar to Week 5 lab)
- **Database:** SNOWBEARAIR_DB.GSULLIVAN

---

## How to Add More Tests

To add additional test scenarios:

1. Open `exercise06_test_cases.csv`
2. Add a new row with 40 values (20 inputs + 20 expected results)
3. Re-upload to the stage
4. Run the SQL script

No code changes required!
