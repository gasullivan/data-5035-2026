-- ============================================================================
-- FILE: exercise02.sql
-- PURPOSE: Data Quality Analysis for DONATIONS table
-- AUTHOR: Greg
-- DATE: January 2026
-- DESCRIPTION: Identifies and flags 7 key data quality issues in donor data
--              including name formatting, missing categories, age/DOB mismatches,
--              phone format inconsistencies, donation amount outliers, and 
--              incomplete ZIP codes.
-- ============================================================================

SELECT 
    donation_id,
    name,
    age,
    date_of_birth,
    street_address,
    city,
    state,
    zip,
    phone,
    category,
    organization,
    amount,
    
    -- ========================================================================
    -- DQ CHECK 1: Inconsistent Name Format
    -- ISSUE: Names are stored in mixed formats - some as "Last, First" and 
    --        others as "First Last". This inconsistency makes sorting, 
    --        searching, and mail merge operations unreliable.
    -- IMPACT: Complicates donor communication and database matching
    -- RULE: Flag records where name contains a comma (Last, First format)
    -- ========================================================================
    CASE 
        WHEN CONTAINS(name, ',') THEN 1 
        ELSE 0 
    END AS dq_reversed_name,
    
    -- ========================================================================
    -- DQ CHECK 2: Missing or Blank Category
    -- ISSUE: 20 records have blank/empty category values, making it impossible
    --        to segment donors by interest area or properly route communications.
    -- IMPACT: Prevents targeted marketing and reduces donation effectiveness
    -- RULE: Flag records where category is blank, empty string, or just whitespace
    -- ========================================================================
    CASE 
        WHEN category IS NULL 
            OR TRIM(category) = '' 
        THEN 1 
        ELSE 0 
    END AS dq_missing_category,
    
    -- ========================================================================
    -- DQ CHECK 3: Age and Date of Birth Mismatch
    -- ISSUE: The AGE field doesn't match the calculated age from DATE_OF_BIRTH
    --        in 167 out of 200 records. This suggests either data entry errors
    --        or that AGE field was never updated while DOB remained static.
    -- IMPACT: Age restrictions for marketing (e.g., can't contact minors), 
    --         demographic analysis, and donor profiling become unreliable
    -- RULE: Calculate age from DOB (assuming current year 2024) and flag if 
    --       difference from AGE field exceeds 5 years
    -- NOTE: This check handles 2-digit year parsing by assuming years >24 
    --       refer to 1900s, while <=24 refer to 2000s
    -- ========================================================================
    CASE 
        WHEN ABS(
            age - (
                2024 - CASE 
                    WHEN YEAR(TO_DATE(date_of_birth)) > 2024 
                    THEN YEAR(TO_DATE(date_of_birth)) - 100
                    ELSE YEAR(TO_DATE(date_of_birth))
                END
            )
        ) > 5 
        THEN 1 
        ELSE 0 
    END AS dq_age_dob_mismatch,
    
    -- ========================================================================
    -- DQ CHECK 4: Phone Number Format Inconsistency
    -- ISSUE: Phone numbers are stored in at least 5 different formats:
    --        - Parentheses: (123) 456-7890
    --        - Dashes: 123-456-7890
    --        - Dots: 123.456.7890
    --        - Extensions: 1-234-567-8901x123
    --        - International: +1-234-567-8901
    --        Additionally, some numbers are missing a digit (only 9 digits)
    -- IMPACT: Automated calling systems fail, validation is difficult, and
    --         phone number matching across systems becomes unreliable
    -- RULE: Flag phone numbers with non-standard length (should be 10 digits
    --       for US numbers, excluding formatting characters)
    -- ========================================================================
    CASE 
        WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 
        THEN 1 
        ELSE 0 
    END AS dq_invalid_phone_format,
    
    -- ========================================================================
    -- DQ CHECK 5: Unrealistic Donation Amounts
    -- ISSUE: 8 donations exceed $1 million, which is highly unusual for 
    --        typical individual donors. These appear to be data entry errors
    --        (extra digits, decimal place errors) rather than legitimate 
    --        major gifts.
    -- IMPACT: Financial reporting becomes inaccurate, budget projections are
    --         skewed, and tax documentation may be incorrect
    -- RULE: Flag donations over $1,000,000 as potential data quality issues
    -- NOTE: In a production system, this threshold should be validated against
    --       organizational major gift policies and historical giving patterns
    -- ========================================================================
    CASE 
        WHEN amount > 1000000 
        THEN 1 
        ELSE 0 
    END AS dq_outlier_amount,
    
    -- ========================================================================
    -- DQ CHECK 6: Incomplete ZIP Codes
    -- ISSUE: 11 records have ZIP codes with fewer than 5 digits. US ZIP codes
    --        should always be 5 digits (or 5+4 format with hyphen). Leading 
    --        zeros may have been dropped during data import.
    -- IMPACT: Mail delivery failures, geographic analysis errors, and inability
    --         to match with demographic data or map visualizations
    -- RULE: Flag ZIP codes with fewer than 5 digits
    -- NOTE: This check doesn't validate that the ZIP is a real USPS code,
    --       only that it has the correct number of digits
    -- ========================================================================
    CASE 
        WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 
        THEN 1 
        ELSE 0 
    END AS dq_incomplete_zip,
    
    -- ========================================================================
    -- DQ CHECK 7: Placeholder or Generic Category Values  
    -- ISSUE: 15 records use "Unknown" and 19 use "N/A" as category values.
    --        While technically not null, these are placeholder values that 
    --        provide no meaningful classification.
    -- IMPACT: Same as missing category - prevents proper donor segmentation
    --         and reduces effectiveness of targeted fundraising campaigns
    -- RULE: Flag records where category is "Unknown" or "N/A"
    -- ========================================================================
    CASE 
        WHEN category IN ('Unknown', 'N/A') 
        THEN 1 
        ELSE 0 
    END AS dq_placeholder_category,
    
    -- ========================================================================
    -- SUMMARY FIELD: Total Data Quality Issues per Record
    -- This field sums all the individual DQ flags to give an overall quality
    -- score per record. Records with higher scores have more data quality
    -- issues and may require prioritized cleanup.
    -- ========================================================================
    (
        CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END +
        CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END +
        CASE WHEN ABS(age - (2024 - CASE WHEN YEAR(TO_DATE(date_of_birth)) > 2024 THEN YEAR(TO_DATE(date_of_birth)) - 100 ELSE YEAR(TO_DATE(date_of_birth)) END)) > 5 THEN 1 ELSE 0 END +
        CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 THEN 1 ELSE 0 END +
        CASE WHEN amount > 1000000 THEN 1 ELSE 0 END +
        CASE WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 THEN 1 ELSE 0 END +
        CASE WHEN category IN ('Unknown', 'N/A') THEN 1 ELSE 0 END
    ) AS total_dq_issues

FROM
    data5035.spring26.donations
    
ORDER BY
    total_dq_issues DESC,
    donation_id;

/**
============================================================================
DATA QUALITY ANALYSIS SUMMARY
============================================================================

OVERVIEW:
This donor database has significant data quality challenges across nearly 
every field. Out of 200 records analyzed, the majority have at least one 
data quality issue, with many records having multiple problems. The issues 
range from formatting inconsistencies to data integrity problems to potential
data entry errors.

KEY FINDINGS BY ISSUE:

1. NAME FORMAT INCONSISTENCY (90 records affected - 45%)
   - Almost half of all names use "Last, First" format while the other half
     use "First Last" format
   - Recommendation: Standardize on one format (preferably "First Last") and 
     create separate FIRST_NAME and LAST_NAME fields for better data management

2. MISSING CATEGORIES (20 records affected - 10%)
   - 20 records have completely blank category values
   - Recommendation: Make CATEGORY a required field and/or provide a default
     "General" category for records that don't fit other classifications

3. AGE/DATE OF BIRTH MISMATCH (167 records affected - 83.5%)
   - This is the most severe issue, affecting the vast majority of records
   - Many ages are off by 20-30 years or more from what DOB indicates
   - Recommendation: Drop the AGE field entirely and always calculate age
     from DATE_OF_BIRTH at query time. Alternatively, implement a database
     trigger to auto-update AGE whenever DOB changes.

4. PHONE NUMBER FORMAT INCONSISTENCY (56 records affected - 28%)
   - Multiple competing formats make phone number validation and automated
     dialing impossible without preprocessing
   - 2 records have only 9 digits (missing a digit entirely)
   - Recommendation: Standardize on E.164 international format or at minimum
     store only digits and format at presentation time

5. OUTLIER DONATION AMOUNTS (8 records affected - 4%)
   - 8 donations exceed $1 million, ranging from $1.4M to $4.9M
   - Given that 75% of donations are under $400, these appear to be data
     entry errors (possibly decimal place problems or extra digits)
   - Recommendation: Implement validation rules to flag donations over a
     reasonable threshold (e.g., $10,000) for manual review before acceptance

6. INCOMPLETE ZIP CODES (11 records affected - 5.5%)
   - Leading zeros were likely stripped during import (e.g., "730" should
     probably be "00730" for a valid US ZIP)
   - Recommendation: Store ZIP as VARCHAR/TEXT instead of INTEGER to preserve
     leading zeros, and validate against USPS ZIP code database

7. PLACEHOLDER CATEGORIES (34 records affected - 17%)
   - "Unknown" and "N/A" provide no meaningful information for segmentation
   - Combined with blank categories, this means 27% of records lack proper
     classification
   - Recommendation: Review these records and attempt to classify based on
     organization name or other contextual clues

BUSINESS IMPACT:
These data quality issues have real operational consequences:

- DONOR COMMUNICATION: Name formatting and phone number issues make it 
  difficult to reach donors reliably. Mail merge operations may fail or 
  produce incorrectly formatted letters.

- COMPLIANCE: Age verification is impossible with the current AGE/DOB mismatch,
  creating potential legal issues around contacting minors or meeting 
  regulatory requirements.

- ANALYTICS: Demographic analysis and donor segmentation are unreliable when
  27% of records lack meaningful categories and age data is incorrect in 83%
  of records.

- FINANCIAL REPORTING: Outlier amounts of $4.9M skew all summary statistics,
  making it impossible to accurately report on giving patterns or set 
  realistic budget targets.

- GEOGRAPHIC ANALYSIS: Incomplete ZIP codes prevent accurate mapping and 
  demographic overlay analysis.

RECOMMENDATIONS:
1. Immediate: Flag all 8 records with >$1M donations for manual review
2. Short-term: Implement data validation rules at point of entry to prevent
   future quality issues
3. Medium-term: Data cleanup project to standardize name and phone formats
4. Long-term: Database redesign to eliminate the redundant AGE field and 
   properly type ZIP codes as VARCHAR

The most critical issue to address is the AGE/DOB mismatch, as it affects
operational compliance. The second priority should be standardizing phone
numbers to enable reliable contact. The outlier amounts should be reviewed
immediately to ensure financial reporting accuracy.

**/        WHEN ABS(
            age - (
                2024 - CASE 
                    WHEN YEAR(TRY_TO_DATE(date_of_birth, 'MM/DD/YY')) > 2024 
                    THEN YEAR(TRY_TO_DATE(date_of_birth, 'MM/DD/YY')) - 100
                    ELSE YEAR(TRY_TO_DATE(date_of_birth, 'MM/DD/YY'))
                END
            )
        ) > 5 
        THEN 1 
        ELSE 0 
    END AS dq_age_dob_mismatch,
    
    -- ========================================================================
    -- DQ CHECK 4: Phone Number Format Inconsistency
    -- ISSUE: Phone numbers are stored in at least 5 different formats:
    --        - Parentheses: (123) 456-7890
    --        - Dashes: 123-456-7890
    --        - Dots: 123.456.7890
    --        - Extensions: 1-234-567-8901x123
    --        - International: +1-234-567-8901
    --        Additionally, some numbers are missing a digit (only 9 digits)
    -- IMPACT: Automated calling systems fail, validation is difficult, and
    --         phone number matching across systems becomes unreliable
    -- RULE: Flag phone numbers with non-standard length (should be 10 digits
    --       for US numbers, excluding formatting characters)
    -- ========================================================================
    CASE 
        WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 
        THEN 1 
        ELSE 0 
    END AS dq_invalid_phone_format,
    
    -- ========================================================================
    -- DQ CHECK 5: Unrealistic Donation Amounts
    -- ISSUE: 8 donations exceed $1 million, which is highly unusual for 
    --        typical individual donors. These appear to be data entry errors
    --        (extra digits, decimal place errors) rather than legitimate 
    --        major gifts.
    -- IMPACT: Financial reporting becomes inaccurate, budget projections are
    --         skewed, and tax documentation may be incorrect
    -- RULE: Flag donations over $1,000,000 as potential data quality issues
    -- NOTE: In a production system, this threshold should be validated against
    --       organizational major gift policies and historical giving patterns
    -- ========================================================================
    CASE 
        WHEN amount > 1000000 
        THEN 1 
        ELSE 0 
    END AS dq_outlier_amount,
    
    -- ========================================================================
    -- DQ CHECK 6: Incomplete ZIP Codes
    -- ISSUE: 11 records have ZIP codes with fewer than 5 digits. US ZIP codes
    --        should always be 5 digits (or 5+4 format with hyphen). Leading 
    --        zeros may have been dropped during data import.
    -- IMPACT: Mail delivery failures, geographic analysis errors, and inability
    --         to match with demographic data or map visualizations
    -- RULE: Flag ZIP codes with fewer than 5 digits
    -- NOTE: This check doesn't validate that the ZIP is a real USPS code,
    --       only that it has the correct number of digits
    -- ========================================================================
    CASE 
        WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 
        THEN 1 
        ELSE 0 
    END AS dq_incomplete_zip,
    
    -- ========================================================================
    -- DQ CHECK 7: Placeholder or Generic Category Values  
    -- ISSUE: 15 records use "Unknown" and 19 use "N/A" as category values.
    --        While technically not null, these are placeholder values that 
    --        provide no meaningful classification.
    -- IMPACT: Same as missing category - prevents proper donor segmentation
    --         and reduces effectiveness of targeted fundraising campaigns
    -- RULE: Flag records where category is "Unknown" or "N/A"
    -- ========================================================================
    CASE 
        WHEN category IN ('Unknown', 'N/A') 
        THEN 1 
        ELSE 0 
    END AS dq_placeholder_category,
    
    -- ========================================================================
    -- SUMMARY FIELD: Total Data Quality Issues per Record
    -- This field sums all the individual DQ flags to give an overall quality
    -- score per record. Records with higher scores have more data quality
    -- issues and may require prioritized cleanup.
    -- ========================================================================
    (
        CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END +
        CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END +
        CASE WHEN ABS(age - (2024 - CASE WHEN YEAR(TRY_TO_DATE(date_of_birth, 'MM/DD/YY')) > 2024 THEN YEAR(TRY_TO_DATE(date_of_birth, 'MM/DD/YY')) - 100 ELSE YEAR(TRY_TO_DATE(date_of_birth, 'MM/DD/YY')) END)) > 5 THEN 1 ELSE 0 END +
        CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 THEN 1 ELSE 0 END +
        CASE WHEN amount > 1000000 THEN 1 ELSE 0 END +
        CASE WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 THEN 1 ELSE 0 END +
        CASE WHEN category IN ('Unknown', 'N/A') THEN 1 ELSE 0 END
    ) AS total_dq_issues

FROM
    data5035.spring26.donations
    
ORDER BY
    total_dq_issues DESC,
    donation_id;

/**
============================================================================
DATA QUALITY ANALYSIS SUMMARY
============================================================================

OVERVIEW:
This donor database has significant data quality challenges across nearly 
every field. Out of 200 records analyzed, the majority have at least one 
data quality issue, with many records having multiple problems. The issues 
range from formatting inconsistencies to data integrity problems to potential
data entry errors.

KEY FINDINGS BY ISSUE:

1. NAME FORMAT INCONSISTENCY (90 records affected - 45%)
   - Almost half of all names use "Last, First" format while the other half
     use "First Last" format
   - Recommendation: Standardize on one format (preferably "First Last") and 
     create separate FIRST_NAME and LAST_NAME fields for better data management

2. MISSING CATEGORIES (20 records affected - 10%)
   - 20 records have completely blank category values
   - Recommendation: Make CATEGORY a required field and/or provide a default
     "General" category for records that don't fit other classifications

3. AGE/DATE OF BIRTH MISMATCH (167 records affected - 83.5%)
   - This is the most severe issue, affecting the vast majority of records
   - Many ages are off by 20-30 years or more from what DOB indicates
   - Recommendation: Drop the AGE field entirely and always calculate age
     from DATE_OF_BIRTH at query time. Alternatively, implement a database
     trigger to auto-update AGE whenever DOB changes.

4. PHONE NUMBER FORMAT INCONSISTENCY (56 records affected - 28%)
   - Multiple competing formats make phone number validation and automated
     dialing impossible without preprocessing
   - 2 records have only 9 digits (missing a digit entirely)
   - Recommendation: Standardize on E.164 international format or at minimum
     store only digits and format at presentation time

5. OUTLIER DONATION AMOUNTS (8 records affected - 4%)
   - 8 donations exceed $1 million, ranging from $1.4M to $4.9M
   - Given that 75% of donations are under $400, these appear to be data
     entry errors (possibly decimal place problems or extra digits)
   - Recommendation: Implement validation rules to flag donations over a
     reasonable threshold (e.g., $10,000) for manual review before acceptance

6. INCOMPLETE ZIP CODES (11 records affected - 5.5%)
   - Leading zeros were likely stripped during import (e.g., "730" should
     probably be "00730" for a valid US ZIP)
   - Recommendation: Store ZIP as VARCHAR/TEXT instead of INTEGER to preserve
     leading zeros, and validate against USPS ZIP code database

7. PLACEHOLDER CATEGORIES (34 records affected - 17%)
   - "Unknown" and "N/A" provide no meaningful information for segmentation
   - Combined with blank categories, this means 27% of records lack proper
     classification
   - Recommendation: Review these records and attempt to classify based on
     organization name or other contextual clues

BUSINESS IMPACT:
These data quality issues have real operational consequences:

- DONOR COMMUNICATION: Name formatting and phone number issues make it 
  difficult to reach donors reliably. Mail merge operations may fail or 
  produce incorrectly formatted letters.

- COMPLIANCE: Age verification is impossible with the current AGE/DOB mismatch,
  creating potential legal issues around contacting minors or meeting 
  regulatory requirements.

- ANALYTICS: Demographic analysis and donor segmentation are unreliable when
  27% of records lack meaningful categories and age data is incorrect in 83%
  of records.

- FINANCIAL REPORTING: Outlier amounts of $4.9M skew all summary statistics,
  making it impossible to accurately report on giving patterns or set 
  realistic budget targets.

- GEOGRAPHIC ANALYSIS: Incomplete ZIP codes prevent accurate mapping and 
  demographic overlay analysis.

RECOMMENDATIONS:
1. Immediate: Flag all 8 records with >$1M donations for manual review
2. Short-term: Implement data validation rules at point of entry to prevent
   future quality issues
3. Medium-term: Data cleanup project to standardize name and phone formats
4. Long-term: Database redesign to eliminate the redundant AGE field and 
   properly type ZIP codes as VARCHAR

The most critical issue to address is the AGE/DOB mismatch, as it affects
operational compliance. The second priority should be standardizing phone
numbers to enable reliable contact. The outlier amounts should be reviewed
immediately to ensure financial reporting accuracy.

**/