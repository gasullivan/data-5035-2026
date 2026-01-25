-- ============================================================================
-- FILE: exercise02.sql
-- ASSIGNMENT: Data Engineering - Assignment 02 - Data Profiling and Quality

-- DATA: DONATIONS table provided (donations.csv)
-- AUTHOR: Greg Sullivan
-- DATE: January 2026
-- DESCRIPTION: Identify and flag at least 5, but up to 10 key data quality issues
--              in donor data including name formatting, missing categories, 
--              age/DOB mismatches, phone format inconsistencies, donation amount 
--              outliers, incomplete ZIP codes, category inconsistencies, and 
--              suspicious data patterns.  Do not use the Snowflake built-in
--              “Data Metric Functions”. But, now that I know there exists
--              such a capability I am very curious about it.
-- OVERVIEW: First of all, data profiling and quality has been a focus of mine
--           since I wrote my first program.  I learned early on the challenges
--           associated with coming to conclusions on bad data (as they say,
--           "garbage in, garbage out").  In various languages and using a
--           variety of tools, I've always enjoyed identifying data quality
--           issues in any dataset I encountered and found most people
--           surprised I even bothered.  I cannot say, however, that I've
--           undertaken such an endeavor in SQL, but did learn about its
--           modern-day power in my SQL class this summer.  So, here we go...
-- ============================================================================

-- Setup environment
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;
USE ROLE TRAINING_ROLE;

-- ============================================================================
-- MAIN QUERY: Comprehensive Data Quality Analysis
-- APPROACH: Initially, I experimented with smaller segments of code, and
--           eventually, put them all together in a single statement, as
--           required.  I found this file to be quite a mess!  As required
--           in the rubric, I isolate each data quality check in it's own
--           code block within the single SQL statement (very cool!).
-- ============================================================================

-- I'll grab all the fields and look at each of them in some way.
-- I developed these approaches in individual cells before I put
-- them all together in this single statement.  I needed to see
-- some outcomes which I derived from a bit of trial and error.
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
    -- DQ CHECK 8: Mixed Category Naming Conventions
    -- ISSUE: Category values use inconsistent naming conventions - some are
    --        single words while others are multi-word phrases. This suggests
    --        a mixture of coding standards or manual data entry without
    --        proper validation.
    -- IMPACT: Makes it difficult to group or analyze by category, and suggests
    --         the category field may not be following a controlled vocabulary
    -- RULE: Flag categories that contain spaces (multi-word), which may indicate
    --       inconsistent data entry standards
    -- ========================================================================
    CASE 
        WHEN category IS NOT NULL 
            AND TRIM(category) != ''
            AND CONTAINS(category, ' ') 
        THEN 1 
        ELSE 0 
    END AS dq_mixed_category_format,
    
    -- ========================================================================
    -- DQ CHECK 9: Suspiciously Round Ages
    -- ISSUE: Ages that are exact multiples of 5 or 10 may indicate estimation
    --        rather than actual data. When combined with the age/DOB mismatch,
    --        this suggests ages may have been guessed or rounded during entry.
    -- IMPACT: Demographic analysis becomes unreliable if ages are estimated
    --         rather than calculated from actual birth dates
    -- RULE: Flag ages that are multiples of 5 (20, 25, 30, 35, etc.)
    -- NOTE: This is a "soft" data quality issue - not necessarily wrong, but
    --       worth investigating when combined with other issues
    -- ========================================================================
    CASE 
        WHEN MOD(age, 5) = 0 
        THEN 1 
        ELSE 0 
    END AS dq_round_age,
    
    -- ========================================================================
    -- DQ CHECK 10: Excessive Street Address Detail
    -- ISSUE: Some addresses are unusually long (>50 characters), which may
    --        indicate excessive detail that makes the addresses difficult to
    --        use in mailings or hard to read. Long addresses may also indicate
    --        data entry errors or concatenation of multiple fields.
    -- IMPACT: Mail merge templates may not accommodate overly long addresses,
    --         and excessive detail may confuse postal delivery systems
    -- RULE: Flag addresses longer than 50 characters as potentially problematic
    -- ========================================================================
    CASE 
        WHEN LENGTH(street_address) > 50 
        THEN 1 
        ELSE 0 
    END AS dq_excessive_address_detail,
    
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
        CASE WHEN category IN ('Unknown', 'N/A') THEN 1 ELSE 0 END +
        CASE WHEN category IS NOT NULL AND TRIM(category) != '' AND CONTAINS(category, ' ') THEN 1 ELSE 0 END +
        CASE WHEN MOD(age, 5) = 0 THEN 1 ELSE 0 END +
        CASE WHEN LENGTH(street_address) > 50 THEN 1 ELSE 0 END
    ) AS total_dq_issues

FROM
    data5035.spring26.donations
    
ORDER BY
    total_dq_issues DESC,
    donation_id;


-- ============================================================================
-- DATA QUALITY SUMMARY TABLE
-- This query produces an easy-to-read summary table showing the count and
-- percentage of records affected by each data quality issue
-- ============================================================================

SELECT 
    'Data Quality Summary' AS report_title,
    '' AS dq_check_name,
    NULL AS records_affected,
    NULL AS percent_affected,
    '' AS severity

UNION ALL

SELECT 
    '',
    '1. Reversed Name Format',
    SUM(CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Medium'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '2. Missing Category',
    SUM(CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Medium'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '3. Age/DOB Mismatch',
    SUM(CASE WHEN ABS(age - (2024 - CASE WHEN YEAR(TO_DATE(date_of_birth)) > 2024 THEN YEAR(TO_DATE(date_of_birth)) - 100 ELSE YEAR(TO_DATE(date_of_birth)) END)) > 5 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN ABS(age - (2024 - CASE WHEN YEAR(TO_DATE(date_of_birth)) > 2024 THEN YEAR(TO_DATE(date_of_birth)) - 100 ELSE YEAR(TO_DATE(date_of_birth)) END)) > 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'HIGH'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '4. Invalid Phone Format',
    SUM(CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Medium'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '5. Outlier Amount',
    SUM(CASE WHEN amount > 1000000 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN amount > 1000000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'HIGH'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '6. Incomplete ZIP',
    SUM(CASE WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Medium'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '7. Placeholder Category',
    SUM(CASE WHEN category IN ('Unknown', 'N/A') THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN category IN ('Unknown', 'N/A') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Medium'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '8. Mixed Category Format',
    SUM(CASE WHEN category IS NOT NULL AND TRIM(category) != '' AND CONTAINS(category, ' ') THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN category IS NOT NULL AND TRIM(category) != '' AND CONTAINS(category, ' ') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Low'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '9. Round Age (Multiple of 5)',
    SUM(CASE WHEN MOD(age, 5) = 0 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN MOD(age, 5) = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Low'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '10. Excessive Address Detail',
    SUM(CASE WHEN LENGTH(street_address) > 50 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN LENGTH(street_address) > 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    'Low'
FROM data5035.spring26.donations

UNION ALL

SELECT 
    '',
    '---',
    NULL,
    NULL,
    ''

UNION ALL

SELECT 
    '',
    'TOTAL RECORDS',
    COUNT(*),
    100.0,
    ''
FROM data5035.spring26.donations

ORDER BY 
    CASE 
        WHEN dq_check_name = '' THEN 0
        WHEN dq_check_name = '---' THEN 999
        WHEN dq_check_name = 'TOTAL RECORDS' THEN 1000
        ELSE records_affected
    END DESC;


-- ============================================================================
-- DATA QUALITY ANALYSIS SUMMARY
-- ============================================================================
--
-- OVERVIEW:
-- I've seen some bad data, but this database might be the most I've ever seen.
-- The donor database has significant data quality challenges across nearly 
-- every field. Out of 200 records analyzed, the majority have at least one 
-- data quality issue, with many records having multiple problems.
--
-- KEY FINDINGS BY ISSUE:
--
-- 1. NAME FORMAT INCONSISTENCY (90 records - 45%)
--    Recommendation: Standardize on one format and create separate 
--    FIRST_NAME and LAST_NAME fields
--
-- 2. MISSING CATEGORIES (20 records - 10%)
--    Recommendation: Make CATEGORY a required field
--
-- 3. AGE/DATE OF BIRTH MISMATCH (167 records - 83.5%) **CRITICAL**
--    This is the most severe issue. Recommendation: Drop the AGE field 
--    entirely and always calculate from DATE_OF_BIRTH
--
-- 4. PHONE NUMBER FORMAT INCONSISTENCY (56 records - 28%)
--    Recommendation: Standardize on E.164 format or store only digits
--
-- 5. OUTLIER DONATION AMOUNTS (8 records - 4%) **CRITICAL**
--    8 donations exceed $1 million - likely data entry errors
--    Recommendation: Implement validation rules for large donations
--
-- 6. INCOMPLETE ZIP CODES (11 records - 5.5%)
--    Leading zeros were stripped during import
--    Recommendation: Store ZIP as VARCHAR instead of INTEGER
--
-- 7. PLACEHOLDER CATEGORIES (34 records - 17%)
--    "Unknown" and "N/A" provide no meaningful information
--    Recommendation: Review and classify based on organization context
--
-- 8. MIXED CATEGORY FORMAT (various records)
--    Categories use inconsistent naming (single word vs multi-word)
--    Recommendation: Establish controlled vocabulary for categories
--
-- 9. ROUND AGES (many records)
--    Ages that are multiples of 5 may indicate estimation
--    Recommendation: Always calculate age from DOB, don't store estimated ages
--
-- 10. EXCESSIVE ADDRESS DETAIL (some records)
--     Very long addresses may cause formatting issues
--     Recommendation: Establish maximum address length standards
--
-- BUSINESS IMPACT:
-- - DONOR COMMUNICATION: Name/phone formatting issues affect outreach
-- - COMPLIANCE: Age verification impossible with current AGE/DOB mismatch
-- - ANALYTICS: Demographic analysis unreliable with bad category data
-- - FINANCIAL REPORTING: Outlier amounts skew all statistics
-- - GEOGRAPHIC ANALYSIS: Incomplete ZIPs prevent mapping
--
-- RECOMMENDATIONS:
-- 1. Immediate: Flag all 8 records with >$1M donations for manual review
-- 2. Short-term: Implement data validation rules at point of entry
-- 3. Medium-term: Data cleanup project to standardize formats
-- 4. Long-term: Database redesign to eliminate redundant AGE field
--
-- The most critical issues are:
-- (1) AGE/DOB mismatch (83.5% of records)
-- (2) Outlier donation amounts (potential $20M+ in data entry errors)
-- ============================================================================