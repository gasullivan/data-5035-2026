-- ============================================================================
-- DIAGNOSTIC SUMMARY: Run this in a separate cell AFTER your main query
-- ============================================================================

-- Part 1: Summary Statistics
WITH dq_flags AS (
    SELECT 
        donation_id,
        CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END AS dq_reversed_name,
        CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END AS dq_missing_category,
        CASE WHEN ABS(age - (2024 - (CASE WHEN YEAR(date_of_birth) <= 24 THEN YEAR(date_of_birth) + 2000 WHEN YEAR(date_of_birth) < 100 THEN YEAR(date_of_birth) + 1900 ELSE YEAR(date_of_birth) END))) > 5 THEN 1 ELSE 0 END AS dq_age_dob_mismatch,
        CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) != 10 THEN 1 ELSE 0 END AS dq_invalid_phone_format,
        CASE WHEN amount > 1000000 THEN 1 ELSE 0 END AS dq_outlier_amount,
        CASE WHEN LENGTH(CAST(zip AS VARCHAR)) < 5 THEN 1 ELSE 0 END AS dq_incomplete_zip,
        CASE WHEN category IN ('Unknown', 'N/A') THEN 1 ELSE 0 END AS dq_placeholder_category
    FROM data5035.spring26.donations
)

SELECT
    'Reversed Name Format (Last, First)' AS data_quality_issue,
    SUM(dq_reversed_name) AS records_affected,
    ROUND(SUM(dq_reversed_name) / COUNT(*) * 100, 1) AS percent_affected
FROM dq_flags
UNION ALL
SELECT 'Invalid Phone Format', SUM(dq_invalid_phone_format), ROUND(SUM(dq_invalid_phone_format) / COUNT(*) * 100, 1) FROM dq_flags
UNION ALL
SELECT 'Placeholder Category (Unknown/N/A)', SUM(dq_placeholder_category), ROUND(SUM(dq_placeholder_category) / COUNT(*) * 100, 1) FROM dq_flags
UNION ALL
SELECT 'Missing Category', SUM(dq_missing_category), ROUND(SUM(dq_missing_category) / COUNT(*) * 100, 1) FROM dq_flags
UNION ALL
SELECT 'Incomplete ZIP Code (<5 digits)', SUM(dq_incomplete_zip), ROUND(SUM(dq_incomplete_zip) / COUNT(*) * 100, 1) FROM dq_flags
UNION ALL
SELECT 'Outlier Donation Amount (>$1M)', SUM(dq_outlier_amount), ROUND(SUM(dq_outlier_amount) / COUNT(*) * 100, 1) FROM dq_flags
UNION ALL
SELECT 'Age/DOB Mismatch (>5 years)', SUM(dq_age_dob_mismatch), ROUND(SUM(dq_age_dob_mismatch) / COUNT(*) * 100, 1) FROM dq_flags
ORDER BY records_affected DESC;