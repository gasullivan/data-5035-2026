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