SELECT 
    'Missing/Blank (NULL or empty after TRIM)' AS check_type,
    SUM(CASE WHEN category IS NULL OR TRIM(category) = '' THEN 1 ELSE 0 END) AS count
FROM data5035.spring26.donations

UNION ALL

SELECT 
    'Placeholder N/A',
    SUM(CASE WHEN category = 'N/A' THEN 1 ELSE 0 END)
FROM data5035.spring26.donations

UNION ALL

SELECT 
    'Placeholder Unknown',
    SUM(CASE WHEN category = 'Unknown' THEN 1 ELSE 0 END)
FROM data5035.spring26.donations;