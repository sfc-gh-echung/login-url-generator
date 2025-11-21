SELECT DISTINCT
    a.name as account_name,
    a.deployment,
    drc.snowflake_region,
    drc.snowflake_region_group || '.' || drc.snowflake_region as classic_ui_url
FROM SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a
LEFT JOIN snowhouse_import.public.deployment_region_cloud drc 
    ON a.deployment = drc.deployment
INNER JOIN SNOWHOUSE_IMPORT.PROD.USER_ETL_V u
    ON u.ACCOUNT_ID = a.ID
    AND u.DEPLOYMENT = a.DEPLOYMENT
WHERE a.SUSPENDED_TIME IS NULL  -- Not suspended
    AND a.DELETED_ON IS NULL  -- Not deleted
    AND u.SCIM_EXTERNAL_ID IS NOT NULL  -- SCIM users only
    AND u.DELETED_ON IS NULL  -- Active users only
    AND u.LAST_SUC_LOGIN IS NULL  -- Never logged in
    AND u.CREATED_ON >= DATEADD(day, -5, DATE_TRUNC('day', '2025-11-20'::DATE))  -- Start of 5 days ago
    AND u.CREATED_ON < DATEADD(day, -4, DATE_TRUNC('day', '2025-11-20'::DATE))  -- Start of 4 days ago
ORDER BY a.name

