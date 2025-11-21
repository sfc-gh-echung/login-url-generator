SELECT 
    a.name as account_name,
    a.deployment,
    drc.snowflake_region,
    drc.snowflake_region_group || '.' || drc.snowflake_region as classic_ui_url
FROM snowhouse_import.prod.account_etl_v a
LEFT JOIN snowhouse_import.public.deployment_region_cloud drc 
    ON a.deployment = drc.deployment
WHERE a.SUSPENDED_TIME IS NULL
    AND a.DELETED_ON IS NULL

