-- ============================================================================
-- SQL Runner - Snowflake Database Analysis
-- ============================================================================
-- This file contains SQL statements for various database analysis tasks
-- Each statement is labeled and commented for easy execution and understanding
-- ============================================================================

-- TASK_001: List all databases in the Snowflake account
-- Description: Shows all available databases with basic information
-- Label: list_databases
SELECT 
    DATABASE_NAME,
    DATABASE_OWNER,
    IS_TRANSIENT,
    COMMENT,
    CREATED,
    LAST_ALTERED
FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES
ORDER BY DATABASE_NAME;

-- ============================================================================

-- TASK_002: Count total tables across all databases
-- Description: Gets the total number of tables in the entire Snowflake account
-- Label: total_table_count
SELECT COUNT(*) AS TOTAL_TABLES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE DELETED IS NULL;

-- ============================================================================

-- TASK_003: Count tables per database
-- Description: Shows each database with its table count
-- Label: tables_per_database
SELECT 
    TABLE_CATALOG AS DATABASE_NAME,
    COUNT(*) AS TABLE_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE DELETED IS NULL
GROUP BY TABLE_CATALOG
ORDER BY TABLE_COUNT DESC, DATABASE_NAME;

-- ============================================================================

-- TASK_004: Detailed database and table summary
-- Description: Comprehensive view of databases with table counts and types
-- Label: database_table_summary
SELECT 
    d.DATABASE_NAME,
    d.DATABASE_OWNER,
    COALESCE(t.TABLE_COUNT, 0) AS TABLE_COUNT,
    COALESCE(t.VIEW_COUNT, 0) AS VIEW_COUNT,
    COALESCE(t.EXTERNAL_TABLE_COUNT, 0) AS EXTERNAL_TABLE_COUNT,
    d.CREATED AS DATABASE_CREATED,
    d.COMMENT AS DATABASE_COMMENT
FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES d
LEFT JOIN (
    SELECT 
        TABLE_CATALOG,
        COUNT(*) AS TABLE_COUNT,
        SUM(CASE WHEN TABLE_TYPE = 'VIEW' THEN 1 ELSE 0 END) AS VIEW_COUNT,
        SUM(CASE WHEN TABLE_TYPE = 'EXTERNAL TABLE' THEN 1 ELSE 0 END) AS EXTERNAL_TABLE_COUNT
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
    GROUP BY TABLE_CATALOG
) t ON d.DATABASE_NAME = t.TABLE_CATALOG
ORDER BY TABLE_COUNT DESC, d.DATABASE_NAME;

-- ============================================================================

-- TASK_005: Database size and object summary
-- Description: Shows databases with various object counts for complete overview
-- Label: database_object_summary
WITH database_stats AS (
    SELECT 
        TABLE_CATALOG AS DATABASE_NAME,
        COUNT(*) AS TOTAL_OBJECTS,
        SUM(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 1 ELSE 0 END) AS TABLES,
        SUM(CASE WHEN TABLE_TYPE = 'VIEW' THEN 1 ELSE 0 END) AS VIEWS,
        SUM(CASE WHEN TABLE_TYPE = 'EXTERNAL TABLE' THEN 1 ELSE 0 END) AS EXTERNAL_TABLES
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
    GROUP BY TABLE_CATALOG
)
SELECT 
    d.DATABASE_NAME,
    d.DATABASE_OWNER,
    COALESCE(ds.TOTAL_OBJECTS, 0) AS TOTAL_OBJECTS,
    COALESCE(ds.TABLES, 0) AS TABLES,
    COALESCE(ds.VIEWS, 0) AS VIEWS,
    COALESCE(ds.EXTERNAL_TABLES, 0) AS EXTERNAL_TABLES,
    d.IS_TRANSIENT,
    d.CREATED
FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES d
LEFT JOIN database_stats ds ON d.DATABASE_NAME = ds.DATABASE_NAME
ORDER BY TOTAL_OBJECTS DESC, d.DATABASE_NAME;

-- ============================================================================
-- End of SQL Runner File
-- ============================================================================ 