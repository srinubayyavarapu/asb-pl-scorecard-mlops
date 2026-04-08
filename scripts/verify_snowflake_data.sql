-- ============================================================
-- STEP 3: Run this in Snowflake Worksheet to verify data
-- ============================================================

USE DATABASE ASB_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- Check all tables exist and have data
SELECT 'CREDIT_RISK' AS schema_name, TABLE_NAME, ROW_COUNT
FROM ASB_ANALYTICS.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'CREDIT_RISK'
UNION ALL
SELECT 'GDW' AS schema_name, TABLE_NAME, ROW_COUNT
FROM ASB_ANALYTICS.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'GDW'
ORDER BY schema_name, TABLE_NAME;

-- Quick data check: Home Loans
SELECT COUNT(*) AS total, SUM(CASE WHEN ACCOUNT_STATUS = 'ACTIVE' THEN 1 ELSE 0 END) AS active
FROM CREDIT_RISK.HLACCTBASE_FINAL;

-- Quick data check: Default rates
SELECT DEFAULT_FLAG, COUNT(*) AS cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM CREDIT_RISK.ADD_DEFAULT_FLAG_202408
GROUP BY DEFAULT_FLAG;

-- Quick data check: GDW Extract (monthly history)
SELECT MIN(OBSERVATION_DATE) AS earliest, MAX(OBSERVATION_DATE) AS latest, COUNT(DISTINCT ACCOUNT_KEY) AS accounts
FROM GDW.HL_GDWEXTRACT_199607_202408;
