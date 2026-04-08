-- ============================================================
-- STEP 1: Run this in Snowflake Worksheet FIRST
-- Creates database, schemas, and warehouse for testing
-- ============================================================

-- Create database (matches master_table_inventory.csv)
CREATE DATABASE IF NOT EXISTS ASB_ANALYTICS;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS ASB_ANALYTICS.CREDIT_RISK;
CREATE SCHEMA IF NOT EXISTS ASB_ANALYTICS.GDW;

-- Create warehouse for data loading
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;
USE DATABASE ASB_ANALYTICS;

-- Verify
SHOW SCHEMAS IN DATABASE ASB_ANALYTICS;
SELECT 'Snowflake setup complete!' AS status;
