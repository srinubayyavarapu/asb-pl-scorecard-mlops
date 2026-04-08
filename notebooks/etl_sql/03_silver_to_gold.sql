-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 03 - Silver to Gold (ML-Ready Datasets)
-- MAGIC **Joins Silver tables into ML-ready Gold datasets.**
-- MAGIC
-- MAGIC Creates:
-- MAGIC 1. HL Scorecard Training Data (Home Loans)
-- MAGIC 2. CC Scorecard Training Data (Credit Cards)
-- MAGIC 3. PL Scorecard Training Data (Personal Loans)
-- MAGIC 4. All Defaults Cross-Product
-- MAGIC 5. Model Parameters Reference
-- MAGIC
-- MAGIC Runs on: Serverless SQL Warehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 1: Home Loans Scorecard Training Data
-- MAGIC Joins: Account Base + GDW Extract + Base Level + Property + Default Flags

-- COMMAND ----------

CREATE OR REPLACE TABLE asb_dev.retail_gold.hl_scorecard_training AS
SELECT
    a.account_key,
    a.customer_id,
    a.product_type,
    a.loan_amount,
    a.interest_rate,
    a.loan_term_months,
    a.lvr,
    a.property_value,
    a.property_type,
    a.region,
    a.employment_type,
    a.annual_income,
    a.credit_score,
    a.months_on_book,
    a.account_status,
    a.origination_date,
    a.observation_date,
    -- Base level features
    b.customer_segment,
    b.debt_to_income,
    b.num_dependants,
    b.age,
    b.marital_status,
    b.time_at_address_months,
    b.time_at_employer_months,
    b.num_credit_enquiries,
    -- Property features
    p.property_class,
    p.num_bedrooms,
    p.land_area_sqm,
    p.year_built,
    p.valuation_amount,
    -- Arrears features (latest from GDW)
    g.arrears_days,
    g.outstanding_balance,
    g.monthly_payment,
    g.payment_status,
    g.exposure_amount,
    -- Target variable
    d.default_flag,
    d.max_arrears_days AS default_max_arrears,
    -- Metadata
    current_timestamp() AS _gold_created_at,
    'home_loans' AS _product_type
FROM asb_dev.
.hlacctbase_final a
LEFT JOIN asb_dev.retail_silver.hl_baselevel_199607_202408 b
    ON a.account_key = b.account_key
LEFT JOIN asb_dev.retail_silver.hl_ppty_res_type_199607_202408 p
    ON a.account_key = p.account_key
LEFT JOIN (
    -- Get latest GDW snapshot per account
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_key ORDER BY observation_date DESC) AS _rn
        FROM asb_dev.retail_silver.hl_gdwextract_199607_202408
    ) WHERE _rn = 1
) g ON a.account_key = g.account_key
LEFT JOIN asb_dev.retail_silver.add_default_flag_202408 d
    ON a.account_key = d.account_key
WHERE a.account_status = 'ACTIVE';

-- COMMAND ----------

SELECT 'hl_scorecard_training' AS tbl,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN default_flag = 1 THEN 1 ELSE 0 END) AS bad_count,
    SUM(CASE WHEN default_flag = 0 THEN 1 ELSE 0 END) AS good_count,
    ROUND(SUM(CASE WHEN default_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bad_rate_pct
FROM asb_dev.retail_gold.hl_scorecard_training;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 2: Credit Card Scorecard Training Data

-- COMMAND ----------

CREATE OR REPLACE TABLE asb_dev.retail_gold.cc_scorecard_training AS
SELECT
    a.account_key,
    a.customer_id,
    a.credit_limit,
    a.current_balance,
    a.utilization_pct,
    a.min_payment_due,
    a.months_on_book,
    a.annual_income,
    a.p1_score,
    a.observation_date,
    -- CC behavior features
    c.cash_advance_pct,
    c.num_transactions_3m,
    c.avg_transaction_amt,
    c.overlimit_count_12m,
    c.late_payment_count_12m,
    c.reward_tier,
    -- Facility features
    f.facility_type,
    f.facility_limit,
    f.drawn_amount,
    -- Arrears from GDW
    g.arrears_days,
    g.outstanding_balance AS gdw_balance,
    g.payment_status,
    -- Default flag
    d.default_flag,
    -- Metadata
    current_timestamp() AS _gold_created_at,
    'credit_cards' AS _product_type
FROM asb_dev.retail_silver.cc_scorecard_withp1 a
LEFT JOIN asb_dev.retail_silver.cc_dataframe c
    ON a.account_key = c.account_key
LEFT JOIN asb_dev.retail_silver.cc_facilitysnapshot_temp2 f
    ON a.account_key = f.account_key
LEFT JOIN (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_key ORDER BY observation_date DESC) AS _rn
        FROM asb_dev.retail_silver.cc_gdwextract_199607_202408
    ) WHERE _rn = 1
) g ON a.account_key = g.account_key
LEFT JOIN asb_dev.retail_silver.add_default_flag_202408 d
    ON a.account_key = d.account_key;

-- COMMAND ----------

SELECT 'cc_scorecard_training' AS tbl, COUNT(*) AS total_rows,
    ROUND(SUM(CASE WHEN default_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bad_rate_pct
FROM asb_dev.retail_gold.cc_scorecard_training;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 3: Personal Loans Scorecard Training Data

-- COMMAND ----------

CREATE OR REPLACE TABLE asb_dev.retail_gold.pl_scorecard_training AS
SELECT
    a.account_key,
    a.customer_id,
    a.loan_amount,
    a.interest_rate,
    a.loan_term_months,
    a.loan_purpose,
    a.annual_income,
    a.p1_score,
    a.observation_date,
    -- PL behavior
    p.repayment_frequency,
    p.auto_debit,
    p.missed_payments_6m,
    p.remaining_term_months,
    -- Default flag
    d.default_flag,
    -- Metadata
    current_timestamp() AS _gold_created_at,
    'personal_loans' AS _product_type
FROM asb_dev.retail_silver.pl_scorecard_withp1 a
LEFT JOIN asb_dev.retail_silver.pl_dataframe p
    ON a.account_key = p.account_key
LEFT JOIN asb_dev.retail_silver.add_default_flag_202408 d
    ON a.account_key = d.account_key;

-- COMMAND ----------

SELECT 'pl_scorecard_training' AS tbl, COUNT(*) AS total_rows,
    ROUND(SUM(CASE WHEN default_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS bad_rate_pct
FROM asb_dev.retail_gold.pl_scorecard_training;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 4: Cross-Product Defaults

-- COMMAND ----------

CREATE OR REPLACE TABLE asb_dev.retail_gold.all_defaults_base AS
SELECT *, current_timestamp() AS _gold_created_at
FROM asb_dev.retail_silver.all_default_baselevel_202408
WHERE _is_current = TRUE;

SELECT 'all_defaults_base' AS tbl, COUNT(*) AS rows FROM asb_dev.retail_gold.all_defaults_base;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold 5: SAS Model Parameters Reference

-- COMMAND ----------

CREATE OR REPLACE TABLE asb_dev.retail_gold.cc_model_params_ref AS
SELECT *, current_timestamp() AS _gold_created_at FROM asb_dev.retail_silver.cc_scorecard_model;

CREATE OR REPLACE TABLE asb_dev.retail_gold.pl_model_params_ref AS
SELECT *, current_timestamp() AS _gold_created_at FROM asb_dev.retail_silver.pl_scorecard_model;

SELECT 'model_params' AS tbl,
    (SELECT COUNT(*) FROM asb_dev.retail_gold.cc_model_params_ref) +
    (SELECT COUNT(*) FROM asb_dev.retail_gold.pl_model_params_ref) AS rows;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer Verification

-- COMMAND ----------

SELECT table_name
FROM asb_dev.information_schema.tables
WHERE table_schema = 'retail_gold'
ORDER BY table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC | Gold Table | Source Tables | Purpose |
-- MAGIC |-----------|-------------|---------|
-- MAGIC | hl_scorecard_training | hlacctbase + hl_baselevel + hl_ppty + hl_gdwextract + default_flags | ML: Home Loans model training |
-- MAGIC | cc_scorecard_training | cc_scorecard + cc_dataframe + cc_facility + cc_gdwextract + defaults | ML: Credit Cards model training |
-- MAGIC | pl_scorecard_training | pl_scorecard + pl_dataframe + defaults | ML: Personal Loans model training |
-- MAGIC | all_defaults_base | all_default_baselevel | Cross-product default analysis |
-- MAGIC | cc_model_params_ref | cc_scorecard_model | SAS model reference (validation) |
-- MAGIC | pl_model_params_ref | pl_scorecard_model | SAS model reference (validation) |
