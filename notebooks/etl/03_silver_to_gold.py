# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Silver to Gold (PL Application Scorecard)
# MAGIC
# MAGIC Joins the 9 client-side silver tables into a single application-level
# MAGIC dataset for ML. Source-system column names (OCCUPATION, INCOME_SOURCE,
# MAGIC HOUSING_STATUS, ...) are aliased to the ML-expected names
# MAGIC (pie_occupation, pie_incomesource, pie_accomodator, ...).
# MAGIC
# MAGIC ```
# MAGIC FACT_SMApplicationRequest                    (1 row per application)
# MAGIC   + DIM_SMApplicationRequest                 (purpose / term / type)
# MAGIC   + DIM_SMOnyxApplication                    (channel / bureau / DTI)
# MAGIC   + DIM_SMApplicationRequestSummary          (demographics + financials)
# MAGIC   LEFT JOIN FACT_SMBridgeapplicationfacility (approved -> facility)
# MAGIC   LEFT JOIN DIM_facility                     (facility master)
# MAGIC   LEFT JOIN FACT_CreditFacility (aggregated  (24-mo performance per facility)
# MAGIC ```
# MAGIC
# MAGIC Derives:
# MAGIC - **target_flag**: Good / Bad / Indeterminate / Rejected (from 24-mo arrears)
# MAGIC - **sample_flag**: Funded / NotFunded
# MAGIC - **dp3_application**: month-end of application_date (period field)
# MAGIC
# MAGIC Output: `<catalog>.gold.pl_application_scorecard_data`

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../utils/job_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog (bundle passes ${var.catalog})")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

# Silver source FQNs — match target_silver_table values from master CSV
S = f"{catalog}.silver"
APP_REQ_DIM_SILVER    = f"{S}.dim_sm_application_request_silver"
ONYX_DIM_SILVER       = f"{S}.dim_sm_onyx_application_silver"
SUMMARY_DIM_SILVER    = f"{S}.dim_sm_application_request_summary_silver"
APP_REQ_FACT_SILVER   = f"{S}.fact_sm_application_request_silver"
BRIDGE_FACT_SILVER    = f"{S}.fact_sm_bridge_application_facility_silver"
FACILITY_DIM_SILVER   = f"{S}.dim_facility_silver"
PERFORMANCE_SILVER    = f"{S}.fact_credit_facility_silver"

GOLD_TABLE = f"{catalog}.gold.pl_application_scorecard_data"

# Target derivation thresholds (per walkthrough)
BAD_ARREARS_THRESHOLD          = 90   # >= 90 days arrears OR hardship = Bad
INDETERMINATE_ARREARS_THRESHOLD = 1   # 1..89 days = Indeterminate
PERFORMANCE_WINDOW_MONTHS       = 24

print(f"Catalog: {catalog}")
print(f"Output:  {GOLD_TABLE}")

# Ensure Gold schema exists (UC does not auto-create schemas on saveAsTable)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper — current-only view of SCD2 silvers

# COMMAND ----------

def silver_current(table_fqn):
    """Read a Silver table and return only currently-effective rows, with
    SCD2 / ingestion metadata stripped so the join output stays clean.
    Tolerates non-SCD2 (append-only) silvers that have no _is_current column."""
    df = spark.table(table_fqn)
    if "_is_current" in df.columns:
        df = df.filter(F.col("_is_current") == True)
    drop_meta = [c for c in df.columns if c.startswith("_")]
    return df.drop(*drop_meta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load silvers

# COMMAND ----------

start = datetime.now()

app_req_dim  = silver_current(APP_REQ_DIM_SILVER)
onyx_dim     = silver_current(ONYX_DIM_SILVER)
summary_dim  = silver_current(SUMMARY_DIM_SILVER)
app_req_fact = silver_current(APP_REQ_FACT_SILVER)
bridge_fact  = silver_current(BRIDGE_FACT_SILVER)
facility_dim = silver_current(FACILITY_DIM_SILVER)
performance  = silver_current(PERFORMANCE_SILVER)

print(f"DIM_SMApplicationRequest:         {app_req_dim.count():,}")
print(f"DIM_SMOnyxApplication:            {onyx_dim.count():,}")
print(f"DIM_SMApplicationRequestSummary:  {summary_dim.count():,}")
print(f"FACT_SMApplicationRequest:        {app_req_fact.count():,}")
print(f"FACT_SMBridgeapplicationfacility: {bridge_fact.count():,}")
print(f"DIM_facility:                     {facility_dim.count():,}")
print(f"FACT_CreditFacility:              {performance.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the application grain (4-way join on origination side)

# COMMAND ----------

# Slim each dim so joins don't drag duplicate columns
app_req_dim_slim = app_req_dim.select(
    "application_key",
    "loan_purpose",
    F.col("requested_term_months").alias("loan_term_months_requested"),
    "application_type",
)

onyx_slim = onyx_dim.select(
    "onyx_key",
    "channel",
    "branch_code",
    F.col("credit_bureau_score").alias("credit_score_bureau"),
    "dti_ratio",
)

summary_slim = summary_dim.select(
    "summary_key",
    "customer_id",
    "age",
    "annual_income",
    F.col("occupation").alias("pie_occupation"),
    F.col("income_source").alias("pie_incomesource"),
    F.col("housing_status").alias("pie_accomodator"),
    "region",
    F.col("marital").alias("marital_status"),
    F.col("dependants").alias("num_dependants"),
    "existing_debt",
    F.col("num_enquiries_6m").alias("num_credit_enquiries"),
    F.col("time_at_address_mo").alias("time_at_address_months"),
    F.col("time_at_employer_mo").alias("time_at_employer_months"),
)

app_grain = (
    app_req_fact.alias("f")
    .join(app_req_dim_slim.alias("d1"), "application_key", "left")
    .join(onyx_slim.alias("d2"),         "onyx_key",         "left")
    .join(summary_slim.alias("d3"),      "summary_key",      "left")
    .select(
        F.col("f.application_request_id").alias("application_id"),
        F.col("f.customer_id"),
        F.col("f.application_date"),
        F.col("f.decision_date"),
        F.col("f.requested_amount").alias("loan_amount_requested"),
        F.col("f.decision").alias("application_decision"),
        F.col("f.decision_reason"),
        F.col("f.product_code"),
        # From dims
        F.col("d1.loan_purpose"),
        F.col("d1.loan_term_months_requested"),
        F.col("d1.application_type"),
        F.col("d2.channel"),
        F.col("d2.branch_code"),
        F.col("d2.credit_score_bureau"),
        F.col("d2.dti_ratio"),
        F.col("d3.age"),
        F.col("d3.annual_income"),
        F.col("d3.pie_occupation"),
        F.col("d3.pie_incomesource"),
        F.col("d3.pie_accomodator"),
        F.col("d3.region"),
        F.col("d3.marital_status"),
        F.col("d3.num_dependants"),
        F.col("d3.existing_debt"),
        F.col("d3.num_credit_enquiries"),
        F.col("d3.time_at_address_months"),
        F.col("d3.time_at_employer_months"),
    )
)

# Cast application_date to date type if it isn't already (from CSV string ingest)
app_grain = app_grain.withColumn("application_date", F.to_date(F.col("application_date")))

print(f"Application grain: {app_grain.count():,} rows (should equal FACT_SMApplicationRequest)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Attach facility (bridge -> DIM_facility) for approved applications

# COMMAND ----------

bridge_slim = bridge_fact.select(
    F.col("application_request_id").alias("application_id"),
    "facility_key",
    F.col("created_date").alias("funded_date"),
)

facility_slim = facility_dim.select(
    "facility_key",
    F.col("facility_reference").alias("facility_id"),
    F.col("initial_limit").alias("loan_amount_funded"),
    "interest_rate",
    F.col("term_months").alias("loan_term_months"),
    "repayment_frequency",
    "auto_debit_flag",
    "facility_status",
)

app_with_facility = (
    app_grain.alias("a")
    .join(bridge_slim.alias("b"),  "application_id", "left")
    .join(facility_slim.alias("f"), "facility_key",  "left")
)

print(f"After bridge + facility join: {app_with_facility.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregate FACT_CreditFacility into a 24-month performance summary per facility

# COMMAND ----------

perf_agg = (
    performance
    .filter(F.col("months_since_funding") <= PERFORMANCE_WINDOW_MONTHS)
    .groupBy("facility_key")
    .agg(
        F.max("arrears_days").alias("max_arrears_days_24mo"),
        F.max(F.when(F.col("hardship_flag") == "Y", 1).otherwise(0)).alias("hardship_ever_24mo"),
        F.count("*").alias("perf_observations"),
        F.max("snapshot_date").alias("last_observed_date"),
    )
)

print(f"Facilities with performance: {perf_agg.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Final join — attach performance summary to application grain

# COMMAND ----------

joined = (
    app_with_facility.alias("a")
    .join(perf_agg.alias("p"), "facility_key", "left")
)

# Drop the surrogate facility_key from the gold output — the business id
# facility_id (already aliased from facility_reference) is what downstream uses.
joined = joined.drop("facility_key")

print(f"Final joined rows: {joined.count():,} (should equal applications count)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derive target_flag and sample_flag

# COMMAND ----------

# Target derivation per walkthrough:
#   funded + max_arrears >= 90 OR hardship   -> Bad
#   funded + max_arrears == 0                 -> Good
#   funded + 1..89 day arrears                -> Indeterminate
#   not funded (declined application)         -> Rejected
gold = (
    joined
    .withColumn(
        "target_flag",
        F.when(F.col("application_decision") == "DECLINED", F.lit("Rejected"))
         .when(
             (F.col("max_arrears_days_24mo") >= BAD_ARREARS_THRESHOLD) |
             (F.col("hardship_ever_24mo") == 1),
             F.lit("Bad"),
         )
         .when(F.col("max_arrears_days_24mo") == 0, F.lit("Good"))
         .when(F.col("max_arrears_days_24mo") >= INDETERMINATE_ARREARS_THRESHOLD, F.lit("Indeterminate"))
         .otherwise(F.lit("NotFunded")),  # funded but no perf yet (edge case)
    )
    .withColumn(
        "sample_flag",
        F.when(F.col("application_decision") == "APPROVED", F.lit("Funded"))
         .otherwise(F.lit("NotFunded")),
    )
    # DP3 month-end of application_date (period field per walkthrough)
    .withColumn("dp3_application", F.last_day(F.col("application_date")))
    .withColumn("_gold_built_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Distribution sanity check

# COMMAND ----------

print("\nTarget distribution:")
gold.groupBy("target_flag").count().orderBy(F.col("count").desc()).show()

print("Sample flag distribution:")
gold.groupBy("sample_flag").count().show()

print("\nFunded population — arrears profile:")
gold.filter(F.col("sample_flag") == "Funded") \
    .groupBy("target_flag") \
    .agg(F.count("*").alias("n"),
         F.round(F.avg("max_arrears_days_24mo"), 1).alias("avg_max_arrears")) \
    .orderBy("target_flag") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Gold

# COMMAND ----------

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

# Iceberg UniForm so Snowflake can read this, plus CDF for downstream monitors
enable_iceberg_uniform(GOLD_TABLE, extra_props={"delta.enableChangeDataFeed": "true"})

final_count = spark.table(GOLD_TABLE).count()

elapsed = (datetime.now() - start).total_seconds()
print(f"\n{'='*55}\nSILVER -> GOLD COMPLETE\n{'='*55}")
print(f"Output:  {GOLD_TABLE}")
print(f"Rows:    {final_count:,}")
print(f"Elapsed: {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|pl_application_scorecard_data|{final_count}|silver_to_gold|{elapsed:.1f}s")
