# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Silver to Gold (PL Application Scorecard)
# MAGIC
# MAGIC Joins the 4 Silver tables into a single application-level dataset for ML:
# MAGIC
# MAGIC ```
# MAGIC pl_applications_silver         (TTD: funded + rejected)
# MAGIC   LEFT JOIN pl_facilities_silver       on application_id  (funded only)
# MAGIC   LEFT JOIN credit_performance_agg     on facility_id     (24-mo aggregate)
# MAGIC   LEFT JOIN pl_sas_final_scores_silver on application_id  (SAS reference)
# MAGIC ```
# MAGIC
# MAGIC Derives:
# MAGIC - **target_flag**: Good / Bad / Indeterminate / Rejected (from 24-mo performance)
# MAGIC - **sample_flag**: Funded / NotFunded (drives reject inference downstream)
# MAGIC - **dp3_application**: month-end of application_date (period field per walkthrough)
# MAGIC
# MAGIC Output: `<catalog>.retail_gold.pl_application_scorecard_data`

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "asb_dev", "Unity Catalog")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

APPLICATIONS_SILVER       = f"{catalog}.retail_silver.pl_applications_silver"
FACILITIES_SILVER         = f"{catalog}.retail_silver.pl_facilities_silver"
CREDIT_PERFORMANCE_SILVER = f"{catalog}.retail_silver.pl_credit_performance_silver"
SAS_SCORES_SILVER         = f"{catalog}.retail_silver.pl_sas_final_scores_silver"

GOLD_TABLE = f"{catalog}.retail_gold.pl_application_scorecard_data"

# Target derivation thresholds (per ASB walkthrough)
BAD_ARREARS_THRESHOLD          = 90   # >= 90 days arrears OR hardship = Bad
INDETERMINATE_ARREARS_THRESHOLD = 1   # 1..89 days = Indeterminate
PERFORMANCE_WINDOW_MONTHS       = 24

print(f"Catalog: {catalog}")
print(f"Output:  {GOLD_TABLE}")

# Ensure Gold schema exists (UC does not auto-create schemas on saveAsTable)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.retail_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper — current-only view of SCD2 silvers

# COMMAND ----------

def silver_current(table_fqn):
    """Read a Silver table and return only the currently-effective rows.
    Tolerates non-SCD2 (append-only) silvers that have no _is_current column."""
    df = spark.table(table_fqn)
    if "_is_current" in df.columns:
        df = df.filter(F.col("_is_current") == True)
    # Drop SCD2 / ingestion metadata so the join output stays clean
    drop_meta = [c for c in df.columns if c.startswith("_")]
    return df.drop(*drop_meta), drop_meta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load silvers

# COMMAND ----------

start = datetime.now()

apps,   _ = silver_current(APPLICATIONS_SILVER)
facs,   _ = silver_current(FACILITIES_SILVER)
perf,   _ = silver_current(CREDIT_PERFORMANCE_SILVER)
scores, _ = silver_current(SAS_SCORES_SILVER)

print(f"Applications:        {apps.count():,}")
print(f"Facilities:          {facs.count():,}")
print(f"Credit performance:  {perf.count():,}")
print(f"SAS scores:          {scores.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregate Credit Performance (24-month window per facility)

# COMMAND ----------

perf_agg = (
    perf
    .filter(F.col("months_since_funding") <= PERFORMANCE_WINDOW_MONTHS)
    .groupBy("facility_id")
    .agg(
        F.max("arrears_days").alias("max_arrears_days_24mo"),
        F.max(F.when(F.col("hardship_flag") == "Y", 1).otherwise(0)).alias("hardship_ever_24mo"),
        F.count("*").alias("perf_observations"),
        F.max("observation_date").alias("last_observed_date"),
    )
)

print(f"Aggregated facilities: {perf_agg.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Join everything on the application grain

# COMMAND ----------

# Facility carries application_id (FK), pull only relevant cols to avoid name clashes
fac_slim = facs.select(
    "application_id",
    "facility_id",
    "funded_date",
    "loan_amount_funded",
    "interest_rate",
    "loan_term_months",
    "repayment_frequency",
    "auto_debit_flag",
    "facility_status",
)

scores_slim = scores.select(
    "application_id",
    F.col("sas_final_p_good").alias("sas_final_p_good"),
    F.col("sas_pd_estimate").alias("sas_pd_estimate"),
    F.col("sas_decision").alias("sas_decision"),
    F.col("sas_model_version").alias("sas_model_version"),
)

joined = (
    apps.alias("a")
    .join(fac_slim.alias("f"), "application_id", "left")
    .join(perf_agg.alias("p"), "facility_id", "left")
    .join(scores_slim.alias("s"), "application_id", "left")
)

print(f"Joined rows: {joined.count():,} (should equal applications count)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Derive target_flag and sample_flag

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
    # DP3 month-end of application_date (per walkthrough)
    .withColumn("dp3_application", F.last_day(F.col("application_date")))
    .withColumn("_gold_built_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribution sanity check

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
# MAGIC ## 6. Write Gold

# COMMAND ----------

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

# Enable CDF — Gold is the source of truth for ML and downstream monitors
spark.sql(f"ALTER TABLE {GOLD_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

final_count = spark.table(GOLD_TABLE).count()

elapsed = (datetime.now() - start).total_seconds()
print(f"\n{'='*55}\nSILVER -> GOLD COMPLETE\n{'='*55}")
print(f"Output:  {GOLD_TABLE}")
print(f"Rows:    {final_count:,}")
print(f"Elapsed: {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|pl_application_scorecard_data|{final_count}|silver_to_gold|{elapsed:.1f}s")
