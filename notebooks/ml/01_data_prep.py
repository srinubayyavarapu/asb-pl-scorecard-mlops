# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Data Preparation
# MAGIC PL Application Scorecard
# MAGIC
# MAGIC 1. Load Gold table (`pl_application_scorecard_data`)
# MAGIC 2. Apply 4-way population split per ASB walkthrough:
# MAGIC    - **DEV**: model fitting (Funded, earlier dates)
# MAGIC    - **HOL**: hold-out validation (Funded, earlier dates)
# MAGIC    - **OOT**: out-of-time validation (Funded, latest 4 months)
# MAGIC    - **TTD**: through-the-door (full population — used for reject inference)
# MAGIC 3. Write ML-ready dataset to Gold

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

MODEL_NAME   = "application_scorecard"
TARGET       = "target_flag"           # Good / Bad / Indeterminate / Rejected
PRIMARY_KEY  = "application_id"

SOURCE_TABLE = f"{catalog}.gold.pl_application_scorecard_data"
OUTPUT_TABLE = f"{catalog}.gold.pl_scorecard_dev_data"

# Split ratios applied to FUNDED population (Bad/Good/Indeterminate only).
# Rejected applicants (5K) are routed to TTD only — they get inferred labels in training.
DEV_RATIO    = 0.70
HOL_RATIO    = 0.20
# OOT = remainder, but rather than ratio-based we cut by date:
# the most recent OOT_MONTHS of applications form OOT, the rest split DEV/HOL.
OOT_MONTHS   = 4
SEED         = 42

print(f"Model:   {MODEL_NAME}")
print(f"Source:  {SOURCE_TABLE}")
print(f"Output:  {OUTPUT_TABLE}")
print( f"Seed:    {SEED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold

# COMMAND ----------

start_time = datetime.now()
df = spark.table(SOURCE_TABLE)

print(f"Loaded: {df.count():,} rows, {len(df.columns)} cols")

# Target / sample distribution from Gold
print("\ntarget_flag x sample_flag (input):")
df.groupBy(TARGET, "sample_flag").count().orderBy(TARGET, "sample_flag").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population Split
# MAGIC
# MAGIC - Identify the OOT cutoff: latest `OOT_MONTHS` of application_date.
# MAGIC - Funded applications before the cutoff -> DEV (70%) + HOL (20%) stratified by target_flag
# MAGIC   - 10% remainder of pre-cutoff funded blends into OOT to balance the date-only OOT.
# MAGIC   - In practice, the date cut alone gives ~OOT 17%, which is fine.
# MAGIC - Funded applications on/after cutoff -> OOT
# MAGIC - Rejected (NotFunded) -> TTD only (no Funded outcome to learn from directly)

# COMMAND ----------

max_date = df.agg(F.max("application_date")).collect()[0][0]
print(f"Max application_date in Gold: {max_date}")

# OOT cutoff: latest OOT_MONTHS-month window from max_date
oot_cutoff = F.add_months(F.lit(max_date), -OOT_MONTHS)
print(f"OOT cutoff: anything >= max_date - {OOT_MONTHS} months")

# Step 1: tag every Funded application with date-based slice intent
funded = df.filter(F.col("sample_flag") == "Funded")
not_funded = df.filter(F.col("sample_flag") == "NotFunded")

# Funded by date partition
funded_pre  = funded.filter(F.col("application_date") <  oot_cutoff)
funded_post = funded.filter(F.col("application_date") >= oot_cutoff)

print(f"\nFunded pre-cutoff:  {funded_pre.count():,}  (split into DEV + HOL)")
print(f"Funded post-cutoff: {funded_post.count():,}  (-> OOT)")
print(f"NotFunded (TTD-only): {not_funded.count():,}")

# Step 2: stratified DEV/HOL split on funded_pre, stratified by target_flag
fractions_dev = {row[0]: DEV_RATIO for row in funded_pre.select(TARGET).distinct().collect()}
df_dev = funded_pre.sampleBy(TARGET, fractions=fractions_dev, seed=SEED)
df_hol = funded_pre.subtract(df_dev)

# Tag populations
df_dev = df_dev.withColumn("_population", F.lit("DEV"))
df_hol = df_hol.withColumn("_population", F.lit("HOL"))
df_oot = funded_post.withColumn("_population", F.lit("OOT"))
df_ttd_only = not_funded.withColumn("_population", F.lit("TTD"))

# All four splits unioned
df_final = (
    df_dev.unionByName(df_hol)
          .unionByName(df_oot)
          .unionByName(df_ttd_only)
          .withColumn("_ml_prepared_at", F.current_timestamp())
          .withColumn("_model_name", F.lit(MODEL_NAME))
)

print("\nFinal split:")
df_final.groupBy("_population", TARGET).count() \
        .orderBy("_population", TARGET).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold ML-ready dataset

# COMMAND ----------

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

enable_iceberg_uniform(OUTPUT_TABLE)

final_count = spark.table(OUTPUT_TABLE).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\nPopulation x target distribution:")
spark.table(OUTPUT_TABLE).groupBy("_population", TARGET) \
     .agg(F.count("*").alias("rows")) \
     .orderBy("_population", TARGET).show()

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*55}\nDATA PREP COMPLETE\n{'='*55}")
print(f"Output:  {OUTPUT_TABLE}")
print(f"Rows:    {final_count:,}")
print(f"Elapsed: {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{final_count}|data_prep|{elapsed:.1f}s")
