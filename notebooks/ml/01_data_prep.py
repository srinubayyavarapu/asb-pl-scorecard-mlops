# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Data Preparation
# MAGIC Credit Card Behaviour Scorecard — PD model
# MAGIC
# MAGIC 1. Load Silver table
# MAGIC 2. Apply target definition
# MAGIC 3. Stratified 70/30 Dev / Holdout split
# MAGIC 4. Write ML-ready dataset to Gold

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config (inline — one place, easy to change)

# COMMAND ----------

dbutils.widgets.text("catalog", "asb_dev", "Unity Catalog")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME   = "cc_behaviour_scorecard"
TARGET       = "defaulted"
PRIMARY_KEY  = "customer_id"

SOURCE_TABLE = f"{catalog}.retail_silver.cc_customer_data"
OUTPUT_TABLE = f"{catalog}.retail_gold.cc_scorecard_dev_data"

DEV_RATIO    = 0.70
SEED         = 42

print(f"Model:   {MODEL_NAME}")
print(f"Source:  {SOURCE_TABLE}")
print(f"Output:  {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver

# COMMAND ----------

start_time = datetime.now()

df = spark.table(SOURCE_TABLE)

if "_is_current" in df.columns:
    df = df.filter(F.col("_is_current") == True)

meta_cols = [c for c in df.columns if c.startswith("_")]
if meta_cols:
    df = df.drop(*meta_cols)

print(f"Loaded: {df.count():,} rows, {len(df.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target + Good/Bad Distribution

# COMMAND ----------

df = df.withColumn(TARGET, F.col(TARGET).cast("integer"))
#this is dataframe
total = df.count()
bad = df.filter(F.col(TARGET) == 1).count()
good = total - bad
print(f"Good: {good:,} ({good/total:.2%})    Bad: {bad:,} ({bad/total:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stratified Dev / Holdout Split

# COMMAND ----------

fractions = {row[0]: DEV_RATIO for row in df.select(TARGET).distinct().collect()}

df_dev = df.sampleBy(TARGET, fractions=fractions, seed=SEED)
df_holdout = df.subtract(df_dev)

df_dev = df_dev.withColumn("_population", F.lit("dev"))
df_holdout = df_holdout.withColumn("_population", F.lit("holdout"))

df_final = (
    df_dev.unionByName(df_holdout)
    .withColumn("_ml_prepared_at", F.current_timestamp())
    .withColumn("_model_name", F.lit(MODEL_NAME))
)

print(f"Dev:     {df_dev.count():,}")
print(f"Holdout: {df_holdout.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Table

# COMMAND ----------

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

final_count = spark.table(OUTPUT_TABLE).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = (
    spark.table(OUTPUT_TABLE).groupBy("_population")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col(TARGET).cast("int")).alias("bad_count"),
        F.round(F.mean(F.col(TARGET).cast("int")), 4).alias("bad_rate"),
    ).orderBy("_population")
)
summary.show()

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nDATA PREP COMPLETE\n{'='*50}")
print(f"Output:  {OUTPUT_TABLE}")
print(f"Rows:    {final_count:,}")
print(f"Elapsed: {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{final_count}|data_prep|{elapsed:.1f}s")
