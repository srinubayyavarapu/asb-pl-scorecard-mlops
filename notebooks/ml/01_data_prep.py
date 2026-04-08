# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - ML Data Preparation
# MAGIC **Config-driven data preparation for scorecard development.**
# MAGIC
# MAGIC Reads `data_prep.yaml` for the specified model and executes:
# MAGIC 1. Load source tables from Silver layer
# MAGIC 2. Join tables as specified in config
# MAGIC 3. Apply good/bad definition
# MAGIC 4. Apply exclusions
# MAGIC 5. Stratified sampling (Dev / Holdout / Out-of-Time)
# MAGIC 6. Write ML-ready dataset to Gold layer
# MAGIC
# MAGIC **SAS Equivalent:** LIBNAME + PROC SQL joins + DATA step filtering + PROC SURVEYSELECT

# COMMAND ----------

import os as _os
_nb_dir = _os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
exec(open(f"/Workspace{_nb_dir}/00_ml_config_loader.py").read())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("model_name", "", "Model name from registry")
dbutils.widgets.text("catalog", "asb_dev", "Unity Catalog name")

model_name = dbutils.widgets.get("model_name").strip()
catalog = dbutils.widgets.get("catalog").strip()

if not model_name:
    dbutils.notebook.exit("ERROR: model_name parameter is required")

config = load_model_config(model_name, catalog)
model_cfg = config["model"]
prep_cfg = config["data_prep"]

print(f"Model:  {model_cfg['description']}")
print(f"Target: {model_cfg['target_variable']}")
print(f"Key:    {model_cfg['primary_key']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Source Tables

# COMMAND ----------

start_time = datetime.now()

source_dfs = {}
for alias, table_fqn in prep_cfg["source_tables"].items():
    print(f"Loading {alias}: {table_fqn}")
    df = spark.table(table_fqn)

    # If Silver has SCD columns, filter to current records only
    if "_is_current" in df.columns:
        df = df.filter(F.col("_is_current") == True)

    # Drop metadata columns (start with _)
    meta_cols = [c for c in df.columns if c.startswith("_")]
    if meta_cols:
        df = df.drop(*meta_cols)

    source_dfs[alias] = df
    print(f"  {alias}: {df.count():,} rows, {len(df.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join Tables

# COMMAND ----------

join_key = model_cfg["primary_key"]
result_df = source_dfs.get("base")

for join_spec in prep_cfg.get("joins", []):
    left_alias = join_spec["left"]
    right_alias = join_spec["right"]
    join_on = join_spec["on"]
    join_how = join_spec["how"]

    left_df = result_df if left_alias == "_result" else source_dfs[left_alias]
    right_df = source_dfs[right_alias]

    # Avoid duplicate columns: drop join key from right if it exists in left
    right_cols_to_drop = [c for c in right_df.columns if c in left_df.columns and c != join_on]
    if right_cols_to_drop:
        right_df = right_df.drop(*right_cols_to_drop)

    result_df = left_df.join(right_df, on=join_on, how=join_how)
    print(f"  JOIN {left_alias} {join_how} {right_alias} ON {join_on} => {result_df.count():,} rows")

print(f"\nAfter joins: {result_df.count():,} rows, {len(result_df.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply Good/Bad Definition & Exclusions

# COMMAND ----------

gbd = prep_cfg["good_bad_definition"]
target_col = gbd["target_column"]

# Apply observation window
obs_window = gbd.get("observation_window", {})
if obs_window:
    # Find a date column to filter on (watermark/observation date)
    date_cols = [c for c in result_df.columns if "date" in c.lower() and "observation" in c.lower()]
    if date_cols:
        obs_col = date_cols[0]
        result_df = result_df.filter(
            (F.col(obs_col) >= obs_window["start"]) &
            (F.col(obs_col) <= obs_window["end"])
        )
        print(f"Observation window ({obs_col}): {obs_window['start']} to {obs_window['end']} => {result_df.count():,} rows")

# Apply exclusions
for exclusion in gbd.get("exclusions", []):
    before = result_df.count()
    result_df = result_df.filter(f"NOT ({exclusion})")
    after = result_df.count()
    print(f"Exclusion: {exclusion} => removed {before - after:,} rows")

# Ensure target column exists and is binary
if target_col in result_df.columns:
    result_df = result_df.withColumn(target_col, F.col(target_col).cast("integer"))
else:
    # Derive from bad_condition
    bad_cond = gbd["bad_condition"]
    result_df = result_df.withColumn(target_col, F.expr(f"CASE WHEN {bad_cond} THEN 1 ELSE 0 END"))

# Print good/bad distribution
total = result_df.count()
bad_count = result_df.filter(F.col(target_col) == 1).count()
good_count = total - bad_count
bad_rate = bad_count / total if total > 0 else 0

print(f"\nGood/Bad Distribution:")
print(f"  Total:    {total:,}")
print(f"  Good:     {good_count:,} ({1 - bad_rate:.2%})")
print(f"  Bad:      {bad_count:,} ({bad_rate:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Stratified Sampling

# COMMAND ----------

sampling = prep_cfg["sampling"]
dev_ratio = sampling["dev_ratio"]
holdout_ratio = sampling["holdout_ratio"]
stratify_col = sampling["stratify_by"]
seed = sampling["random_seed"]
oot_date = sampling.get("oot_start_date")

# Out-of-Time split (if configured)
df_oot = None
df_in_time = result_df

if oot_date:
    date_cols = [c for c in result_df.columns if "date" in c.lower() and "observation" in c.lower()]
    if date_cols:
        obs_col = date_cols[0]
        df_oot = result_df.filter(F.col(obs_col) >= oot_date)
        df_in_time = result_df.filter(F.col(obs_col) < oot_date)
        print(f"Out-of-Time split ({obs_col} >= {oot_date}):")
        print(f"  In-time: {df_in_time.count():,}")
        print(f"  OOT:     {df_oot.count():,}")

# Stratified Dev/Holdout split on in-time data
distinct_vals = [row[0] for row in df_in_time.select(stratify_col).distinct().collect()]
dev_fractions = {val: dev_ratio for val in distinct_vals}

df_dev = df_in_time.sampleBy(stratify_col, fractions=dev_fractions, seed=seed)
df_holdout = df_in_time.subtract(df_dev)

# Add population label
df_dev = df_dev.withColumn("_population", F.lit("dev"))
df_holdout = df_holdout.withColumn("_population", F.lit("holdout"))

if df_oot is not None:
    df_oot = df_oot.withColumn("_population", F.lit("oot"))
    df_final = df_dev.unionByName(df_holdout).unionByName(df_oot)
else:
    df_final = df_dev.unionByName(df_holdout)

# Add audit columns
df_final = (
    df_final
    .withColumn("_ml_prepared_at", F.current_timestamp())
    .withColumn("_model_name", F.lit(model_name))
)

print(f"\nSampling Results:")
print(f"  Dev:     {df_dev.count():,} ({dev_ratio:.0%})")
print(f"  Holdout: {df_holdout.count():,} ({holdout_ratio:.0%})")
if df_oot is not None:
    print(f"  OOT:     {df_oot.count():,}")
print(f"  Total:   {df_final.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Gold Layer

# COMMAND ----------

output_table = prep_cfg["output_table"]

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)

final_count = spark.table(output_table).count()
print(f"\nWritten to: {output_table}")
print(f"Rows: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Data Quality Summary

# COMMAND ----------

df_check = spark.table(output_table)

# Per-population summary
print(f"\n{'='*60}")
print(f"DATA PREP COMPLETE: {model_name}")
print(f"{'='*60}")

pop_summary = (
    df_check.groupBy("_population")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col(target_col).cast("int")).alias("bad_count"),
        F.round(F.mean(F.col(target_col).cast("int")), 4).alias("bad_rate"),
    )
    .orderBy("_population")
)
pop_summary.show()

# Null check on feature columns
feature_cols = [c for c in df_check.columns if not c.startswith("_")]
null_cols = []
for c in feature_cols:
    nc = df_check.filter(F.col(c).isNull()).count()
    if nc > 0:
        null_cols.append((c, nc))

if null_cols:
    print(f"Columns with nulls ({len(null_cols)}):")
    for col_name, count in null_cols:
        print(f"  {col_name}: {count:,} nulls")
else:
    print("No null values in feature columns")

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\nElapsed: {elapsed:.1f}s")

# COMMAND ----------

result = f"SUCCESS|{model_name}|{final_count}|data_prep|{elapsed:.1f}s"
print(result)
dbutils.notebook.exit(result)
