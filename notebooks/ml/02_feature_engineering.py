# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Feature Engineering
# MAGIC **Config-driven WoE/IV analysis, variable banding, and feature selection.**
# MAGIC
# MAGIC Reads `features.yaml` and produces:
# MAGIC 1. WoE/IV table per feature
# MAGIC 2. Banding lookup table
# MAGIC 3. Transformed feature store (WoE-encoded)
# MAGIC
# MAGIC **SAS Equivalent:** PROC FORMAT + %macro CREATE_RISKGRADE_SET + WoE/IV macros

# COMMAND ----------

# MAGIC %run ./00_ml_config_loader

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime
import numpy as np
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("model_name", "", "Model name from registry")
dbutils.widgets.text("catalog", "asb_dev", "Unity Catalog name")

model_name = dbutils.widgets.get("model_name").strip()
catalog = dbutils.widgets.get("catalog").strip()

config = load_model_config(model_name, catalog)
model_cfg = config["model"]
feat_cfg = config["features"]
prep_cfg = config["data_prep"]

target_col = model_cfg["target_variable"]
input_table = prep_cfg["output_table"]

print(f"Model:  {model_name}")
print(f"Input:  {input_table}")
print(f"Target: {target_col}")
print(f"Features: {len(feat_cfg['features'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Development Data

# COMMAND ----------

start_time = datetime.now()

df = spark.table(input_table).filter(F.col("_population") == "dev")
total = df.count()
bad_total = df.filter(F.col(target_col) == 1).count()
good_total = total - bad_total

print(f"Dev population: {total:,} rows (Good: {good_total:,}, Bad: {bad_total:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: WoE/IV Calculation

# COMMAND ----------

def calculate_woe_iv(df, feature_name, target_col, bins=None, feature_type="continuous",
                     max_bins=10, min_bin_pct=0.05):
    """
    Calculate Weight of Evidence and Information Value for a feature.

    Returns a Spark DataFrame with columns:
      feature, band, bin_label, good_count, bad_count, good_dist, bad_dist, woe, iv
    """
    pdf = df.select(feature_name, target_col).toPandas()
    pdf = pdf.dropna(subset=[feature_name])

    total_good = (pdf[target_col] == 0).sum()
    total_bad = (pdf[target_col] == 1).sum()

    if feature_type == "continuous" and bins is None:
        # Auto-bin using quantiles
        min_obs = max(int(len(pdf) * min_bin_pct), 1)
        try:
            pdf["_bin"] = pd.qcut(pdf[feature_name], q=max_bins, duplicates="drop")
        except ValueError:
            pdf["_bin"] = pd.cut(pdf[feature_name], bins=min(max_bins, pdf[feature_name].nunique()))
    elif feature_type == "categorical" or bins is not None:
        pdf["_bin"] = pdf[feature_name].astype(str)
    else:
        pdf["_bin"] = pdf[feature_name].astype(str)

    grouped = pdf.groupby("_bin")[target_col].agg(["sum", "count"])
    grouped.columns = ["bad_count", "total_count"]
    grouped["good_count"] = grouped["total_count"] - grouped["bad_count"]

    # Avoid division by zero with Laplace smoothing
    grouped["good_dist"] = (grouped["good_count"] + 0.5) / (total_good + 0.5)
    grouped["bad_dist"] = (grouped["bad_count"] + 0.5) / (total_bad + 0.5)

    grouped["woe"] = np.log(grouped["good_dist"] / grouped["bad_dist"])
    grouped["iv"] = (grouped["good_dist"] - grouped["bad_dist"]) * grouped["woe"]

    grouped["feature"] = feature_name
    grouped["bin_label"] = grouped.index.astype(str)
    grouped["band"] = range(1, len(grouped) + 1)

    total_iv = grouped["iv"].sum()

    result = grouped[["feature", "band", "bin_label", "good_count", "bad_count",
                       "good_dist", "bad_dist", "woe", "iv"]].reset_index(drop=True)

    return result, total_iv

# COMMAND ----------

import pandas as pd

iv_thresholds = feat_cfg["iv_thresholds"]
auto_params = feat_cfg.get("auto_banding", {})
max_bins = auto_params.get("max_bins", 10)
min_bin_pct = auto_params.get("min_bin_size", 0.05)

all_woe_results = []
iv_summary = []
selected_features = []

print(f"\n{'Feature':<25} {'Type':<15} {'IV':>10} {'Status':<15}")
print("-" * 65)

for feat_spec in feat_cfg["features"]:
    fname = feat_spec["name"]
    ftype = feat_spec["type"]

    if fname not in df.columns:
        print(f"{fname:<25} {'MISSING':<15} {'N/A':>10} {'SKIPPED':<15}")
        continue

    # Calculate WoE/IV
    woe_df, total_iv = calculate_woe_iv(
        df, fname, target_col,
        feature_type=ftype,
        max_bins=max_bins,
        min_bin_pct=min_bin_pct,
    )

    # Determine status based on IV thresholds
    if total_iv < iv_thresholds["exclude_below"]:
        status = "EXCLUDED"
    elif total_iv > iv_thresholds["suspicious_above"]:
        status = "SUSPICIOUS"
    elif total_iv < iv_thresholds["weak_below"]:
        status = "WEAK"
    else:
        status = "SELECTED"
        selected_features.append(fname)

    print(f"{fname:<25} {ftype:<15} {total_iv:>10.4f} {status:<15}")

    iv_summary.append({"feature": fname, "type": ftype, "iv": total_iv, "status": status})
    all_woe_results.append(woe_df)

print(f"\nSelected features: {len(selected_features)} / {len(feat_cfg['features'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save WoE/IV Table

# COMMAND ----------

# Combine all WoE results into one DataFrame
if all_woe_results:
    combined_woe = pd.concat(all_woe_results, ignore_index=True)
    woe_spark_df = spark.createDataFrame(combined_woe)

    woe_table = feat_cfg["woe_iv_table"]
    (
        woe_spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(woe_table)
    )
    print(f"WoE/IV table written: {woe_table} ({len(combined_woe)} rows)")

# IV summary table
iv_spark_df = spark.createDataFrame(pd.DataFrame(iv_summary))
iv_spark_df.orderBy(F.col("iv").desc()).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build Banding Lookup Table

# COMMAND ----------

# The banding lookup is a subset of the WoE table with band definitions
banding_table = feat_cfg["banding_table"]

if all_woe_results:
    banding_df = (
        woe_spark_df
        .select("feature", "band", "bin_label", "woe")
        .withColumn("_created_at", F.current_timestamp())
        .withColumn("_model_name", F.lit(model_name))
    )

    (
        banding_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(banding_table)
    )
    print(f"Banding lookup written: {banding_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Transform Features (WoE Encoding)

# COMMAND ----------

# Load full dataset (all populations) and apply WoE encoding
df_all = spark.table(input_table)

# For each selected feature, join the WoE lookup to encode
# For now, keep raw features + add WoE columns for selected features
df_encoded = df_all

if all_woe_results:
    woe_lookup = spark.table(woe_table)

    for fname in selected_features:
        feat_woe = woe_lookup.filter(F.col("feature") == fname).select(
            F.col("bin_label"),
            F.col("woe").alias(f"{fname}_woe")
        )
        # For continuous features, WoE encoding happens at scoring time via banding
        # For now, store the raw feature + flag it as selected

    # Add selected feature list as metadata
    df_encoded = df_encoded.withColumn(
        "_selected_features",
        F.lit(",".join(selected_features))
    )

# Write feature store
feature_table = feat_cfg["feature_store_table"]
(
    df_encoded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(feature_table)
)

print(f"Feature store written: {feature_table} ({df_encoded.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()

print(f"\n{'='*60}")
print(f"FEATURE ENGINEERING COMPLETE: {model_name}")
print(f"{'='*60}")
print(f"  Features analysed:  {len(feat_cfg['features'])}")
print(f"  Features selected:  {len(selected_features)}")
print(f"  Selected: {', '.join(selected_features)}")
print(f"  WoE/IV table:       {feat_cfg['woe_iv_table']}")
print(f"  Banding table:      {feat_cfg['banding_table']}")
print(f"  Feature store:      {feat_cfg['feature_store_table']}")
print(f"  Elapsed:            {elapsed:.1f}s")

# COMMAND ----------

result = f"SUCCESS|{model_name}|{len(selected_features)}_features|feature_eng|{elapsed:.1f}s"
print(result)
dbutils.notebook.exit(result)
