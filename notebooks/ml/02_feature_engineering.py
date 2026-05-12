# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Feature Engineering
# MAGIC PL Application Scorecard
# MAGIC
# MAGIC 1. Band continuous variables (quantile binning, max 10 bins)
# MAGIC 2. Group rare categorical levels (per ASB walkthrough — `PIE_*` examples)
# MAGIC 3. WoE / IV per banded variable on DEV (Funded) population
# MAGIC 4. Select features with IV >= IV_WEAK_BELOW
# MAGIC 5. Write feature store + WoE/IV reference tables

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %run ../utils/job_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog (bundle passes ${var.catalog})")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME    = "application_scorecard"
TARGET        = "target_flag"
PRIMARY_KEY   = "application_id"

INPUT_TABLE   = f"{catalog}.gold.pl_scorecard_dev_data"
WOE_TABLE     = f"{catalog}.pl_scorecard.woe_iv"
FEATURE_TABLE = f"{catalog}.pl_scorecard.feature_store"

# Continuous variables — banded via qcut (max 10 bins).
CONTINUOUS = [
    "credit_score_bureau",
    "annual_income",
    "age",
    "num_dependants",
    "time_at_address_months",
    "time_at_employer_months",
    "num_credit_enquiries",
    "existing_debt",
    "loan_amount_requested",
]

# Categorical (PIE_*-style) — rare-level grouping then WoE.
CATEGORICAL = [
    "pie_occupation",
    "pie_incomesource",
    "pie_accomodator",
    "marital_status",
    "region",
    "loan_purpose",
]

IV_SELECTED_THRESHOLD = 0.02   # IV >= 0.02 → SELECTED, otherwise excluded
IV_WEAK_BELOW         = 0.10   # IV in [0.02, 0.10) → SELECTED but flagged WEAK
MAX_BINS              = 10
MIN_LEVEL_PCT         = 0.05   # group categorical levels with < 5% share into "Other"

print(f"Model:    {MODEL_NAME}")
print(f"Input:    {INPUT_TABLE}")
print(f"Continuous features: {len(CONTINUOUS)}")
print(f"Categorical features: {len(CATEGORICAL)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Schema Exists + Load DEV (Funded only — target derived from performance)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.pl_scorecard")

start_time = datetime.now()

# Modelling sample = DEV population, restricted to Bad/Good (binary target).
# Indeterminate is excluded from feature selection (ambiguous outcomes).
df = (
    spark.table(INPUT_TABLE)
    .filter(F.col("_population") == "DEV")
    .filter(F.col(TARGET).isin("Bad", "Good"))
)
n_dev = df.count()
print(f"DEV (Bad+Good) rows: {n_dev:,}")

# Binary target column for IV calc: 1 = Bad, 0 = Good
df = df.withColumn("target_binary", F.when(F.col(TARGET) == "Bad", 1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## WoE/IV Calculation Helpers

# COMMAND ----------

def woe_iv_continuous(pdf, feature, target_col, max_bins=10):
    """Quantile-band a continuous feature, then compute WoE/IV per band.
    Cast to float so qcut/cut's internal np.isnan works on integer dtypes."""
    pdf = pdf.dropna(subset=[feature]).copy()
    pdf[feature] = pd.to_numeric(pdf[feature], errors="coerce").astype("float64")
    try:
        pdf["_bin"] = pd.qcut(pdf[feature], q=max_bins, duplicates="drop")
    except ValueError:
        pdf["_bin"] = pd.cut(pdf[feature], bins=min(max_bins, max(pdf[feature].nunique(), 2)))

    return _woe_iv_from_groups(pdf, feature, target_col, "_bin", "continuous")


def woe_iv_categorical(pdf, feature, target_col, min_level_pct=0.05):
    """Group rare levels (< min_level_pct of population) into 'Other', then WoE/IV."""
    pdf = pdf.dropna(subset=[feature]).copy()
    counts = pdf[feature].value_counts(normalize=True)
    rare = counts[counts < min_level_pct].index.tolist()
    pdf["_bin"] = pdf[feature].where(~pdf[feature].isin(rare), other="Other")

    return _woe_iv_from_groups(pdf, feature, target_col, "_bin", "categorical")


def _woe_iv_from_groups(pdf, feature, target_col, bin_col, var_kind):
    grouped = pdf.groupby(bin_col, observed=True)[target_col].agg(["sum", "count"])
    grouped.columns = ["bad_count", "total_count"]
    grouped["good_count"] = grouped["total_count"] - grouped["bad_count"]

    total_good = (pdf[target_col] == 0).sum()
    total_bad  = (pdf[target_col] == 1).sum()

    grouped["good_dist"] = (grouped["good_count"] + 0.5) / (total_good + 0.5)
    grouped["bad_dist"]  = (grouped["bad_count"]  + 0.5) / (total_bad  + 0.5)
    grouped["woe"] = np.log(grouped["good_dist"] / grouped["bad_dist"])
    grouped["iv"]  = (grouped["good_dist"] - grouped["bad_dist"]) * grouped["woe"]

    grouped["feature"]   = feature
    grouped["bin_label"] = grouped.index.astype(str)
    grouped["band"]      = range(1, len(grouped) + 1)
    grouped["var_kind"]  = var_kind

    return (
        grouped[["feature", "var_kind", "band", "bin_label", "good_count", "bad_count",
                 "good_dist", "bad_dist", "woe", "iv"]].reset_index(drop=True),
        grouped["iv"].sum(),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute IV per Feature

# COMMAND ----------

all_woe = []
iv_summary = []
selected = []

print(f"\n{'Feature':<30} {'Kind':<12} {'IV':>10}  Status")
print("-" * 70)

for feat in CONTINUOUS:
    if feat not in df.columns:
        print(f"{feat:<30} {'continuous':<12} {'N/A':>10}  MISSING")
        continue
    pdf = df.select(feat, "target_binary").toPandas()
    if pdf[feat].isnull().all():
        print(f"{feat:<30} {'continuous':<12} {'N/A':>10}  ALL_NULL")
        continue
    woe_df, iv = woe_iv_continuous(pdf, feat, "target_binary", max_bins=MAX_BINS)

    if iv < IV_SELECTED_THRESHOLD:
        status = "EXCLUDED"
    else:
        # Keep — flag weak signal for awareness but include in modelling
        status = "SELECTED_WEAK" if iv < IV_WEAK_BELOW else "SELECTED"
        selected.append(feat)
    print(f"{feat:<30} {'continuous':<12} {iv:>10.4f}  {status}")
    iv_summary.append({"feature": feat, "kind": "continuous", "iv": round(iv, 4), "status": status})
    all_woe.append(woe_df)

for feat in CATEGORICAL:
    if feat not in df.columns:
        print(f"{feat:<30} {'categorical':<12} {'N/A':>10}  MISSING")
        continue
    pdf = df.select(feat, "target_binary").toPandas()
    if pdf[feat].isnull().all():
        continue
    woe_df, iv = woe_iv_categorical(pdf, feat, "target_binary", min_level_pct=MIN_LEVEL_PCT)

    if iv < IV_SELECTED_THRESHOLD:
        status = "EXCLUDED"
    else:
        # Keep — flag weak signal for awareness but include in modelling
        status = "SELECTED_WEAK" if iv < IV_WEAK_BELOW else "SELECTED"
        selected.append(feat)
    print(f"{feat:<30} {'categorical':<12} {iv:>10.4f}  {status}")
    iv_summary.append({"feature": feat, "kind": "categorical", "iv": round(iv, 4), "status": status})
    all_woe.append(woe_df)

print(f"\nSelected: {len(selected)} / {len(CONTINUOUS) + len(CATEGORICAL)}")
print(f"  -> {selected}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write WoE/IV Table (audit trail)

# COMMAND ----------

if all_woe:
    combined = pd.concat(all_woe, ignore_index=True)
    (
        spark.createDataFrame(combined).write
        .format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(WOE_TABLE)
    )
    enable_iceberg_uniform(WOE_TABLE)
    print(f"WoE/IV written: {WOE_TABLE}  ({len(combined):,} bands)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Feature Store (full population — TTD)
# MAGIC
# MAGIC Feature store carries the full TTD population (30K) so reject inference in 03
# MAGIC can score declined applicants with the same features as the funded ones.
# MAGIC `_selected_features` column lists the features the model should use.

# COMMAND ----------

# Use TTD-wide dataset (all populations) so reject inference can score everyone
df_all = (
    spark.table(INPUT_TABLE)
    .withColumn("_selected_features", F.lit(",".join(selected)))
)

(
    df_all.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(FEATURE_TABLE)
)

enable_iceberg_uniform(FEATURE_TABLE)

# Register UC Feature Table — adds PK so fe.log_model() can capture lineage
try:
    spark.sql(f"ALTER TABLE {FEATURE_TABLE} ALTER COLUMN {PRIMARY_KEY} SET NOT NULL")
    spark.sql(f"ALTER TABLE {FEATURE_TABLE} DROP CONSTRAINT IF EXISTS pk_pl_feature")
    spark.sql(f"ALTER TABLE {FEATURE_TABLE} ADD CONSTRAINT pk_pl_feature PRIMARY KEY ({PRIMARY_KEY})")
    print(f"UC Feature Table registered (PK={PRIMARY_KEY})")
except Exception as e:
    print(f"PK note: {type(e).__name__}: {str(e)[:120]}")

print(f"Feature store: {FEATURE_TABLE} ({df_all.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*55}\nFEATURE ENGINEERING COMPLETE\n{'='*55}")
print(f"Selected ({len(selected)}): {', '.join(selected)}")
print(f"Elapsed: {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{len(selected)}_features|feature_eng|{elapsed:.1f}s")
