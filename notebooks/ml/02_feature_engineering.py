# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Feature Engineering
# MAGIC Credit Card Behaviour Scorecard
# MAGIC
# MAGIC 1. WoE/IV analysis per feature
# MAGIC 2. IV-based feature selection
# MAGIC 3. Write feature store (UC Feature Table with PK)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "asb_dev", "Unity Catalog")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME    = "cc_behaviour_scorecard"
TARGET        = "defaulted"
PRIMARY_KEY   = "customer_id"

INPUT_TABLE   = f"{catalog}.retail_gold.cc_scorecard_dev_data"
WOE_TABLE     = f"{catalog}.retail_gold.cc_woe_iv"
FEATURE_TABLE = f"{catalog}.retail_ml.cc_feature_store"

FEATURES = [
    "credit_score", "annual_income", "credit_utilization_ratio",
    "debt_to_income_ratio", "number_of_late_payments", "number_of_credit_lines",
    "tenure_in_years", "total_spend_last_year", "fraud_transactions", "age",
]
IV_EXCLUDE_BELOW = 0.02
IV_WEAK_BELOW    = 0.10
MAX_BINS         = 10

print(f"Model:    {MODEL_NAME}")
print(f"Input:    {INPUT_TABLE}")
print(f"Features: {len(FEATURES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Dev Population

# COMMAND ----------

start_time = datetime.now()

df = spark.table(INPUT_TABLE).filter(F.col("_population") == "dev")
print(f"Dev rows: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## WoE/IV Calculation

# COMMAND ----------

def calc_woe_iv(pdf, feature, target, max_bins=10):
    pdf = pdf.dropna(subset=[feature])
    try:
        pdf["_bin"] = pd.qcut(pdf[feature], q=max_bins, duplicates="drop")
    except ValueError:
        pdf["_bin"] = pd.cut(pdf[feature], bins=min(max_bins, pdf[feature].nunique()))

    grouped = pdf.groupby("_bin", observed=True)[target].agg(["sum", "count"])
    grouped.columns = ["bad_count", "total_count"]
    grouped["good_count"] = grouped["total_count"] - grouped["bad_count"]

    total_good = (pdf[target] == 0).sum()
    total_bad = (pdf[target] == 1).sum()

    grouped["good_dist"] = (grouped["good_count"] + 0.5) / (total_good + 0.5)
    grouped["bad_dist"] = (grouped["bad_count"] + 0.5) / (total_bad + 0.5)
    grouped["woe"] = np.log(grouped["good_dist"] / grouped["bad_dist"])
    grouped["iv"] = (grouped["good_dist"] - grouped["bad_dist"]) * grouped["woe"]

    grouped["feature"] = feature
    grouped["bin_label"] = grouped.index.astype(str)
    grouped["band"] = range(1, len(grouped) + 1)

    return (
        grouped[["feature", "band", "bin_label", "good_count", "bad_count",
                 "good_dist", "bad_dist", "woe", "iv"]].reset_index(drop=True),
        grouped["iv"].sum(),
    )

# COMMAND ----------

all_woe = []
iv_summary = []
selected = []

print(f"\n{'Feature':<30} {'IV':>10}  Status")
print("-" * 55)

for feat in FEATURES:
    if feat not in df.columns:
        print(f"{feat:<30} {'N/A':>10}  MISSING")
        continue

    pdf = df.select(feat, TARGET).toPandas()
    woe_df, iv = calc_woe_iv(pdf, feat, TARGET, max_bins=MAX_BINS)

    if iv < IV_EXCLUDE_BELOW:
        status = "EXCLUDED"
    elif iv < IV_WEAK_BELOW:
        status = "WEAK"
    else:
        status = "SELECTED"
        selected.append(feat)

    print(f"{feat:<30} {iv:>10.4f}  {status}")
    iv_summary.append({"feature": feat, "iv": round(iv, 4), "status": status})
    all_woe.append(woe_df)

print(f"\nSelected: {len(selected)} / {len(FEATURES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write WoE/IV Table

# COMMAND ----------

combined = pd.concat(all_woe, ignore_index=True)
(
    spark.createDataFrame(combined).write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(WOE_TABLE)
)
print(f"WoE/IV written: {WOE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Feature Store (UC Feature Table)

# COMMAND ----------

df_all = (
    spark.table(INPUT_TABLE)
    .withColumn("_selected_features", F.lit(",".join(selected)))
)

(
    df_all.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(FEATURE_TABLE)
)

# Register as UC Feature Table — adds PK so fe.log_model() can capture lineage
try:
    spark.sql(
        f"ALTER TABLE {FEATURE_TABLE} ALTER COLUMN {PRIMARY_KEY} SET NOT NULL"
    )
    spark.sql(
        f"ALTER TABLE {FEATURE_TABLE} DROP CONSTRAINT IF EXISTS pk_cc_feature"
    )
    spark.sql(
        f"ALTER TABLE {FEATURE_TABLE} ADD CONSTRAINT pk_cc_feature "
        f"PRIMARY KEY ({PRIMARY_KEY})"
    )
    print(f"UC Feature Table registered (PK={PRIMARY_KEY})")
except Exception as e:
    print(f"PK note: {type(e).__name__}: {str(e)[:120]}")

print(f"Feature store: {FEATURE_TABLE} ({df_all.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nFEATURE ENGINEERING COMPLETE\n{'='*50}")
print(f"Selected features ({len(selected)}): {', '.join(selected)}")
print(f"Elapsed: {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{len(selected)}_features|feature_eng|{elapsed:.1f}s")
