# Databricks notebook source

# MAGIC %md
# MAGIC # 07 - Model Scoring
# MAGIC PL Application Scorecard — Batch Inference
# MAGIC
# MAGIC 1. Load `@Champion` from Unity Catalog
# MAGIC 2. Score the **full TTD population** (funded + rejected)
# MAGIC 3. Output per ASB walkthrough:
# MAGIC    - `final_p_good` — model output (probability of Good)
# MAGIC    - `goodbadflag_inferred` — Good / Bad classification at PD threshold
# MAGIC    - `credit_score` — scorecard-scaled (target 600 @ 50:1 odds, doubling every 20 points)
# MAGIC    - `risk_grade` — A1 / A2 / B1 / B2 / C1 / C2 / D
# MAGIC 4. Write scored output with CDF enabled (Lakehouse Monitoring readiness)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow
import numpy as np
import pandas as pd

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
PRIMARY_KEY   = "application_id"

FEATURE_TABLE = f"{catalog}.pl_scorecard.feature_store"
OUTPUT_TABLE  = f"{catalog}.pl_scorecard.scored_output"
UC_MODEL      = f"{catalog}.pl_scorecard.application_scorecard"

# Scorecard scaling: target 600 @ 50:1 odds, doubling every 20 points
TARGET_SCORE = 600
TARGET_ODDS  = 50
PDO          = 20

# Inferred Good/Bad threshold on Final_p_good.
# Set to 0.95 because the synthetic data's bureau-dominated signal produces
# avg p_good ~ 0.99 for Funded; a 0.55 threshold classifies everyone Good.
# 0.95 = "predict Bad if PD >= 5%" — matches the base bad rate and
# produces both classes in the output (essential for monitor metrics).
GOOD_THRESHOLD = 0.95

RISK_GRADES = [
    ("A1", 750, 999), ("A2", 700, 749), ("B1", 650, 699), ("B2", 600, 649),
    ("C1", 550, 599), ("C2", 500, 549), ("D",    0, 499),
]

print(f"Model:  {MODEL_NAME}")
print(f"Output: {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load @Champion

# COMMAND ----------

start_time = datetime.now()
mlflow.set_registry_uri("databricks-uc")
model_uri = f"models:/{UC_MODEL}@Champion"
loaded_model = mlflow.sklearn.load_model(model_uri)

client = mlflow.tracking.MlflowClient()
champion_version = client.get_model_version_by_alias(UC_MODEL, "Champion").version
print(f"Loaded @Champion v{champion_version}: {type(loaded_model).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Full TTD Population

# COMMAND ----------

df = spark.table(FEATURE_TABLE)
selected_features = [
    f.strip() for f in df.select("_selected_features").first()[0].split(",") if f.strip()
]

keep = [PRIMARY_KEY, "customer_id", "application_date", "dp3_application",
        "application_decision", "sample_flag", "target_flag",
        "sas_final_p_good", "sas_decision",
        "_population"] + selected_features

cols_present = [c for c in keep if c in df.columns]
pdf = df.select(*cols_present).toPandas()

X = pdf[selected_features].copy()
for c in X.columns:
    if X[c].dtype == "object":
        X[c] = X[c].astype("category").cat.codes
X = X.fillna(0)

print(f"Scoring {len(X):,} rows  |  {len(selected_features)} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predict + Standardise Outputs

# COMMAND ----------

p_bad  = loaded_model.predict_proba(X)[:, 1]
p_good = 1 - p_bad

pdf["pd_estimate"]  = p_bad
pdf["final_p_good"] = p_good

# Goodbadflag_inferred — per walkthrough, threshold-driven decision
pdf["goodbadflag_inferred"] = np.where(p_good >= GOOD_THRESHOLD, "Good", "Bad")

# Scorecard scaling
factor = PDO / np.log(2)
offset = TARGET_SCORE - factor * np.log(TARGET_ODDS)
odds   = np.where(p_bad > 0, p_good / np.clip(p_bad, 1e-10, 1), TARGET_ODDS * 10)
pdf["credit_score"] = np.round(offset + factor * np.log(odds)).astype(int).clip(0, 999)


def grade(score):
    for g, lo, hi in RISK_GRADES:
        if lo <= score <= hi:
            return g
    return "D"

pdf["risk_grade"] = pdf["credit_score"].apply(grade)

print(f"\nScore distribution:")
print(f"  mean={pdf['credit_score'].mean():.0f}  median={pdf['credit_score'].median():.0f}  range=[{pdf['credit_score'].min()}, {pdf['credit_score'].max()}]")

print("\nRisk grade x sample_flag:")
print(pdf.groupby(["sample_flag", "risk_grade"]).size().to_string())

print("\ngoodbadflag_inferred x application_decision:")
print(pdf.groupby(["application_decision", "goodbadflag_inferred"]).size().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Scored Output

# COMMAND ----------

# Observed label for monitoring — Good/Bad ONLY for Funded applications with
# observed performance. NULL for Rejected (never funded -> no ground truth)
# and Indeterminate (ambiguous outcome).
# This is the canonical "label arrives later" pattern for credit scoring monitors.
pdf["_observed_label"] = np.where(
    (pdf["sample_flag"] == "Funded") & pdf["target_flag"].isin(["Good", "Bad"]),
    pdf["target_flag"],
    None,
)

pdf["_scored_at"]      = pd.Timestamp.now()
pdf["_model_name"]     = MODEL_NAME
pdf["_model_uri"]      = model_uri
pdf["_model_version"]  = int(champion_version)

scored_df = spark.createDataFrame(pdf)
(
    scored_df.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

# Iceberg UniForm so Snowflake can read this, plus CDF for Lakehouse Monitoring
enable_iceberg_uniform(OUTPUT_TABLE, extra_props={"delta.enableChangeDataFeed": "true"})

final_count = spark.table(OUTPUT_TABLE).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*55}\nSCORING COMPLETE\n{'='*55}")
print(f"Model:        {model_uri}  (v{champion_version})")
print(f"Rows scored:  {final_count:,}")
print(f"Score range:  {int(pdf['credit_score'].min())} - {int(pdf['credit_score'].max())}")
print(f"Mean p_good:  {pdf['final_p_good'].mean():.4f}")
print(f"Output:       {OUTPUT_TABLE}")
print(f"Elapsed:      {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{final_count}|scoring|{elapsed:.1f}s")
