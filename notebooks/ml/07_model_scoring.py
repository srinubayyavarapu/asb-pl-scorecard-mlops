# Databricks notebook source

# MAGIC %md
# MAGIC # 07 - Model Scoring
# MAGIC Credit Card Behaviour Scorecard — Batch Inference
# MAGIC
# MAGIC 1. Load `@Champion` from Unity Catalog
# MAGIC 2. Score the population
# MAGIC 3. Apply scorecard scaling (PD → credit score)
# MAGIC 4. Assign risk grades
# MAGIC 5. Write scored output with CDF enabled (for Lakehouse Monitoring)

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow
import numpy as np
import pandas as pd

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

FEATURE_TABLE = f"{catalog}.retail_ml.cc_feature_store"
OUTPUT_TABLE  = f"{catalog}.retail_gold.cc_scored_output"
UC_MODEL      = f"{catalog}.retail_ml.cc_behaviour_scorecard"

# Scorecard scaling: target score 600 @ odds 50:1, doubling every 20 points
TARGET_SCORE = 600
TARGET_ODDS  = 50
PDO          = 20

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
# MAGIC ## Load Scoring Data

# COMMAND ----------

df = spark.table(FEATURE_TABLE)
feature_cols = [f.strip() for f in df.select("_selected_features").first()[0].split(",") if f.strip()]

keep = [PRIMARY_KEY] + feature_cols + [TARGET, "_population"]
pdf = df.select(*[c for c in keep if c in df.columns]).toPandas()

X = pdf[feature_cols].fillna(0)
for c in X.columns:
    if X[c].dtype == "object":
        X[c] = X[c].astype("category").cat.codes

print(f"Scoring {len(X):,} rows  |  {len(feature_cols)} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score + Scorecard Scaling

# COMMAND ----------

y_prob = loaded_model.predict_proba(X)[:, 1]
pdf["pd_estimate"] = y_prob

factor = PDO / np.log(2)
offset = TARGET_SCORE - factor * np.log(TARGET_ODDS)
odds = np.where(y_prob > 0, (1 - y_prob) / np.clip(y_prob, 1e-10, 1), TARGET_ODDS * 10)
pdf["credit_score"] = np.round(offset + factor * np.log(odds)).astype(int).clip(0, 999)

print(f"Score: mean={pdf['credit_score'].mean():.0f}  median={pdf['credit_score'].median():.0f}  range=[{pdf['credit_score'].min()}, {pdf['credit_score'].max()}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assign Risk Grades

# COMMAND ----------

def grade(score):
    for g, lo, hi in RISK_GRADES:
        if lo <= score <= hi:
            return g
    return "D"

pdf["risk_grade"] = pdf["credit_score"].apply(grade)

print("\nRisk Grade Distribution:")
print(pdf.groupby("risk_grade").agg(
    count=("risk_grade", "size"),
    avg_pd=("pd_estimate", "mean"),
    avg_score=("credit_score", "mean"),
).sort_index().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Scored Output

# COMMAND ----------

pdf["_scored_at"] = pd.Timestamp.now()
pdf["_model_name"] = MODEL_NAME
pdf["_model_uri"] = model_uri
pdf["_model_version"] = int(champion_version)

scored_df = spark.createDataFrame(pdf).withColumn(TARGET, F.col(TARGET).cast("double"))
(
    scored_df.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)

# Enable Change Data Feed so Lakehouse Monitoring can track new scores
spark.sql(f"ALTER TABLE {OUTPUT_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

final_count = spark.table(OUTPUT_TABLE).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nSCORING COMPLETE\n{'='*50}")
print(f"Model:       {model_uri}  (v{champion_version})")
print(f"Rows:        {final_count:,}")
print(f"Score range: {pdf['credit_score'].min()} - {pdf['credit_score'].max()}")
print(f"Mean PD:     {pdf['pd_estimate'].mean():.4f}")
print(f"Output:      {OUTPUT_TABLE}")
print(f"Elapsed:     {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{final_count}|scoring|{elapsed:.1f}s")
