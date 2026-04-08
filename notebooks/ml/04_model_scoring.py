# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Model Scoring
# MAGIC **Config-driven batch scoring pipeline.**
# MAGIC
# MAGIC Loads the registered Champion model from Unity Catalog,
# MAGIC scores the input population, applies scorecard scaling,
# MAGIC assigns risk grades, and writes to Gold layer.
# MAGIC
# MAGIC **SAS Equivalent:** SCORE statement + PROC FORMAT risk grade assignment

# COMMAND ----------

import os as _os
_nb_dir = _os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
exec(open(f"/Workspace{_nb_dir}/00_ml_config_loader.py").read())

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow
import numpy as np
import pandas as pd

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
score_cfg = config["scoring"]
train_cfg = config["training"]
feat_cfg = config["features"]

target_col = model_cfg["target_variable"]
input_table = score_cfg["input_table"]
output_table = score_cfg["output_table"]

print(f"Model:  {model_name}")
print(f"Input:  {input_table}")
print(f"Output: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Champion Model from UC Registry

# COMMAND ----------

start_time = datetime.now()

# Load model URI saved by training task
model_uri_table = f"{catalog}.retail_ml.hl_model_load_uri"
model_uri = spark.table(model_uri_table).collect()[0]["model_uri"]
print(f"Loading model: {model_uri}")
loaded_model = mlflow.sklearn.load_model(model_uri)
print(f"  Model loaded: {type(loaded_model).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load & Prepare Scoring Data

# COMMAND ----------

# Load from feature store (has _selected_features column)
feature_store_table = feat_cfg["feature_store_table"]
df = spark.table(feature_store_table)
total_rows = df.count()
print(f"Input rows: {total_rows:,} (from {feature_store_table})")

# Get feature columns
selected_features_str = df.select("_selected_features").first()[0]
feature_cols = [f.strip() for f in selected_features_str.split(",") if f.strip()]

# Keep key columns + features for scoring
key_col = model_cfg["primary_key"]
keep_cols = [key_col] + feature_cols + [target_col, "_population"]
keep_cols = [c for c in keep_cols if c in df.columns]

pdf = df.select(*keep_cols).toPandas()

# Prepare feature matrix
X = pdf[feature_cols].fillna(0)
for col in X.columns:
    if X[col].dtype == "object":
        X[col] = X[col].astype("category").cat.codes

print(f"Scoring {len(X):,} rows with {len(feature_cols)} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Score & Apply Scorecard Scaling

# COMMAND ----------

# Get raw probabilities
y_prob = loaded_model.predict_proba(X)[:, 1]
pdf["pd_estimate"] = y_prob

# Scorecard scaling
scaling = score_cfg["scorecard_scaling"]
target_score = scaling["target_score"]
target_odds = scaling["target_odds"]
pdo = scaling["pdo"]

factor = pdo / np.log(2)
offset = target_score - factor * np.log(target_odds)

# Convert PD to odds, then to score
# odds = (1 - PD) / PD
odds = np.where(y_prob > 0, (1 - y_prob) / np.clip(y_prob, 1e-10, 1), target_odds * 10)
pdf["credit_score"] = np.round(offset + factor * np.log(odds)).astype(int)

# Clip scores to reasonable range
pdf["credit_score"] = pdf["credit_score"].clip(0, 999)

print(f"\nScore Distribution:")
print(f"  Mean:   {pdf['credit_score'].mean():.0f}")
print(f"  Median: {pdf['credit_score'].median():.0f}")
print(f"  Min:    {pdf['credit_score'].min()}")
print(f"  Max:    {pdf['credit_score'].max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Assign Risk Grades

# COMMAND ----------

def assign_risk_grade(score, risk_grades):
    """Assign risk grade based on score and config."""
    for rg in risk_grades:
        if rg["score_min"] <= score <= rg["score_max"]:
            return rg["grade"]
    return "D"  # Default to worst grade

pdf["risk_grade"] = pdf["credit_score"].apply(
    lambda s: assign_risk_grade(s, score_cfg["risk_grades"])
)

# Risk grade distribution
print("\nRisk Grade Distribution:")
grade_dist = pdf.groupby("risk_grade").agg(
    count=("risk_grade", "size"),
    avg_pd=("pd_estimate", "mean"),
    avg_score=("credit_score", "mean"),
).sort_index()
print(grade_dist.to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write Scored Output

# COMMAND ----------

# Add audit columns
pdf["_scored_at"] = pd.Timestamp.now()
pdf["_model_name"] = model_name
pdf["_model_uri"] = model_uri

# Convert to Spark and write
scored_df = spark.createDataFrame(pdf)

(
    scored_df.write
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
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()

print(f"\n{'='*60}")
print(f"SCORING COMPLETE: {model_name}")
print(f"{'='*60}")
print(f"  Model:        {model_uri}")
print(f"  Rows scored:  {final_count:,}")
print(f"  Score range:  {pdf['credit_score'].min()} - {pdf['credit_score'].max()}")
print(f"  Mean PD:      {pdf['pd_estimate'].mean():.4f}")
print(f"  Output:       {output_table}")
print(f"  Elapsed:      {elapsed:.1f}s")

# COMMAND ----------

result = f"SUCCESS|{model_name}|{final_count}|scoring|{elapsed:.1f}s"
print(result)
dbutils.notebook.exit(result)
