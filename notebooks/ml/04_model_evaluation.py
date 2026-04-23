# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Model Evaluation
# MAGIC Credit Card Behaviour Scorecard
# MAGIC
# MAGIC 1. Load `@Challenger` from Unity Catalog
# MAGIC 2. Score holdout population and compute metrics
# MAGIC 3. Compare against `@Champion` (if it exists)
# MAGIC 4. Tag model version with `evaluation_status`

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow, mlflow.sklearn
from sklearn.metrics import (
    accuracy_score, f1_score, log_loss, roc_auc_score,
    precision_score, recall_score, roc_curve,
)
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

FEATURE_TABLE = f"{catalog}.retail_ml.cc_feature_store"
UC_MODEL      = f"{catalog}.retail_ml.cc_behaviour_scorecard"
EVAL_TABLE    = f"{catalog}.retail_ml.cc_evaluation_results"
EXPERIMENT    = "/Shared/ml/cc_behaviour_scorecard_experiments"

EVAL_POPULATION = "holdout"

print(f"Model:    {MODEL_NAME}")
print(f"UC Model: {UC_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load @Challenger

# COMMAND ----------

start_time = datetime.now()

mlflow.set_registry_uri("databricks-uc")
client = mlflow.tracking.MlflowClient()

challenger_info = client.get_model_version_by_alias(UC_MODEL, "Challenger")
challenger_version = challenger_info.version
challenger_uri = f"models:/{UC_MODEL}@Challenger"
challenger_model = mlflow.sklearn.load_model(challenger_uri)
print(f"Challenger: v{challenger_version}  ({type(challenger_model).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Holdout Data

# COMMAND ----------

df = spark.table(FEATURE_TABLE).filter(F.col("_population") == EVAL_POPULATION)
feature_cols = [f.strip() for f in df.select("_selected_features").first()[0].split(",") if f.strip()]

pdf = df.select(*feature_cols, TARGET).toPandas().dropna(subset=feature_cols)
X_eval = pdf[feature_cols].fillna(0)
for c in X_eval.columns:
    if X_eval[c].dtype == "object":
        X_eval[c] = X_eval[c].astype("category").cat.codes
y_eval = pdf[TARGET].astype(int)

print(f"Holdout rows: {len(X_eval):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Challenger

# COMMAND ----------

def credit_metrics(y, p):
    auc = roc_auc_score(y, p)
    fpr, tpr, _ = roc_curve(y, p)
    return {"auc": round(auc, 4), "gini": round(2 * auc - 1, 4), "ks": round(max(tpr - fpr), 4)}

mlflow.set_experiment(EXPERIMENT)

with mlflow.start_run(run_name=f"evaluation_{datetime.now():%Y%m%d_%H%M}") as run:
    mlflow.set_tag("step", "evaluation")
    mlflow.set_tag("model_name", MODEL_NAME)
    mlflow.set_tag("model_version", str(challenger_version))

    y_pred = challenger_model.predict(X_eval)
    y_prob = challenger_model.predict_proba(X_eval)[:, 1]

    eval_metrics = {
        "accuracy":  accuracy_score(y_eval, y_pred),
        "f1_score":  f1_score(y_eval, y_pred, zero_division=0),
        "precision": precision_score(y_eval, y_pred, zero_division=0),
        "recall":    recall_score(y_eval, y_pred, zero_division=0),
        "log_loss":  log_loss(y_eval, y_prob),
        "roc_auc":   roc_auc_score(y_eval, y_prob),
    }
    for k, v in eval_metrics.items():
        mlflow.log_metric(k, round(v, 4))

    challenger = credit_metrics(y_eval, y_prob)
    print(f"\nChallenger: AUC={challenger['auc']}  Gini={challenger['gini']}  KS={challenger['ks']}")

    eval_run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Champion Comparison

# COMMAND ----------

comparison = "passed"
champion = None
champion_version = None

try:
    champ_info = client.get_model_version_by_alias(UC_MODEL, "Champion")
    champion_version = champ_info.version
    champ_model = mlflow.sklearn.load_model(f"models:/{UC_MODEL}@Champion")
    champ_prob = champ_model.predict_proba(X_eval)[:, 1]
    champion = credit_metrics(y_eval, champ_prob)

    print(f"\nChampion  (v{champion_version}): AUC={champion['auc']}  Gini={champion['gini']}")
    print(f"Challenger (v{challenger_version}): AUC={challenger['auc']}  Gini={challenger['gini']}")

    if challenger["auc"] >= champion["auc"]:
        comparison = "passed"
        print("Challenger >= Champion — PASS")
    else:
        comparison = "failed"
        print("Challenger < Champion — FAIL")
except Exception:
    print("No existing @Champion — first deployment, auto-pass")
    comparison = "passed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Evaluation Results

# COMMAND ----------

row = {
    "model_name": MODEL_NAME,
    "challenger_version": int(challenger_version),
    "champion_version": int(champion_version) if champion_version else None,
    "evaluation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "challenger_auc":  challenger["auc"],
    "challenger_gini": challenger["gini"],
    "challenger_ks":   challenger["ks"],
    "champion_auc":  champion["auc"]  if champion else None,
    "champion_gini": champion["gini"] if champion else None,
    "champion_ks":   champion["ks"]   if champion else None,
    "comparison_result": comparison,
}

sdf = spark.createDataFrame(pd.DataFrame([row]))
mode = "append" if spark.catalog.tableExists(EVAL_TABLE) else "overwrite"
sdf.write.format("delta").mode(mode).saveAsTable(EVAL_TABLE)
print(f"\nResults written: {EVAL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tag Model Version

# COMMAND ----------

passed = comparison == "passed"
client.set_model_version_tag(UC_MODEL, challenger_version, "evaluation_status", "passed" if passed else "failed")
client.set_model_version_tag(UC_MODEL, challenger_version, "evaluation_run_id", eval_run_id)
client.set_model_version_tag(UC_MODEL, challenger_version, "evaluation_auc", str(challenger["auc"]))

if not passed:
    raise Exception(f"Evaluation FAILED for {MODEL_NAME} v{challenger_version}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nEVALUATION COMPLETE\n{'='*50}")
print(f"Challenger:  v{challenger_version}  (AUC={challenger['auc']}, Gini={challenger['gini']})")
print(f"Comparison:  {comparison}")
print(f"Elapsed:     {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|v{challenger_version}|evaluation|{comparison}|{elapsed:.1f}s")
