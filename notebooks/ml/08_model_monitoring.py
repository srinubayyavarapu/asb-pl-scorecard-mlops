# Databricks notebook source

# MAGIC %md
# MAGIC # 08 - Model Monitoring
# MAGIC Credit Card Behaviour Scorecard
# MAGIC
# MAGIC 1. **Lakehouse Monitoring** — Databricks-native InferenceLog profile on the scored table
# MAGIC    (auto-creates metric tables + a monitoring dashboard)
# MAGIC 2. **Custom credit-risk metrics** — Gini, KS, AUC per population, written to a Delta log
# MAGIC 3. **PSI drift** — dev vs holdout
# MAGIC 4. **Triggered retraining** — fires the training job when critical drift is detected

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from sklearn.metrics import roc_auc_score, roc_curve
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorInferenceLog, MonitorInferenceLogProblemType,
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

MODEL_NAME       = "cc_behaviour_scorecard"
TARGET           = "defaulted"

SCORED_TABLE     = f"{catalog}.retail_gold.cc_scored_output"
MONITORING_TABLE = f"{catalog}.retail_ml.cc_monitoring_log"
BASELINE_TABLE   = f"{catalog}.retail_ml.cc_monitoring_baseline"

# Derive environment from catalog (asb_dev → dev, asb_stg → stg, asb_prod → prod)
env = catalog.replace("asb_", "")
TRAINING_JOB = f"asb-ml-cc-training-{env}"

PSI_WARNING   = 0.10
PSI_CRITICAL  = 0.25
AUTO_RETRAIN  = True

print(f"Model:       {MODEL_NAME}")
print(f"Scored:      {SCORED_TABLE}")
print(f"Retrain job: {TRAINING_JOB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakehouse Monitoring (Databricks-native)

# COMMAND ----------

start_time = datetime.now()
w = WorkspaceClient()
lakehouse_active = False

try:
    w.quality_monitors.get(SCORED_TABLE)
    w.quality_monitors.run_refresh(SCORED_TABLE)
    print(f"Lakehouse Monitor refreshed: {SCORED_TABLE}")
    lakehouse_active = True
except Exception:
    print(f"Creating Lakehouse Monitor for {SCORED_TABLE}")
    w.quality_monitors.create(
        table_name=SCORED_TABLE,
        assets_dir=f"/Shared/ml/monitoring/{MODEL_NAME}",
        output_schema_name=f"{catalog}.retail_ml",
        inference_log=MonitorInferenceLog(
            problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
            prediction_col="pd_estimate",
            label_col=TARGET,
            model_id_col="_model_uri",
            timestamp_col="_scored_at",
            granularities=["1 day"],
        ),
    )
    print(f"  Dashboard: /Shared/ml/monitoring/{MODEL_NAME}")
    print(f"  Metric tables: {SCORED_TABLE}_profile_metrics, {SCORED_TABLE}_drift_metrics")
    lakehouse_active = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Credit-Risk Metrics

# COMMAND ----------

def metrics(y, p):
    if len(np.unique(y)) < 2:
        return {"auc": None, "gini": None, "ks": None}
    auc = roc_auc_score(y, p)
    fpr, tpr, _ = roc_curve(y, p)
    return {"auc": round(auc, 4), "gini": round(2 * auc - 1, 4), "ks": round(max(tpr - fpr), 4)}

def psi(expected, actual, bins=10):
    exp_hist, edges = np.histogram(expected, bins=bins)
    act_hist, _ = np.histogram(actual, bins=edges)
    exp_pct = (exp_hist + 0.5) / (exp_hist.sum() + 0.5)
    act_pct = (act_hist + 0.5) / (act_hist.sum() + 0.5)
    return round(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)), 4)

pdf = spark.table(SCORED_TABLE).select(TARGET, "pd_estimate", "_population").toPandas()
rows = []
ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for pop in pdf["_population"].unique():
    part = pdf[pdf["_population"] == pop]
    m = metrics(part[TARGET].astype(int), part["pd_estimate"])
    print(f"\n{pop.upper()} ({len(part):,} rows):  AUC={m['auc']}  Gini={m['gini']}  KS={m['ks']}")
    for k, v in m.items():
        if v is not None:
            rows.append({"model_name": MODEL_NAME, "run_timestamp": ts, "population": pop,
                          "metric": k, "value": v, "alert_level": "none"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## PSI Drift Detection (dev vs holdout)

# COMMAND ----------

dev_probs = pdf[pdf["_population"] == "dev"]["pd_estimate"]
for pop in ["holdout"]:
    part = pdf[pdf["_population"] == pop]["pd_estimate"]
    if len(part) == 0:
        continue
    p = psi(dev_probs, part)
    alert = "critical" if p >= PSI_CRITICAL else "warning" if p >= PSI_WARNING else "none"
    print(f"\nPSI (dev vs {pop}): {p}  [{alert.upper()}]")
    rows.append({"model_name": MODEL_NAME, "run_timestamp": ts, "population": pop,
                  "metric": "psi", "value": p, "alert_level": alert})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Monitoring Log

# COMMAND ----------

sdf = spark.createDataFrame(pd.DataFrame(rows))
mode = "append" if spark.catalog.tableExists(MONITORING_TABLE) else "overwrite"
sdf.write.format("delta").mode(mode).saveAsTable(MONITORING_TABLE)
print(f"\nMonitoring log: {MONITORING_TABLE} ({len(rows)} metrics)")

if not spark.catalog.tableExists(BASELINE_TABLE):
    sdf.write.format("delta").mode("overwrite").saveAsTable(BASELINE_TABLE)
    print(f"Baseline saved: {BASELINE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triggered Retraining

# COMMAND ----------

critical = [r for r in rows if r["alert_level"] == "critical"]
warnings = [r for r in rows if r["alert_level"] == "warning"]
retrained = False

if AUTO_RETRAIN and critical:
    print(f"\nCritical drift detected — triggering {TRAINING_JOB}")
    try:
        jobs = list(w.jobs.list(name=TRAINING_JOB))
        if jobs:
            run = w.jobs.run_now(job_id=jobs[0].job_id)
            print(f"  Retraining triggered: run_id={run.run_id}")
            for c in critical:
                print(f"    - {c['metric']} ({c['population']}): {c['value']}")
            retrained = True
        else:
            print(f"  Job not found: {TRAINING_JOB}")
    except Exception as e:
        print(f"  Retrain error: {type(e).__name__}: {str(e)[:120]}")
elif critical:
    print(f"\nCritical alerts present ({len(critical)}) but auto-retrain disabled")
else:
    print("\nNo critical drift")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nMONITORING COMPLETE\n{'='*50}")
print(f"Lakehouse Monitor: {'Active' if lakehouse_active else 'Not available'}")
print(f"Metrics logged:    {len(rows)}")
print(f"Critical alerts:   {len(critical)}")
print(f"Warning alerts:    {len(warnings)}")
print(f"Retraining:        {'Triggered' if retrained else 'Not triggered'}")
print(f"Elapsed:           {elapsed:.1f}s")

status = "ALERT" if critical else "SUCCESS"
dbutils.notebook.exit(f"{status}|{MODEL_NAME}|{len(critical)}_critical|{len(warnings)}_warning|monitoring|{elapsed:.1f}s")
