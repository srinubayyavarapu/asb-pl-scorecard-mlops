# Databricks notebook source

# MAGIC %md
# MAGIC # 08 - Model Monitoring
# MAGIC PL Application Scorecard
# MAGIC
# MAGIC 1. **Lakehouse Monitoring** — Databricks-native InferenceLog profile on the scored table
# MAGIC 2. **Custom credit-risk metrics** — Gini, KS, AUC per population (Funded only)
# MAGIC 3. **PSI drift** — DEV vs OOT on `final_p_good`
# MAGIC 4. **Weighted Gini by DP3 month-end** — per ASB walkthrough
# MAGIC 5. **Triggered retraining** — fires the training job when critical drift is detected

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

# MAGIC %run ../utils/job_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog (bundle passes ${var.catalog})")
dbutils.widgets.text("environment", "", "Bundle environment slug (stg/prod)")
catalog     = dbutils.widgets.get("catalog").strip()
environment = dbutils.widgets.get("environment").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME       = "application_scorecard"
TARGET           = "target_flag"

SCORED_TABLE     = f"{catalog}.pl_scorecard.scored_output"
MONITORING_TABLE = f"{catalog}.pl_scorecard.monitoring_log"
BASELINE_TABLE   = f"{catalog}.pl_scorecard.pl_monitoring_baseline"

TRAINING_JOB = f"asb-ml-pl-training-{environment}"

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
    # InferenceLog classification expects prediction & label to share a class
    # domain. We use:
    #   prediction_col = goodbadflag_inferred  ("Good" / "Bad")
    #   label_col      = _observed_label       ("Good" / "Bad" / NULL for Rejected/Indeterminate)
    # The label is NULL until 24-month performance lands, which is the standard
    # credit-scorecard monitoring pattern.
    w.quality_monitors.create(
        table_name=SCORED_TABLE,
        assets_dir=f"/Shared/ml/monitoring/{MODEL_NAME}",
        output_schema_name=f"{catalog}.pl_scorecard",
        inference_log=MonitorInferenceLog(
            problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
            prediction_col="goodbadflag_inferred",
            label_col="_observed_label",
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
# MAGIC ## Custom Credit-Risk Metrics (per population)

# COMMAND ----------

def credit_metrics(y, p):
    if len(np.unique(y)) < 2:
        return {"auc": None, "gini": None, "ks": None}
    auc = roc_auc_score(y, p)
    fpr, tpr, _ = roc_curve(y, p)
    return {"auc": round(auc, 4), "gini": round(2 * auc - 1, 4), "ks": round(max(tpr - fpr), 4)}


def psi(expected, actual, bins=10):
    exp_hist, edges = np.histogram(expected, bins=bins)
    act_hist, _     = np.histogram(actual, bins=edges)
    exp_pct = (exp_hist + 0.5) / (exp_hist.sum() + 0.5)
    act_pct = (act_hist + 0.5) / (act_hist.sum() + 0.5)
    return round(float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct))), 4)


pdf = (
    spark.table(SCORED_TABLE)
    .select(TARGET, "final_p_good", "_population", "dp3_application", "sample_flag")
    .toPandas()
)
rows = []
ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Funded-only Bad/Good split has measurable Gini; Rejected has no ground truth
funded_pdf = pdf[pdf[TARGET].isin(["Bad", "Good"])].copy()
funded_pdf["_y"] = (funded_pdf[TARGET] == "Bad").astype(int)
# Use PD = 1 - p_good for ROC (higher = worse)
funded_pdf["_pd"] = 1 - funded_pdf["final_p_good"]

for pop in funded_pdf["_population"].unique():
    part = funded_pdf[funded_pdf["_population"] == pop]
    m = credit_metrics(part["_y"].values, part["_pd"].values)
    print(f"\n{pop:<5} (Funded Bad/Good, {len(part):,} rows):  AUC={m['auc']}  Gini={m['gini']}  KS={m['ks']}")
    for k, v in m.items():
        if v is not None:
            rows.append({"model_name": MODEL_NAME, "run_timestamp": ts, "population": pop,
                          "metric": k, "value": v, "alert_level": "none"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weighted Gini by DP3 month-end

# COMMAND ----------

weighted_gini_by_dp3 = None
if len(funded_pdf) > 0:
    total_n = 0
    weighted = 0.0
    for dp3, sub in funded_pdf.groupby("dp3_application"):
        if len(np.unique(sub["_y"])) < 2:
            continue
        m = credit_metrics(sub["_y"].values, sub["_pd"].values)
        if m["gini"] is not None:
            weighted += len(sub) * m["gini"]
            total_n  += len(sub)
    if total_n:
        weighted_gini_by_dp3 = round(weighted / total_n, 4)

print(f"\nWeighted Gini by DP3 month-end: {weighted_gini_by_dp3}")
if weighted_gini_by_dp3 is not None:
    rows.append({"model_name": MODEL_NAME, "run_timestamp": ts, "population": "ALL",
                  "metric": "weighted_gini_dp3", "value": weighted_gini_by_dp3, "alert_level": "none"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## PSI Drift (DEV vs OOT)

# COMMAND ----------

dev_p = pdf[pdf["_population"] == "DEV"]["final_p_good"].dropna()
for pop in ["HOL", "OOT"]:
    part = pdf[pdf["_population"] == pop]["final_p_good"].dropna()
    if len(part) == 0 or len(dev_p) == 0:
        continue
    p = psi(dev_p, part)
    alert = "critical" if p >= PSI_CRITICAL else "warning" if p >= PSI_WARNING else "none"
    print(f"\nPSI (DEV vs {pop}): {p}  [{alert.upper()}]")
    rows.append({"model_name": MODEL_NAME, "run_timestamp": ts, "population": pop,
                  "metric": "psi_p_good", "value": p, "alert_level": alert})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Monitoring Log

# COMMAND ----------

sdf = spark.createDataFrame(pd.DataFrame(rows))
mode = "append" if spark.catalog.tableExists(MONITORING_TABLE) else "overwrite"
sdf.write.format("delta").mode(mode).saveAsTable(MONITORING_TABLE)
enable_iceberg_uniform(MONITORING_TABLE)
print(f"\nMonitoring log: {MONITORING_TABLE} ({len(rows)} metrics)")

if not spark.catalog.tableExists(BASELINE_TABLE):
    sdf.write.format("delta").mode("overwrite").saveAsTable(BASELINE_TABLE)
    enable_iceberg_uniform(BASELINE_TABLE)
    print(f"Baseline saved: {BASELINE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triggered Retraining (on critical drift)

# COMMAND ----------

critical = [r for r in rows if r["alert_level"] == "critical"]
warnings = [r for r in rows if r["alert_level"] == "warning"]
retrained = False

if AUTO_RETRAIN and critical:
    print(f"\nCritical drift detected -- triggering {TRAINING_JOB}")
    try:
        # In bundle dev mode the job name gets a "[dev <user>] " prefix; substring match handles it
        jobs = list(w.jobs.list(name=TRAINING_JOB))
        if not jobs:
            jobs = [j for j in w.jobs.list() if TRAINING_JOB in (j.settings.name or "")]
        if jobs:
            run = w.jobs.run_now(job_id=jobs[0].job_id)
            print(f"  Retraining triggered: run_id={run.run_id} on '{jobs[0].settings.name}'")
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
print(f"\n{'='*55}\nMONITORING COMPLETE\n{'='*55}")
print(f"Lakehouse Monitor: {'Active' if lakehouse_active else 'Not available'}")
print(f"Metrics logged:    {len(rows)}")
print(f"Critical alerts:   {len(critical)}")
print(f"Warning alerts:    {len(warnings)}")
print(f"Retraining:        {'Triggered' if retrained else 'Not triggered'}")
print(f"Elapsed:           {elapsed:.1f}s")

status = "ALERT" if critical else "SUCCESS"
dbutils.notebook.exit(f"{status}|{MODEL_NAME}|{len(critical)}_critical|{len(warnings)}_warning|monitoring|{elapsed:.1f}s")
