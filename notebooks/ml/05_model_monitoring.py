# Databricks notebook source

# MAGIC %md
# MAGIC # 05 - Model Monitoring
# MAGIC **Config-driven model performance monitoring and drift detection.**
# MAGIC
# MAGIC Two monitoring approaches (both run):
# MAGIC 1. **Lakehouse Monitoring** - Databricks-native data profiling on the scored output table
# MAGIC    (InferenceLog profile with automatic drift metrics and dashboard)
# MAGIC 2. **Custom Metrics** - Credit-risk-specific metrics (Gini, KS, AUC, PSI) per population
# MAGIC    logged to a Delta table for historical tracking and alerting

# COMMAND ----------

# MAGIC %run ./00_ml_config_loader

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
from sklearn.metrics import roc_auc_score, roc_curve
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
mon_cfg = config["monitoring"]
score_cfg = config["scoring"]
feat_cfg = config["features"]
prep_cfg = config["data_prep"]

target_col = model_cfg["target_variable"]
scored_table = score_cfg["output_table"]
monitoring_table = mon_cfg["monitoring_table"]
baseline_table = mon_cfg["baseline_table"]

print(f"Model:      {model_name}")
print(f"Scored:     {scored_table}")
print(f"Monitor:    {monitoring_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Lakehouse Monitoring (Databricks-Native)
# MAGIC
# MAGIC Creates or refreshes a Databricks Data Profile on the scored output table.
# MAGIC This provides automatic drift detection, distribution profiling, and a
# MAGIC generated monitoring dashboard -- all managed by the platform.

# COMMAND ----------

start_time = datetime.now()

# Attempt Lakehouse Monitoring setup (requires SDK >= 0.68.0)
lakehouse_monitor_active = False
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        MonitorInferenceLog, MonitorInferenceLogProblemType
    )

    w = WorkspaceClient()

    # Check if monitor already exists
    try:
        existing = w.quality_monitors.get(scored_table)
        print(f"Lakehouse Monitor exists for {scored_table}")
        # Refresh to pick up new data
        w.quality_monitors.run_refresh(scored_table)
        print("  Refresh triggered")
        lakehouse_monitor_active = True
    except Exception:
        # Create new monitor with InferenceLog profile
        print(f"Creating Lakehouse Monitor for {scored_table}...")
        w.quality_monitors.create(
            table_name=scored_table,
            assets_dir=f"/Shared/ml/monitoring/{model_name}",
            output_schema_name=f"{catalog}.retail_ml",
            inference_log=MonitorInferenceLog(
                problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
                prediction_col="pd_estimate",
                label_col=target_col,
                model_id_col="_model_uri",
                timestamp_col="_scored_at",
                granularities=["1 day"],
            ),
        )
        print("  Lakehouse Monitor created")
        lakehouse_monitor_active = True

except ImportError:
    print("Databricks SDK not available - skipping Lakehouse Monitor")
    print("  (Install databricks-sdk >= 0.68.0 for native monitoring)")
except Exception as e:
    print(f"Lakehouse Monitor setup skipped: {type(e).__name__}: {str(e)[:200]}")
    print("  Custom monitoring will still run below")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Scored Data for Custom Metrics

# COMMAND ----------

df_scored = spark.table(scored_table)
total = df_scored.count()
print(f"Scored rows: {total:,}")

pdf = df_scored.select(target_col, "pd_estimate", "credit_score", "risk_grade", "_population").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate Performance Metrics Per Population

# COMMAND ----------

def calc_metrics(y_true, y_prob):
    """Calculate Gini, KS, AUC."""
    if len(np.unique(y_true)) < 2:
        return {"auc": None, "gini": None, "ks": None}

    auc = roc_auc_score(y_true, y_prob)
    gini = 2 * auc - 1
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    ks = max(tpr - fpr)
    return {"auc": round(auc, 4), "gini": round(gini, 4), "ks": round(ks, 4)}


def calc_psi(expected, actual, bins=10):
    """Calculate PSI between two distributions."""
    exp_hist, edges = np.histogram(expected, bins=bins)
    act_hist, _ = np.histogram(actual, bins=edges)
    exp_pct = (exp_hist + 0.5) / (exp_hist.sum() + 0.5)
    act_pct = (act_hist + 0.5) / (act_hist.sum() + 0.5)
    return round(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)), 4)


monitoring_rows = []
run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for pop in pdf["_population"].unique():
    pop_data = pdf[pdf["_population"] == pop]
    y_true = pop_data[target_col].astype(int)
    y_prob = pop_data["pd_estimate"]

    metrics = calc_metrics(y_true, y_prob)

    print(f"\n{pop.upper()} Population ({len(pop_data):,} rows):")
    print(f"  AUC:  {metrics['auc']}")
    print(f"  Gini: {metrics['gini']}")
    print(f"  KS:   {metrics['ks']}")

    for metric_name, metric_value in metrics.items():
        if metric_value is not None:
            monitoring_rows.append({
                "model_name": model_name,
                "run_timestamp": run_timestamp,
                "population": pop,
                "metric": metric_name,
                "value": metric_value,
                "alert_level": "none",
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: PSI Drift Detection

# COMMAND ----------

dev_probs = pdf[pdf["_population"] == "dev"]["pd_estimate"]

for pop in ["holdout", "oot"]:
    pop_probs = pdf[pdf["_population"] == pop]["pd_estimate"]
    if len(pop_probs) == 0:
        continue

    psi = calc_psi(dev_probs, pop_probs)
    thresholds = mon_cfg["drift_thresholds"]["psi"]

    if psi >= thresholds["critical"]:
        alert = "critical"
    elif psi >= thresholds["warning"]:
        alert = "warning"
    else:
        alert = "none"

    print(f"\nPSI (dev vs {pop}): {psi} [{alert.upper()}]")

    monitoring_rows.append({
        "model_name": model_name,
        "run_timestamp": run_timestamp,
        "population": pop,
        "metric": "psi",
        "value": psi,
        "alert_level": alert,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Gini/KS Drift Detection (vs Baseline)

# COMMAND ----------

baseline_exists = spark.catalog.tableExists(baseline_table)

if baseline_exists:
    baseline_df = spark.table(baseline_table).toPandas()

    for pop in pdf["_population"].unique():
        for metric in ["gini", "ks"]:
            baseline_row = baseline_df[
                (baseline_df["population"] == pop) & (baseline_df["metric"] == metric)
            ]
            if baseline_row.empty:
                continue

            baseline_val = baseline_row["value"].iloc[0]
            current_row = [r for r in monitoring_rows if r["population"] == pop and r["metric"] == metric]
            if not current_row:
                continue

            current_val = current_row[0]["value"]
            drop = baseline_val - current_val

            threshold_key = f"{metric}_drop"
            if threshold_key in mon_cfg["drift_thresholds"]:
                thresholds = mon_cfg["drift_thresholds"][threshold_key]
                if drop >= thresholds["critical"]:
                    current_row[0]["alert_level"] = "critical"
                    print(f"CRITICAL: {metric} dropped by {drop:.4f} on {pop}")
                elif drop >= thresholds["warning"]:
                    current_row[0]["alert_level"] = "warning"
                    print(f"WARNING: {metric} dropped by {drop:.4f} on {pop}")
else:
    print("No baseline found - this run will become the baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write Monitoring Log

# COMMAND ----------

mon_spark_df = spark.createDataFrame(pd.DataFrame(monitoring_rows))

# Append to monitoring table (don't overwrite - keep history)
if spark.catalog.tableExists(monitoring_table):
    (
        mon_spark_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(monitoring_table)
    )
else:
    (
        mon_spark_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(monitoring_table)
    )

print(f"\nMonitoring log written: {monitoring_table} ({len(monitoring_rows)} metrics)")

# Save baseline if first run
if not baseline_exists:
    (
        mon_spark_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(baseline_table)
    )
    print(f"Baseline saved: {baseline_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Alert Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()

critical_alerts = [r for r in monitoring_rows if r["alert_level"] == "critical"]
warning_alerts = [r for r in monitoring_rows if r["alert_level"] == "warning"]

print(f"\n{'='*60}")
print(f"MONITORING COMPLETE: {model_name}")
print(f"{'='*60}")
print(f"  Lakehouse Monitor:  {'Active' if lakehouse_monitor_active else 'Not available'}")
print(f"  Custom metrics:     {len(monitoring_rows)} logged")
print(f"  Critical alerts:    {len(critical_alerts)}")
print(f"  Warning alerts:     {len(warning_alerts)}")
print(f"  Elapsed:            {elapsed:.1f}s")

if critical_alerts:
    print(f"\n  CRITICAL ALERTS:")
    for a in critical_alerts:
        print(f"    {a['metric']} ({a['population']}): {a['value']}")

if warning_alerts:
    print(f"\n  WARNING ALERTS:")
    for a in warning_alerts:
        print(f"    {a['metric']} ({a['population']}): {a['value']}")

# COMMAND ----------

status = "ALERT" if critical_alerts else "SUCCESS"
result = f"{status}|{model_name}|{len(critical_alerts)}_critical|monitoring|{elapsed:.1f}s"
print(result)
dbutils.notebook.exit(result)
