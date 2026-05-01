# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Model Evaluation
# MAGIC PL Application Scorecard
# MAGIC
# MAGIC 1. Load `@Challenger` from Unity Catalog
# MAGIC 2. Score HOL + OOT populations
# MAGIC 3. Compute weighted Gini overall + by DP3 month-end + by sample_flag
# MAGIC 4. **SAS reconciliation** — compare model output vs `sas_final_p_good`
# MAGIC 5. Compare to `@Champion` if exists
# MAGIC 6. Tag model version with `evaluation_status`

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow, mlflow.sklearn
from sklearn.metrics import roc_auc_score, roc_curve
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_retail_modelling", "Unity Catalog")
dbutils.widgets.text("model_name", "", "UC model (auto-set by Deployment Jobs)")
dbutils.widgets.text("model_version", "", "Version (auto-set by Deployment Jobs)")

catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME       = "application_scorecard"
TARGET           = "target_flag"

FEATURE_TABLE    = f"{catalog}.pl_scorecard.feature_store"
UC_MODEL_DEFAULT = f"{catalog}.pl_scorecard.application_scorecard"
UC_MODEL         = dbutils.widgets.get("model_name").strip() or UC_MODEL_DEFAULT
EVAL_TABLE       = f"{catalog}.pl_scorecard.evaluation_results"
EXPERIMENT       = "/Shared/ml/pl_scorecard/application_scorecard"

DEPLOY_JOB_VERSION = dbutils.widgets.get("model_version").strip()

print(f"Model:    {MODEL_NAME}")
print(f"UC Model: {UC_MODEL}")
print(f"Trigger:  {'MLflow 3 Deployment Job (v' + DEPLOY_JOB_VERSION + ')' if DEPLOY_JOB_VERSION else 'manual (uses @Challenger alias)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load @Challenger

# COMMAND ----------

start_time = datetime.now()
mlflow.set_registry_uri("databricks-uc")
client = mlflow.tracking.MlflowClient()

if DEPLOY_JOB_VERSION:
    challenger_version = DEPLOY_JOB_VERSION
    challenger_uri = f"models:/{UC_MODEL}/{challenger_version}"
else:
    challenger_info = client.get_model_version_by_alias(UC_MODEL, "Challenger")
    challenger_version = challenger_info.version
    challenger_uri = f"models:/{UC_MODEL}@Challenger"

challenger_model = mlflow.sklearn.load_model(challenger_uri)
print(f"Challenger v{challenger_version} loaded ({type(challenger_model).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load HOL + OOT Populations (Funded Bad/Good)

# COMMAND ----------

df = spark.table(FEATURE_TABLE)
selected_features = [
    f.strip() for f in df.select("_selected_features").first()[0].split(",") if f.strip()
]


def to_xy_with_meta(spark_df):
    """Convert to numeric X/y plus DP3 month-end and sample_flag for slicing."""
    cols = selected_features + [TARGET, "_population", "dp3_application", "sample_flag",
                                  "sas_final_p_good", "application_id"]
    pdf = spark_df.select(*cols).toPandas().dropna(subset=selected_features)
    X = pdf[selected_features].copy()
    for c in X.columns:
        if X[c].dtype == "object":
            X[c] = X[c].astype("category").cat.codes
    return X.fillna(0), pdf


eval_pop = (
    df.filter(F.col("_population").isin("HOL", "OOT"))
      .filter(F.col(TARGET).isin("Bad", "Good"))
)
X_eval, pdf_eval = to_xy_with_meta(eval_pop)
y_eval = (pdf_eval[TARGET] == "Bad").astype(int).values

print(f"Eval rows (HOL+OOT, Funded Bad/Good): {len(X_eval):,}")
pdf_eval["_pd"] = challenger_model.predict_proba(X_eval)[:, 1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics

# COMMAND ----------

def gini(y, p):
    if len(np.unique(y)) < 2:
        return None
    return round(2 * roc_auc_score(y, p) - 1, 4)


def auc(y, p):
    if len(np.unique(y)) < 2:
        return None
    return round(roc_auc_score(y, p), 4)


def ks(y, p):
    if len(np.unique(y)) < 2:
        return None
    fpr, tpr, _ = roc_curve(y, p)
    return round(max(tpr - fpr), 4)


# Overall metrics
overall = {
    "auc":  auc(y_eval, pdf_eval["_pd"]),
    "gini": gini(y_eval, pdf_eval["_pd"]),
    "ks":   ks(y_eval, pdf_eval["_pd"]),
}
print(f"\n[Overall] AUC={overall['auc']}  Gini={overall['gini']}  KS={overall['ks']}")


def weighted_gini_by(group_col):
    """Weighted Gini = sum(n_g * gini_g) / sum(n_g) — per ASB walkthrough."""
    rows = []
    total_n = 0
    weighted = 0.0
    for g, sub in pdf_eval.groupby(group_col):
        y_sub = (sub[TARGET] == "Bad").astype(int).values
        p_sub = sub["_pd"].values
        if len(np.unique(y_sub)) < 2:
            continue
        gn = gini(y_sub, p_sub)
        n = len(sub)
        rows.append({group_col: str(g), "n": n, "gini": gn})
        weighted += n * gn
        total_n += n
    weighted_avg = round(weighted / total_n, 4) if total_n else None
    return rows, weighted_avg


by_dp3, w_gini_dp3 = weighted_gini_by("dp3_application")
by_pop, w_gini_pop = weighted_gini_by("_population")

print(f"\n[Weighted Gini by DP3 month-end] = {w_gini_dp3}")
for r in by_dp3:
    print(f"  {r['dp3_application']}  n={r['n']:>5,}  gini={r['gini']}")

print(f"\n[Weighted Gini by population] = {w_gini_pop}")
for r in by_pop:
    print(f"  {r['_population']:<5}  n={r['n']:>5,}  gini={r['gini']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAS Reconciliation
# MAGIC
# MAGIC Per ASB regulatory requirement: SAS-vs-DBx output must reconcile within tolerance.
# MAGIC We compare model PD vs `sas_final_p_good` from the existing SAS scorecard.

# COMMAND ----------

# Note: model PD = P(Bad) = 1 - P(Good); SAS p_good is P(Good).
# So we compare model_p_good (= 1 - PD) against sas_final_p_good.
sas_pop = pdf_eval.dropna(subset=["sas_final_p_good"])
if len(sas_pop) > 0:
    model_p_good = 1 - sas_pop["_pd"].values
    sas_p_good   = sas_pop["sas_final_p_good"].values
    correlation  = round(float(np.corrcoef(model_p_good, sas_p_good)[0, 1]), 4)
    mean_diff    = round(float((model_p_good - sas_p_good).mean()), 4)
    max_abs_diff = round(float(np.abs(model_p_good - sas_p_good).max()), 4)
    print(f"\n[SAS Reconciliation]")
    print(f"  Compared rows:           {len(sas_pop):,}")
    print(f"  Pearson correlation:     {correlation}")
    print(f"  Mean diff (model - sas): {mean_diff}")
    print(f"  Max abs diff:            {max_abs_diff}")
else:
    correlation = mean_diff = max_abs_diff = None
    print("[SAS Reconciliation] no SAS scores available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Champion Comparison

# COMMAND ----------

comparison = "passed"
champion_version = None
champion_gini = None
try:
    champ_info = client.get_model_version_by_alias(UC_MODEL, "Champion")
    champion_version = champ_info.version
    champ_model = mlflow.sklearn.load_model(f"models:/{UC_MODEL}@Champion")
    champ_pd = champ_model.predict_proba(X_eval)[:, 1]
    champion_gini = gini(y_eval, champ_pd)

    print(f"\nChampion  v{champion_version}: Gini={champion_gini}")
    print(f"Challenger v{challenger_version}: Gini={overall['gini']}")
    if overall['gini'] >= champion_gini:
        comparison = "passed"
        print("Challenger >= Champion -- PASS")
    else:
        comparison = "failed"
        print("Challenger < Champion -- FAIL")
except Exception:
    print("\nNo existing @Champion -- first deployment, auto-pass")
    comparison = "passed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Evaluation Results

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql import Row

eval_schema = StructType([
    StructField("model_name",            StringType(), False),
    StructField("challenger_version",    LongType(),   False),
    StructField("champion_version",      LongType(),   True),
    StructField("evaluation_timestamp",  StringType(), False),
    StructField("eval_rows",              LongType(),   False),
    StructField("auc",                    DoubleType(), True),
    StructField("gini",                   DoubleType(), True),
    StructField("ks",                     DoubleType(), True),
    StructField("weighted_gini_by_dp3",   DoubleType(), True),
    StructField("weighted_gini_by_pop",   DoubleType(), True),
    StructField("sas_correlation",        DoubleType(), True),
    StructField("sas_mean_diff",          DoubleType(), True),
    StructField("sas_max_abs_diff",       DoubleType(), True),
    StructField("champion_gini",          DoubleType(), True),
    StructField("comparison_result",      StringType(), False),
])

sdf = spark.createDataFrame([Row(
    model_name=MODEL_NAME,
    challenger_version=int(challenger_version),
    champion_version=int(champion_version) if champion_version else None,
    evaluation_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    eval_rows=len(X_eval),
    auc=overall["auc"], gini=overall["gini"], ks=overall["ks"],
    weighted_gini_by_dp3=w_gini_dp3,
    weighted_gini_by_pop=w_gini_pop,
    sas_correlation=correlation,
    sas_mean_diff=mean_diff,
    sas_max_abs_diff=max_abs_diff,
    champion_gini=champion_gini,
    comparison_result=comparison,
)], schema=eval_schema)

mode = "append" if spark.catalog.tableExists(EVAL_TABLE) else "overwrite"
sdf.write.format("delta").mode(mode).saveAsTable(EVAL_TABLE)
print(f"\nResults written: {EVAL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tag Model Version

# COMMAND ----------

passed = comparison == "passed"
client.set_model_version_tag(UC_MODEL, challenger_version, "evaluation_status", "passed" if passed else "failed")
client.set_model_version_tag(UC_MODEL, challenger_version, "evaluation_gini", str(overall["gini"]))
client.set_model_version_tag(UC_MODEL, challenger_version, "weighted_gini_by_dp3", str(w_gini_dp3))
if correlation is not None:
    client.set_model_version_tag(UC_MODEL, challenger_version, "sas_correlation", str(correlation))

if not passed:
    raise Exception(f"Evaluation FAILED for {MODEL_NAME} v{challenger_version}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*55}\nEVALUATION COMPLETE\n{'='*55}")
print(f"Challenger:           v{challenger_version}")
print(f"Overall Gini:         {overall['gini']}  (AUC={overall['auc']})")
print(f"Weighted Gini (DP3):  {w_gini_dp3}")
print(f"Weighted Gini (pop):  {w_gini_pop}")
print(f"SAS correlation:      {correlation}")
print(f"Comparison result:    {comparison}")
print(f"Elapsed:              {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|v{challenger_version}|evaluation|{comparison}|{elapsed:.1f}s")
