# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Model Training
# MAGIC PL Application Scorecard
# MAGIC
# MAGIC 1. Initial fit: Logistic Regression on **Funded** DEV (Bad/Good only)
# MAGIC 2. Score Rejected applicants (NotFunded) with the initial model
# MAGIC 3. **Reject Inference** — assign inferred labels to Rejected (parcelling-style:
# MAGIC    duplicate-and-weight by predicted Bad probability)
# MAGIC 4. Final fit: LR on Funded ⊕ inferred-Rejected
# MAGIC 5. Log coefficients + p-values + odds ratios
# MAGIC 6. Register best model as `@Challenger` in Unity Catalog
# MAGIC
# MAGIC LR is the regulatory-blessed choice per ASB walkthrough — no challenger algorithms here.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow, mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, roc_curve
import numpy as np
import pandas as pd
import json
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog (bundle passes ${var.catalog})")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME    = "application_scorecard"
TARGET        = "target_flag"

FEATURE_TABLE = f"{catalog}.pl_scorecard.feature_store"
UC_MODEL      = f"{catalog}.pl_scorecard.application_scorecard"
EXPERIMENT    = "/Shared/ml/pl_scorecard/application_scorecard"

# Reject inference: parcelling-style augmentation.
# For each Rejected applicant, duplicate as 2 rows: one labelled Good (weighted 1-p_bad),
# one labelled Bad (weighted p_bad). LR uses sample_weight to honour the duplication.
REJECT_INFERENCE_ENABLED = True

# Minimum holdout AUC to register.
# Set to 0.55 for the synthetic demo dataset — the bad rate on the
# generated facility population is ~1.7% (145 Bads / 8.3k funded), so DEV
# only carries ~100 Bads after the population split. The MLOps lifecycle
# (gate, Challenger/Champion, monitoring) is what we're demoing here, not
# absolute model accuracy. For real ASB data this should sit at 0.65+.
MIN_HOLDOUT_AUC = 0.55

# LR config — penalty=l2 with C=1.0 is the regulatory baseline
LR_PARAMS = {"penalty": "l2", "C": 1.0, "solver": "lbfgs", "max_iter": 1000}

print(f"Model:      {MODEL_NAME}")
print(f"UC Model:   {UC_MODEL}")
print(f"Experiment: {EXPERIMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Feature Data

# COMMAND ----------

start_time = datetime.now()

df = spark.table(FEATURE_TABLE)
selected_features = [
    f.strip() for f in df.select("_selected_features").first()[0].split(",") if f.strip()
]
print(f"Selected features ({len(selected_features)}): {selected_features}")


def to_xy_funded(spark_df):
    """Convert a Funded subset (Bad/Good) to numeric X/y arrays.
    Categoricals are integer-encoded via .cat.codes (label encoding).
    """
    cols = selected_features + [TARGET]
    pdf = spark_df.select(*cols).toPandas().dropna(subset=selected_features)
    X = pdf[selected_features].copy()
    for c in X.columns:
        if X[c].dtype == "object":
            X[c] = X[c].astype("category").cat.codes
    y = (pdf[TARGET] == "Bad").astype(int)
    return X.fillna(0), y, pdf.index


def to_x_rejected(spark_df):
    """Same conversion but no target (Rejected has no Funded outcome)."""
    cols = selected_features
    pdf = spark_df.select(*cols, "application_id").toPandas().dropna(subset=selected_features)
    X = pdf[selected_features].copy()
    for c in X.columns:
        if X[c].dtype == "object":
            X[c] = X[c].astype("category").cat.codes
    return X.fillna(0), pdf["application_id"].values

# Funded DEV (Bad/Good only — Indeterminate excluded as ambiguous)
df_dev_funded = (
    df.filter(F.col("_population") == "DEV")
      .filter(F.col(TARGET).isin("Bad", "Good"))
)
df_hol_funded = (
    df.filter(F.col("_population") == "HOL")
      .filter(F.col(TARGET).isin("Bad", "Good"))
)
df_oot_funded = (
    df.filter(F.col("_population") == "OOT")
      .filter(F.col(TARGET).isin("Bad", "Good"))
)
# Rejected (TTD-only, NotFunded)
df_rejected = df.filter(F.col(TARGET) == "Rejected")

X_dev, y_dev, _ = to_xy_funded(df_dev_funded)
X_hol, y_hol, _ = to_xy_funded(df_hol_funded)
X_oot, y_oot, _ = to_xy_funded(df_oot_funded)
X_rej, rej_ids  = to_x_rejected(df_rejected)

print(f"\nDEV  Funded (Bad+Good): {len(X_dev):,}  bad_rate={y_dev.mean():.3f}")
print(f"HOL  Funded:            {len(X_hol):,}  bad_rate={y_hol.mean():.3f}")
print(f"OOT  Funded:            {len(X_oot):,}  bad_rate={y_oot.mean():.3f}")
print(f"Rejected (TTD):         {len(X_rej):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics + Coefficient Table Helpers

# COMMAND ----------

def credit_metrics(y, p):
    auc = roc_auc_score(y, p)
    fpr, tpr, _ = roc_curve(y, p)
    return {"auc": round(auc, 4), "gini": round(2 * auc - 1, 4), "ks": round(max(tpr - fpr), 4)}


def lr_coefficient_table(model, feature_names, X, y, sample_weight=None):
    """
    Compute LR coefficients with approximate Wald-style p-values from
    the diagonal of (X^T W X)^-1, weighted by predicted variance per row.
    """
    coef = model.coef_[0]
    intercept = model.intercept_[0]
    p_pred = model.predict_proba(X)[:, 1]
    W = np.diag(p_pred * (1 - p_pred) * (sample_weight if sample_weight is not None else np.ones(len(p_pred))))
    Xb = np.hstack([np.ones((X.shape[0], 1)), X.values])
    try:
        cov = np.linalg.pinv(Xb.T @ W @ Xb)
        std_err = np.sqrt(np.diag(cov))[1:]   # skip intercept
        z = coef / std_err
        # 2-tailed normal p-value
        from scipy.stats import norm
        p_val = 2 * (1 - norm.cdf(np.abs(z)))
    except Exception:
        std_err = np.full_like(coef, np.nan)
        z       = np.full_like(coef, np.nan)
        p_val   = np.full_like(coef, np.nan)

    return pd.DataFrame({
        "feature": feature_names,
        "coefficient": coef.round(6),
        "std_err":     std_err.round(6),
        "z":           z.round(4),
        "p_value":     p_val.round(4),
        "odds_ratio":  np.exp(coef).round(4),
    }), intercept

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

with mlflow.start_run(run_name=f"pl_lr_{datetime.now():%Y%m%d_%H%M}") as run:
    mlflow.set_tag("model_name", MODEL_NAME)
    mlflow.set_tag("algorithm", "logistic_regression")
    mlflow.set_tag("reject_inference", str(REJECT_INFERENCE_ENABLED))
    mlflow.log_param("num_features", len(selected_features))
    mlflow.log_param("feature_list", json.dumps(selected_features))
    mlflow.log_param("dev_rows", len(X_dev))
    mlflow.log_param("dev_bad_rate", round(y_dev.mean(), 4))
    mlflow.log_params({f"lr_{k}": v for k, v in LR_PARAMS.items()})

    # Data lineage
    dataset = mlflow.data.from_spark(
        spark.table(FEATURE_TABLE).filter("_population = 'DEV'"),
        table_name=FEATURE_TABLE, version="0",
    )
    mlflow.log_input(dataset, context="training_funded_only")

    # ── Step 1: Initial fit on Funded only ────────────────────────────────
    initial = Pipeline([
        ("scaler", StandardScaler()),
        ("lr",     LogisticRegression(**LR_PARAMS)),
    ])
    initial.fit(X_dev, y_dev)
    p_dev_init = initial.predict_proba(X_dev)[:, 1]
    p_hol_init = initial.predict_proba(X_hol)[:, 1]

    init_dev = credit_metrics(y_dev, p_dev_init)
    init_hol = credit_metrics(y_hol, p_hol_init)
    print(f"\n[Initial — Funded only]")
    print(f"  DEV  AUC={init_dev['auc']}  Gini={init_dev['gini']}  KS={init_dev['ks']}")
    print(f"  HOL  AUC={init_hol['auc']}  Gini={init_hol['gini']}  KS={init_hol['ks']}")
    for k, v in init_dev.items(): mlflow.log_metric(f"init_dev_{k}", v)
    for k, v in init_hol.items(): mlflow.log_metric(f"init_hol_{k}", v)

    # ── Step 2/3: Reject Inference (parcelling) ───────────────────────────
    if REJECT_INFERENCE_ENABLED and len(X_rej) > 0:
        p_rej = initial.predict_proba(X_rej)[:, 1]
        # Augment training data: each rejected row appears twice — once labelled Good (1-p_bad weight),
        # once labelled Bad (p_bad weight). LR refit uses sample_weight.
        X_rej_good = X_rej.copy(); y_rej_good = np.zeros(len(X_rej)); w_rej_good = 1.0 - p_rej
        X_rej_bad  = X_rej.copy(); y_rej_bad  = np.ones(len(X_rej));  w_rej_bad  = p_rej

        X_aug = pd.concat([X_dev, X_rej_good, X_rej_bad], axis=0).reset_index(drop=True)
        y_aug = np.concatenate([y_dev.values, y_rej_good, y_rej_bad])
        w_aug = np.concatenate([np.ones(len(X_dev)), w_rej_good, w_rej_bad])

        print(f"\n[Reject Inference] augmented training set: {len(X_aug):,} rows ({len(X_dev):,} funded + 2*{len(X_rej):,} parcelled rejected)")
        mlflow.log_metric("reject_inference_n_rejected", len(X_rej))
        mlflow.log_metric("reject_inference_total_aug_rows", len(X_aug))

        final = Pipeline([
            ("scaler", StandardScaler()),
            ("lr",     LogisticRegression(**LR_PARAMS)),
        ])
        final.fit(X_aug, y_aug, lr__sample_weight=w_aug)
    else:
        print("\n[Reject Inference] DISABLED — using initial Funded-only model")
        final = initial

    # ── Final-model evaluation ────────────────────────────────────────────
    p_dev = final.predict_proba(X_dev)[:, 1]
    p_hol = final.predict_proba(X_hol)[:, 1]
    p_oot = final.predict_proba(X_oot)[:, 1]

    m_dev = credit_metrics(y_dev, p_dev)
    m_hol = credit_metrics(y_hol, p_hol)
    m_oot = credit_metrics(y_oot, p_oot)

    print(f"\n[Final — after reject inference]")
    print(f"  DEV  AUC={m_dev['auc']}  Gini={m_dev['gini']}  KS={m_dev['ks']}")
    print(f"  HOL  AUC={m_hol['auc']}  Gini={m_hol['gini']}  KS={m_hol['ks']}")
    print(f"  OOT  AUC={m_oot['auc']}  Gini={m_oot['gini']}  KS={m_oot['ks']}")
    for k, v in m_dev.items(): mlflow.log_metric(f"final_dev_{k}", v)
    for k, v in m_hol.items(): mlflow.log_metric(f"final_hol_{k}", v)
    for k, v in m_oot.items(): mlflow.log_metric(f"final_oot_{k}", v)

    # ── Coefficient table (logged as MLflow table artifact) ───────────────
    lr = final.named_steps["lr"]
    coef_df, intercept = lr_coefficient_table(lr, selected_features, X_dev, y_dev)
    coef_df.loc[len(coef_df)] = ["__intercept__", round(intercept, 6), None, None, None, None]
    print("\n[Coefficients]")
    print(coef_df.to_string(index=False))
    mlflow.log_table(coef_df, artifact_file="coefficients.json")

    # ── Log model with signature ──────────────────────────────────────────
    signature = infer_signature(X_dev, p_dev)
    mlflow.sklearn.log_model(final, artifact_path="model", signature=signature)
    run_id = run.info.run_id

    passed = m_hol["auc"] >= MIN_HOLDOUT_AUC
    mlflow.set_tag("validation_passed", str(passed))
    print(f"\nThreshold: HOL AUC {m_hol['auc']} >= {MIN_HOLDOUT_AUC} -> {'PASS' if passed else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model as @Challenger

# COMMAND ----------

if not passed:
    raise Exception(f"HOL AUC {m_hol['auc']} below threshold {MIN_HOLDOUT_AUC} — model not registered.")

client = mlflow.tracking.MlflowClient()
mv = mlflow.register_model(model_uri=f"runs:/{run_id}/model", name=UC_MODEL)

for _ in range(30):
    if client.get_model_version(UC_MODEL, mv.version).status == "READY":
        break
    time.sleep(5)

client.set_registered_model_alias(UC_MODEL, "Challenger", mv.version)
client.set_model_version_tag(UC_MODEL, mv.version, "training_run_id", run_id)
client.set_model_version_tag(UC_MODEL, mv.version, "training_hol_auc", str(m_hol["auc"]))
client.set_model_version_tag(UC_MODEL, mv.version, "training_oot_auc", str(m_oot["auc"]))
client.set_model_version_tag(UC_MODEL, mv.version, "reject_inference", str(REJECT_INFERENCE_ENABLED))

print(f"\nRegistered: {UC_MODEL} v{mv.version} -> @Challenger")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*55}\nTRAINING COMPLETE\n{'='*55}")
print(f"Model:      logistic_regression{' + reject_inference' if REJECT_INFERENCE_ENABLED else ''}")
print(f"Holdout:    AUC={m_hol['auc']}  Gini={m_hol['gini']}")
print(f"OOT:        AUC={m_oot['auc']}  Gini={m_oot['gini']}")
print(f"Registered: {UC_MODEL} v{mv.version} @Challenger")
print(f"Elapsed:    {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|v{mv.version}|training|{elapsed:.1f}s")
