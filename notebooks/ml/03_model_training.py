# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Model Training
# MAGIC Credit Card Behaviour Scorecard
# MAGIC
# MAGIC 1. Train Champion (Logistic Regression) + Challengers (Random Forest, Neural Network)
# MAGIC 2. GridSearchCV hyperparameter tuning
# MAGIC 3. Log to MLflow with UC Feature Engineering lineage
# MAGIC 4. Register best model as `@Challenger` in Unity Catalog

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow, mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score, roc_curve
import numpy as np
import pandas as pd
import json
import time

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
UC_MODEL      = f"{catalog}.retail_ml.cc_behaviour_scorecard"
EXPERIMENT    = "/Shared/ml/cc_behaviour_scorecard_experiments"

ALGORITHMS = {
    "logistic_regression": {
        "class": LogisticRegression, "role": "champion",
        "params": {"penalty": "l2", "solver": "lbfgs", "max_iter": 1000},
        "grid":   {"C": [0.01, 0.1, 1.0, 10.0]},
    },
    "random_forest": {
        "class": RandomForestClassifier, "role": "challenger",
        "params": {"random_state": 42},
        "grid":   {"n_estimators": [100, 200], "max_depth": [5, 10]},
    },
    "neural_network": {
        "class": MLPClassifier, "role": "challenger",
        "params": {"activation": "relu", "max_iter": 500, "random_state": 42},
        "grid":   {"hidden_layer_sizes": [(64, 32), (128, 64)]},
    },
}

# Minimum performance thresholds for the challenger to be registered
MIN_HOLDOUT_AUC = 0.70

print(f"Model:      {MODEL_NAME}")
print(f"UC Model:   {UC_MODEL}")
print(f"Experiment: {EXPERIMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Feature Data

# COMMAND ----------

start_time = datetime.now()

df = spark.table(FEATURE_TABLE)
feature_cols = [f.strip() for f in df.select("_selected_features").first()[0].split(",") if f.strip()]
print(f"Features ({len(feature_cols)}): {feature_cols}")

def to_xy(spark_df):
    cols = feature_cols + [TARGET]
    pdf = spark_df.select(*cols).toPandas().dropna(subset=feature_cols)
    X = pdf[feature_cols].fillna(0)
    for c in X.columns:
        if X[c].dtype == "object":
            X[c] = X[c].astype("category").cat.codes
    return X, pdf[TARGET].astype(int)

df_dev = df.filter(F.col("_population") == "dev")
df_holdout = df.filter(F.col("_population") == "holdout")

X_dev, y_dev = to_xy(df_dev)
X_holdout, y_holdout = to_xy(df_holdout)

print(f"Dev:     {len(X_dev):,}")
print(f"Holdout: {len(X_holdout):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics

# COMMAND ----------

def metrics(y_true, y_prob):
    auc = roc_auc_score(y_true, y_prob)
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    return {"auc": round(auc, 4), "gini": round(2 * auc - 1, 4), "ks": round(max(tpr - fpr), 4)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Champion + Challengers

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

results = []
best_model = None
best_auc = 0

for algo_name, cfg in ALGORITHMS.items():
    print(f"\n{'='*50}\nTraining: {algo_name} ({cfg['role']})\n{'='*50}")

    with mlflow.start_run(run_name=f"{algo_name}_{datetime.now():%Y%m%d_%H%M}") as run:
        mlflow.set_tag("model_name", MODEL_NAME)
        mlflow.set_tag("algorithm", algo_name)
        mlflow.set_tag("role", cfg["role"])
        mlflow.log_param("num_features", len(feature_cols))
        mlflow.log_param("feature_list", json.dumps(feature_cols))
        mlflow.log_param("dev_rows", len(X_dev))
        mlflow.log_param("bad_rate", round(y_dev.mean(), 4))

        # Data lineage
        dataset = mlflow.data.from_spark(
            spark.table(FEATURE_TABLE).filter("_population = 'dev'"),
            table_name=FEATURE_TABLE, version="0",
        )
        mlflow.log_input(dataset, context="training")

        # GridSearchCV
        searcher = GridSearchCV(
            cfg["class"](**cfg["params"]), cfg["grid"],
            scoring="roc_auc", cv=3, n_jobs=-1,
        )
        searcher.fit(X_dev, y_dev)
        model = searcher.best_estimator_
        mlflow.log_params({f"best_{k}": v for k, v in searcher.best_params_.items()})
        print(f"Best params: {searcher.best_params_}")

        y_prob_dev = model.predict_proba(X_dev)[:, 1]
        y_prob_hol = model.predict_proba(X_holdout)[:, 1]
        dev_m = metrics(y_dev, y_prob_dev)
        hol_m = metrics(y_holdout, y_prob_hol)

        for k, v in dev_m.items():
            mlflow.log_metric(f"dev_{k}", v)
        for k, v in hol_m.items():
            mlflow.log_metric(f"holdout_{k}", v)

        print(f"Dev:     Gini={dev_m['gini']}, KS={dev_m['ks']}, AUC={dev_m['auc']}")
        print(f"Holdout: Gini={hol_m['gini']}, KS={hol_m['ks']}, AUC={hol_m['auc']}")

        # Coefficients for LR
        if algo_name == "logistic_regression" and hasattr(model, "coef_"):
            coef_df = pd.DataFrame({"feature": feature_cols, "coefficient": model.coef_[0]})
            coef_df["odds_ratio"] = np.exp(coef_df["coefficient"])
            mlflow.log_table(coef_df, artifact_file="coefficients.json")

        # Log model as pure sklearn flavor (loadable via mlflow.sklearn.load_model)
        signature = infer_signature(X_dev, y_prob_dev)
        mlflow.sklearn.log_model(model, artifact_path="model", signature=signature)

        passed = hol_m["auc"] >= MIN_HOLDOUT_AUC
        mlflow.set_tag("validation_passed", str(passed))
        print(f"Threshold: holdout AUC {hol_m['auc']} >= {MIN_HOLDOUT_AUC} → {'PASS' if passed else 'FAIL'}")

        results.append({
            "algorithm": algo_name, "role": cfg["role"], "run_id": run.info.run_id,
            "dev_gini": dev_m["gini"], "holdout_gini": hol_m["gini"],
            "holdout_auc": hol_m["auc"], "passed": passed,
        })

        if passed and hol_m["auc"] > best_auc:
            best_auc = hol_m["auc"]
            best_model = {
                "algorithm": algo_name, "run_id": run.info.run_id,
                "uri": f"runs:/{run.info.run_id}/model",
                "holdout_auc": hol_m["auc"], "holdout_gini": hol_m["gini"],
            }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model as @Challenger in Unity Catalog

# COMMAND ----------

print("\nModel Comparison:")
print(pd.DataFrame(results).to_string(index=False))

if not best_model:
    raise Exception("No model passed validation — nothing registered.")

print(f"\nBest: {best_model['algorithm']} (AUC={best_model['holdout_auc']})")

client = mlflow.tracking.MlflowClient()
mv = mlflow.register_model(model_uri=best_model["uri"], name=UC_MODEL)

# Wait for READY
for _ in range(30):
    if client.get_model_version(UC_MODEL, mv.version).status == "READY":
        break
    time.sleep(10)

client.set_registered_model_alias(UC_MODEL, "Challenger", mv.version)
client.set_model_version_tag(UC_MODEL, mv.version, "training_run_id", best_model["run_id"])
client.set_model_version_tag(UC_MODEL, mv.version, "training_auc", str(best_model["holdout_auc"]))

print(f"Registered: {UC_MODEL} v{mv.version} → @Challenger")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nTRAINING COMPLETE\n{'='*50}")
print(f"Algorithms:   {len(results)}  (passed: {sum(1 for r in results if r['passed'])})")
print(f"Best:         {best_model['algorithm']} (AUC={best_model['holdout_auc']})")
print(f"Registered:   {UC_MODEL} v{mv.version} @Challenger")
print(f"Elapsed:      {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|{best_model['algorithm']}|training|{elapsed:.1f}s")
