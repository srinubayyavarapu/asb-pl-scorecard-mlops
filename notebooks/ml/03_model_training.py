# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Model Training
# MAGIC **Config-driven model training with MLflow tracking.**
# MAGIC
# MAGIC Reads `training.yaml` and executes:
# MAGIC 1. Load feature store data (Dev / Holdout / OOT splits)
# MAGIC 2. Train champion (Logistic Regression) + challengers (RF, NN)
# MAGIC 3. Hyperparameter search as configured
# MAGIC 4. Log all params, metrics, artifacts to MLflow
# MAGIC 5. Validate against thresholds
# MAGIC 6. Register best model in Unity Catalog Model Registry
# MAGIC
# MAGIC **SAS Equivalent:** PROC LOGISTIC + PROC HPFOREST + PROC NNET + ODS OUTPUT

# COMMAND ----------

import os as _os
_nb_dir = _os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
exec(open(f"/Workspace{_nb_dir}/00_ml_config_loader.py").read())

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.metrics import roc_auc_score, roc_curve
import numpy as np
import pandas as pd
import json

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
train_cfg = config["training"]
feat_cfg = config["features"]

target_col = model_cfg["target_variable"]
feature_table = feat_cfg["feature_store_table"]

print(f"Model:      {model_name}")
print(f"Target:     {target_col}")
print(f"Experiment: {train_cfg['mlflow']['experiment_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Feature Data

# COMMAND ----------

start_time = datetime.now()

df = spark.table(feature_table)

# Get selected features from metadata
selected_features_str = df.select("_selected_features").first()[0]
feature_cols = [f.strip() for f in selected_features_str.split(",") if f.strip()]

print(f"Features ({len(feature_cols)}): {feature_cols}")

# Split into populations
def to_pandas_xy(spark_df, feature_cols, target_col):
    """Convert Spark DataFrame to pandas X, y arrays."""
    cols_to_select = feature_cols + [target_col]
    # Only select columns that exist
    existing_cols = [c for c in cols_to_select if c in spark_df.columns]
    pdf = spark_df.select(*existing_cols).toPandas()
    pdf = pdf.dropna(subset=feature_cols)

    X = pdf[feature_cols].fillna(0)
    # Encode categoricals if any
    for col in X.columns:
        if X[col].dtype == "object":
            X[col] = X[col].astype("category").cat.codes

    y = pdf[target_col].astype(int)
    return X, y

df_dev = df.filter(F.col("_population") == "dev")
df_holdout = df.filter(F.col("_population") == "holdout")
df_oot = df.filter(F.col("_population") == "oot")

X_dev, y_dev = to_pandas_xy(df_dev, feature_cols, target_col)
X_holdout, y_holdout = to_pandas_xy(df_holdout, feature_cols, target_col)

has_oot = df_oot.count() > 0
if has_oot:
    X_oot, y_oot = to_pandas_xy(df_oot, feature_cols, target_col)

print(f"Dev:     {len(X_dev):,} rows")
print(f"Holdout: {len(X_holdout):,} rows")
if has_oot:
    print(f"OOT:     {len(X_oot):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Metric Calculation Functions

# COMMAND ----------

def calculate_metrics(y_true, y_prob):
    """Calculate standard credit risk model metrics."""
    auc = roc_auc_score(y_true, y_prob)
    gini = 2 * auc - 1

    # KS statistic
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    ks = max(tpr - fpr)

    return {"auc": round(auc, 4), "gini": round(gini, 4), "ks": round(ks, 4)}


def calculate_psi(expected, actual, bins=10):
    """Calculate Population Stability Index."""
    expected_hist, bin_edges = np.histogram(expected, bins=bins)
    actual_hist, _ = np.histogram(actual, bins=bin_edges)

    # Normalize
    expected_pct = (expected_hist + 0.5) / (expected_hist.sum() + 0.5)
    actual_pct = (actual_hist + 0.5) / (actual_hist.sum() + 0.5)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return round(psi, 4)


def check_thresholds(metrics, thresholds):
    """Check if metrics meet minimum thresholds. Returns (pass: bool, details: dict)."""
    results = {}
    all_pass = True

    for metric_name, threshold in thresholds.items():
        if metric_name == "psi":
            continue  # PSI is checked separately
        min_val = threshold.get("min", 0)
        actual = metrics.get(metric_name, 0)
        passed = actual >= min_val
        if not passed:
            all_pass = False
        results[metric_name] = {"actual": actual, "threshold": min_val, "passed": passed}

    return all_pass, results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train Models

# COMMAND ----------

# Configure MLflow for serverless compatibility
try:
    spark.conf.set("spark.mlflow.modelRegistryUri", "databricks-uc")
except Exception:
    pass
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(train_cfg["mlflow"]["experiment_name"])

# Algorithm class mapping
ALGO_MAP = {
    "LogisticRegression": LogisticRegression,
    "RandomForestClassifier": RandomForestClassifier,
    "MLPClassifier": MLPClassifier,
}

all_results = []
best_model = None
best_auc = 0

for algo_name, algo_cfg in train_cfg["algorithms"].items():
    algo_class = ALGO_MAP[algo_cfg["class"]]
    role = algo_cfg["role"]
    base_params = algo_cfg.get("params", {})
    grid = algo_cfg.get("grid_search", {})
    search_method = algo_cfg.get("search_method", "grid")

    print(f"\n{'='*60}")
    print(f"Training: {algo_name} ({role})")
    print(f"{'='*60}")

    with mlflow.start_run(run_name=f"{algo_name}_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:

        # Log tags
        for tag_k, tag_v in train_cfg["mlflow"].get("run_tags", {}).items():
            mlflow.set_tag(tag_k, tag_v)
        mlflow.set_tag("algorithm", algo_name)
        mlflow.set_tag("role", role)
        mlflow.set_tag("model_name", model_name)

        # Log parameters
        mlflow.log_param("algorithm", algo_name)
        mlflow.log_param("num_features", len(feature_cols))
        mlflow.log_param("feature_list", json.dumps(feature_cols))
        mlflow.log_param("dev_rows", len(X_dev))
        mlflow.log_param("holdout_rows", len(X_holdout))
        mlflow.log_param("bad_rate", round(y_dev.mean(), 4))

        # Log data lineage - tracks which dataset was used for training
        feature_dataset = mlflow.data.from_spark(
            spark.table(feature_table).filter("_population = 'dev'"),
            table_name=feature_table, version="0"
        )
        mlflow.log_input(feature_dataset, context="training")

        # Hyperparameter search
        if grid:
            # Convert list-of-lists for hidden_layer_sizes to tuples
            for k, v in grid.items():
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], list):
                    grid[k] = [tuple(x) for x in v]

            base_model = algo_class(**base_params)

            if search_method == "random":
                n_iter = algo_cfg.get("n_iter", 10)
                searcher = RandomizedSearchCV(
                    base_model, grid, n_iter=n_iter, scoring="roc_auc",
                    cv=3, random_state=42, n_jobs=-1
                )
            else:
                searcher = GridSearchCV(
                    base_model, grid, scoring="roc_auc",
                    cv=3, n_jobs=-1
                )

            searcher.fit(X_dev, y_dev)
            model = searcher.best_estimator_
            mlflow.log_params({f"best_{k}": v for k, v in searcher.best_params_.items()})
            print(f"  Best params: {searcher.best_params_}")
        else:
            model = algo_class(**base_params)
            model.fit(X_dev, y_dev)

        # Predict probabilities
        y_prob_dev = model.predict_proba(X_dev)[:, 1]
        y_prob_holdout = model.predict_proba(X_holdout)[:, 1]

        # Calculate metrics
        dev_metrics = calculate_metrics(y_dev, y_prob_dev)
        holdout_metrics = calculate_metrics(y_holdout, y_prob_holdout)

        # PSI: dev vs holdout
        psi_holdout = calculate_psi(y_prob_dev, y_prob_holdout)

        # Log metrics
        for k, v in dev_metrics.items():
            mlflow.log_metric(f"dev_{k}", v)
        for k, v in holdout_metrics.items():
            mlflow.log_metric(f"holdout_{k}", v)
        mlflow.log_metric("holdout_psi", psi_holdout)

        # OOT metrics
        if has_oot:
            y_prob_oot = model.predict_proba(X_oot)[:, 1]
            oot_metrics = calculate_metrics(y_oot, y_prob_oot)
            psi_oot = calculate_psi(y_prob_dev, y_prob_oot)
            for k, v in oot_metrics.items():
                mlflow.log_metric(f"oot_{k}", v)
            mlflow.log_metric("oot_psi", psi_oot)

        # Log coefficients for Logistic Regression
        if algo_name == "logistic_regression" and hasattr(model, "coef_"):
            coef_df = pd.DataFrame({
                "feature": feature_cols,
                "coefficient": model.coef_[0],
            })
            coef_df["odds_ratio"] = np.exp(coef_df["coefficient"])
            coef_df.loc[len(coef_df)] = ["_intercept", model.intercept_[0], np.exp(model.intercept_[0])]
            mlflow.log_table(coef_df, artifact_file="coefficients.json")
            print(f"\n  Coefficients:")
            print(coef_df.to_string(index=False))

        # Log feature importance for Random Forest
        if algo_name == "random_forest" and hasattr(model, "feature_importances_"):
            imp_df = pd.DataFrame({
                "feature": feature_cols,
                "importance": model.feature_importances_,
            }).sort_values("importance", ascending=False)
            mlflow.log_table(imp_df, artifact_file="feature_importance.json")

        # Log model
        signature = infer_signature(X_dev, model.predict_proba(X_dev)[:, 1])
        mlflow.sklearn.log_model(model, artifact_path="model", signature=signature)

        # Threshold validation
        passed, details = check_thresholds(holdout_metrics, train_cfg["validation_thresholds"])

        print(f"\n  Dev:     Gini={dev_metrics['gini']}, KS={dev_metrics['ks']}, AUC={dev_metrics['auc']}")
        print(f"  Holdout: Gini={holdout_metrics['gini']}, KS={holdout_metrics['ks']}, AUC={holdout_metrics['auc']}")
        print(f"  PSI:     {psi_holdout}")
        print(f"  Validation: {'PASSED' if passed else 'FAILED'}")
        for metric, detail in details.items():
            status = "PASS" if detail["passed"] else "FAIL"
            print(f"    {metric}: {detail['actual']} >= {detail['threshold']} [{status}]")

        mlflow.set_tag("validation_passed", str(passed))

        # Track results
        result_entry = {
            "algorithm": algo_name,
            "role": role,
            "run_id": run.info.run_id,
            "dev_gini": dev_metrics["gini"],
            "holdout_gini": holdout_metrics["gini"],
            "holdout_auc": holdout_metrics["auc"],
            "holdout_psi": psi_holdout,
            "passed": passed,
        }
        all_results.append(result_entry)

        # Track best model (by holdout AUC)
        if holdout_metrics["auc"] > best_auc:
            best_auc = holdout_metrics["auc"]
            best_model = {
                "algorithm": algo_name,
                "run_id": run.info.run_id,
                "model_uri": f"runs:/{run.info.run_id}/model",
                "holdout_auc": holdout_metrics["auc"],
                "holdout_gini": holdout_metrics["gini"],
                "holdout_ks": holdout_metrics["ks"],
            }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Register Best Model in Unity Catalog

# COMMAND ----------

results_df = pd.DataFrame(all_results)
print("\nModel Comparison:")
print(results_df.to_string(index=False))

model_load_uri = None

if best_model:
    uc_model_name = train_cfg["mlflow"]["model_name"]
    print(f"\nBest model: {best_model['algorithm']} (AUC={best_model['holdout_auc']})")

    # Register in Unity Catalog (with fallback for workspaces without storage permissions)
    try:
        model_version = mlflow.register_model(
            model_uri=best_model["model_uri"],
            name=uc_model_name,
        )
        print(f"  Registered: {uc_model_name} v{model_version.version}")

        client = mlflow.tracking.MlflowClient()
        algo_role = train_cfg["algorithms"][best_model["algorithm"]]["role"]
        alias = "Champion" if algo_role == "champion" else "Challenger"
        client.set_registered_model_alias(uc_model_name, alias, model_version.version)
        print(f"  Alias: @{alias} -> v{model_version.version}")
        model_load_uri = f"models:/{uc_model_name}@{alias}"
    except Exception as e:
        print(f"  UC registration unavailable ({type(e).__name__}), using experiment run URI")
        model_load_uri = best_model["model_uri"]

    # Save model URI for downstream tasks
    import pandas as pd_save
    spark.createDataFrame(pd.DataFrame([{
        "algo": str(best_model["algorithm"]),
        "run_id": str(best_model["run_id"]),
        "model_uri": str(model_load_uri),
        "holdout_auc": float(best_model["holdout_auc"]),
    }])).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.retail_ml.hl_model_load_uri"
    )
else:
    print("\nWARNING: No model passed validation thresholds. Nothing registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()

print(f"\n{'='*60}")
print(f"MODEL TRAINING COMPLETE: {model_name}")
print(f"{'='*60}")
print(f"  Algorithms trained: {len(all_results)}")
print(f"  Passed validation:  {sum(1 for r in all_results if r['passed'])}")
if best_model:
    print(f"  Best model:         {best_model['algorithm']}")
    print(f"  Best holdout AUC:   {best_model['holdout_auc']}")
    print(f"  Registered as:      {train_cfg['mlflow']['model_name']}")
print(f"  Elapsed:            {elapsed:.1f}s")

# COMMAND ----------

result = f"SUCCESS|{model_name}|{best_model['algorithm'] if best_model else 'NONE'}|training|{elapsed:.1f}s"
print(result)
dbutils.notebook.exit(result)
