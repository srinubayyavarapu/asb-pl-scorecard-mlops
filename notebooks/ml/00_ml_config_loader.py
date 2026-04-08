# Databricks notebook 

# MAGIC %md
# MAGIC # ML Config Loader
# MAGIC Shared utility for loading ML framework configs. Imported via %run by all ML notebooks.
# MAGIC
# MAGIC Supports two modes:
# MAGIC - **File-based**: reads YAML configs from repo (Repos/DAB/local)
# MAGIC - **Embedded**: uses inline configs when files are not accessible (Community Edition)

# COMMAND ----------

import os

# COMMAND ----------

# -- Embedded configs (used when YAML files are not accessible on workspace) --
# In production with Repos/DAB, these are ignored and YAML files are read instead.

EMBEDDED_CONFIGS = {
    "hl_behaviour_scorecard": {
        "model": {
            "description": "Home Loan Customer Behaviour Scorecard - PD estimation",
            "product": "home_loan",
            "use_case": "credit_risk_pd",
            "target_variable": "default_flag",
            "primary_key": "account_key",
            "champion_algorithm": "logistic_regression",
            "challenger_algorithms": ["random_forest", "neural_network"],
            "config_dir": "configs/ml/hl_behaviour_scorecard",
            "is_active": True,
        },
        "data_prep": {
            "source_tables": {
                "base": "${catalog}.retail_silver.hl_sample_data",
            },
            "joins": [],
            "good_bad_definition": {
                "target_column": "default_flag",
                "bad_condition": "default_flag = 1",
                "performance_window_months": 6,
                "observation_window": {"start": "2021-01-01", "end": "2024-06-30"},
                "exclusions": ["account_status = 'CLOSED'", "months_on_book < 6"],
            },
            "sampling": {
                "dev_ratio": 0.70,
                "holdout_ratio": 0.30,
                "oot_start_date": "2023-07-01",
                "stratify_by": "default_flag",
                "random_seed": 42,
            },
            "output_table": "${catalog}.retail_gold.hl_scorecard_dev_data",
        },
        "features": {
            "iv_thresholds": {"exclude_below": 0.02, "weak_below": 0.10, "suspicious_above": 0.50},
            "features": [
                {"name": "credit_score", "type": "continuous", "banding_method": "auto", "monotonicity": "descending"},
                {"name": "lvr", "type": "continuous", "banding_method": "auto", "monotonicity": "ascending"},
                {"name": "annual_income", "type": "continuous", "banding_method": "auto", "monotonicity": "descending"},
                {"name": "loan_amount", "type": "continuous", "banding_method": "auto", "monotonicity": "ascending"},
                {"name": "interest_rate", "type": "continuous", "banding_method": "auto", "monotonicity": "ascending"},
                {"name": "months_on_book", "type": "continuous", "banding_method": "auto", "monotonicity": "descending"},
                {"name": "property_value", "type": "continuous", "banding_method": "auto", "monotonicity": "descending"},
            ],
            "auto_banding": {"max_bins": 10, "min_bin_size": 0.05, "min_bad_count": 20},
            "banding_table": "${catalog}.retail_gold.hl_banding_lookup",
            "woe_iv_table": "${catalog}.retail_gold.hl_woe_iv",
            "feature_store_table": "${catalog}.retail_ml.hl_feature_store",
        },
        "training": {
            "mlflow": {
                "experiment_name": "/Shared/ml/hl_behaviour_scorecard_experiments",
                "model_name": "${catalog}.retail_ml.hl_behaviour_scorecard",
                "run_tags": {"product": "home_loan", "use_case": "credit_risk_pd", "regulatory_framework": "APRA_RBNZ"},
            },
            "algorithms": {
                "logistic_regression": {
                    "library": "sklearn", "class": "LogisticRegression", "role": "champion",
                    "params": {"penalty": "l2", "solver": "lbfgs", "max_iter": 1000},
                    "grid_search": {"C": [0.01, 0.1, 1.0, 10.0]},
                    "search_method": "grid",
                },
                "random_forest": {
                    "library": "sklearn", "class": "RandomForestClassifier", "role": "challenger",
                    "params": {"random_state": 42},
                    "grid_search": {"n_estimators": [100, 200], "max_depth": [5, 10]},
                    "search_method": "grid",
                },
                "neural_network": {
                    "library": "sklearn", "class": "MLPClassifier", "role": "challenger",
                    "params": {"activation": "relu", "max_iter": 500, "random_state": 42},
                    "grid_search": {"hidden_layer_sizes": [(64, 32), (128, 64)]},
                    "search_method": "grid",
                },
            },
            "validation_thresholds": {
                "gini": {"min": 0.40, "populations": ["dev", "holdout", "oot"]},
                "ks": {"min": 0.25, "populations": ["dev", "holdout", "oot"]},
                "auc": {"min": 0.70, "populations": ["dev", "holdout", "oot"]},
                "psi": {"green": 0.10, "amber": 0.25, "populations": ["holdout", "oot"]},
            },
            "sas_equivalence": {
                "coefficient_tolerance": 0.01, "gini_tolerance": 0.02,
                "score_psi_tolerance": 0.10, "pd_tolerance": 0.001, "risk_band_tolerance": 0.01,
            },
        },
        "scoring": {
            "scorecard_scaling": {"target_score": 600, "target_odds": 50, "pdo": 20},
            "risk_grades": [
                {"grade": "A1", "score_min": 750, "score_max": 999, "pd_min": 0.0, "pd_max": 0.005},
                {"grade": "A2", "score_min": 700, "score_max": 749, "pd_min": 0.005, "pd_max": 0.01},
                {"grade": "B1", "score_min": 650, "score_max": 699, "pd_min": 0.01, "pd_max": 0.025},
                {"grade": "B2", "score_min": 600, "score_max": 649, "pd_min": 0.025, "pd_max": 0.05},
                {"grade": "C1", "score_min": 550, "score_max": 599, "pd_min": 0.05, "pd_max": 0.1},
                {"grade": "C2", "score_min": 500, "score_max": 549, "pd_min": 0.1, "pd_max": 0.2},
                {"grade": "D", "score_min": 0, "score_max": 499, "pd_min": 0.2, "pd_max": 1.0},
            ],
            "input_table": "${catalog}.retail_gold.hl_scorecard_dev_data",
            "output_table": "${catalog}.retail_gold.hl_scored_output",
            "schedule": "monthly",
        },
        "monitoring": {
            "metrics": ["gini", "ks", "auc", "psi"],
            "drift_thresholds": {
                "psi": {"warning": 0.10, "critical": 0.25},
                "gini_drop": {"warning": 0.05, "critical": 0.10},
                "ks_drop": {"warning": 0.05, "critical": 0.10},
            },
            "monitoring_table": "${catalog}.retail_ml.hl_monitoring_log",
            "baseline_table": "${catalog}.retail_ml.hl_monitoring_baseline",
            "alert_on_warning": False,
            "alert_on_critical": True,
        },
    }
}

# COMMAND ----------

def _resolve_catalog(value, catalog):
    """Replace ${catalog} placeholder in config values."""
    if isinstance(value, str):
        return value.replace("${catalog}", catalog)
    elif isinstance(value, dict):
        return {k: _resolve_catalog(v, catalog) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_catalog(item, catalog) for item in value]
    return value


def _try_load_yaml():
    """Try to find and load YAML configs from filesystem. Returns repo_root or None."""
    try:
        import yaml
    except ImportError:
        return None

    possible_roots = [
        "/Workspace/Repos/bayyavarapusrinu1@gmail.com/ASB_SAS_to_DBX_MLOPS",
        "/Workspace/ASB-Migration",
    ]
    for root in possible_roots:
        if os.path.exists(os.path.join(root, "configs", "ml", "model_registry.yaml")):
            return root
    return None


def load_model_config(model_name, catalog="asb_dev"):
    """
    Load the full config for a model.
    Tries YAML files first, falls back to embedded configs.
    """
    repo_root = _try_load_yaml()

    if repo_root:
        # File-based loading (production path)
        import yaml
        print(f"Loading config from: {repo_root}")
        reg_path = os.path.join(repo_root, "configs", "ml", "model_registry.yaml")
        with open(reg_path) as f:
            registry = yaml.safe_load(f)
        model_def = registry["models"][model_name]
        config_dir = os.path.join(repo_root, model_def["config_dir"])

        result = {"model": model_def}
        for phase in ["data_prep", "features", "training", "scoring", "monitoring"]:
            phase_path = os.path.join(config_dir, f"{phase}.yaml")
            if os.path.exists(phase_path):
                with open(phase_path) as f:
                    result[phase] = yaml.safe_load(f)
            else:
                result[phase] = None
    else:
        # Embedded config (Community Edition / no file access)
        print(f"Loading embedded config for: {model_name}")
        if model_name not in EMBEDDED_CONFIGS:
            raise ValueError(f"Model '{model_name}' not found. Available: {list(EMBEDDED_CONFIGS.keys())}")
        import copy
        result = copy.deepcopy(EMBEDDED_CONFIGS[model_name])

    # Resolve catalog placeholders
    result = _resolve_catalog(result, catalog)
    return result


def get_catalog():
    """Get the catalog name from widget or default."""
    try:
        return dbutils.widgets.get("catalog")
    except Exception:
        return "asb_dev"
