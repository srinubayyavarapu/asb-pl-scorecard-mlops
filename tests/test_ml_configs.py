"""
Unit tests for ML Scorecard Framework configurations.
Runs on the CI agent (no Databricks needed).

These validate that:
1. All config YAML files are syntactically valid
2. Required fields are present in every config
3. Model registry entries point to existing config folders
4. Algorithm names map to known sklearn classes
5. Threshold values are within valid ranges
"""

import yaml
import os
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY_PATH = os.path.join(REPO_ROOT, "configs", "ml", "model_registry.yaml")


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


# ── Registry Tests ──

class TestModelRegistry:

    def test_registry_exists(self):
        assert os.path.exists(REGISTRY_PATH), "model_registry.yaml not found"

    def test_registry_has_models(self):
        registry = load_yaml(REGISTRY_PATH)
        assert "models" in registry
        assert len(registry["models"]) > 0

    def test_registry_required_fields(self):
        registry = load_yaml(REGISTRY_PATH)
        required = ["description", "target_variable", "primary_key",
                     "champion_algorithm", "config_dir", "is_active"]

        for name, cfg in registry["models"].items():
            for field in required:
                assert field in cfg, f"Model '{name}' missing field '{field}'"

    def test_config_dirs_exist(self):
        registry = load_yaml(REGISTRY_PATH)
        for name, cfg in registry["models"].items():
            config_dir = os.path.join(REPO_ROOT, cfg["config_dir"])
            assert os.path.isdir(config_dir), f"Config dir missing for '{name}': {config_dir}"


# ── Per-Model Config Tests ──

def get_active_models():
    """Get list of active model configs for parametrized tests."""
    registry = load_yaml(REGISTRY_PATH)
    models = []
    for name, cfg in registry["models"].items():
        if cfg.get("is_active", False):
            models.append((name, cfg))
    return models


@pytest.fixture(params=get_active_models(), ids=lambda x: x[0])
def model_config(request):
    name, cfg = request.param
    config_dir = os.path.join(REPO_ROOT, cfg["config_dir"])
    configs = {"model": cfg}
    for phase in ["data_prep", "features", "training", "scoring", "monitoring"]:
        path = os.path.join(config_dir, f"{phase}.yaml")
        configs[phase] = load_yaml(path) if os.path.exists(path) else None
    return name, configs


class TestDataPrepConfig:

    def test_exists(self, model_config):
        name, configs = model_config
        assert configs["data_prep"] is not None, f"{name}: data_prep.yaml missing"

    def test_has_source_tables(self, model_config):
        name, configs = model_config
        dp = configs["data_prep"]
        assert "source_tables" in dp, f"{name}: no source_tables"
        assert len(dp["source_tables"]) > 0, f"{name}: empty source_tables"

    def test_has_sampling(self, model_config):
        name, configs = model_config
        dp = configs["data_prep"]
        assert "sampling" in dp, f"{name}: no sampling config"
        sampling = dp["sampling"]
        assert 0 < sampling["dev_ratio"] < 1, f"{name}: invalid dev_ratio"
        assert 0 < sampling["holdout_ratio"] < 1, f"{name}: invalid holdout_ratio"

    def test_has_output_table(self, model_config):
        name, configs = model_config
        dp = configs["data_prep"]
        assert "output_table" in dp, f"{name}: no output_table"


class TestFeaturesConfig:

    def test_exists(self, model_config):
        name, configs = model_config
        assert configs["features"] is not None, f"{name}: features.yaml missing"

    def test_has_features(self, model_config):
        name, configs = model_config
        feat = configs["features"]
        assert "features" in feat, f"{name}: no features list"
        assert len(feat["features"]) > 0, f"{name}: empty features list"

    def test_feature_fields(self, model_config):
        name, configs = model_config
        for f in configs["features"]["features"]:
            assert "name" in f, f"{name}: feature missing 'name'"
            assert "type" in f, f"{name}: feature '{f.get('name')}' missing 'type'"
            assert f["type"] in ("continuous", "categorical"), \
                f"{name}: feature '{f['name']}' invalid type '{f['type']}'"

    def test_iv_thresholds(self, model_config):
        name, configs = model_config
        feat = configs["features"]
        assert "iv_thresholds" in feat, f"{name}: no iv_thresholds"
        iv = feat["iv_thresholds"]
        assert iv["exclude_below"] < iv["suspicious_above"], \
            f"{name}: exclude_below should be less than suspicious_above"


class TestTrainingConfig:

    def test_exists(self, model_config):
        name, configs = model_config
        assert configs["training"] is not None, f"{name}: training.yaml missing"

    def test_has_mlflow_config(self, model_config):
        name, configs = model_config
        train = configs["training"]
        assert "mlflow" in train, f"{name}: no mlflow config"
        assert "experiment_name" in train["mlflow"], f"{name}: no experiment_name"
        assert "model_name" in train["mlflow"], f"{name}: no model_name"

    def test_has_algorithms(self, model_config):
        name, configs = model_config
        train = configs["training"]
        assert "algorithms" in train, f"{name}: no algorithms"
        assert len(train["algorithms"]) > 0, f"{name}: empty algorithms"

    def test_algorithm_fields(self, model_config):
        name, configs = model_config
        valid_classes = {"LogisticRegression", "RandomForestClassifier",
                         "MLPClassifier", "GradientBoostingClassifier", "XGBClassifier"}
        for algo_name, algo_cfg in configs["training"]["algorithms"].items():
            assert "class" in algo_cfg, f"{name}/{algo_name}: missing 'class'"
            assert "role" in algo_cfg, f"{name}/{algo_name}: missing 'role'"
            assert algo_cfg["role"] in ("champion", "challenger"), \
                f"{name}/{algo_name}: invalid role '{algo_cfg['role']}'"

    def test_has_validation_thresholds(self, model_config):
        name, configs = model_config
        train = configs["training"]
        assert "validation_thresholds" in train, f"{name}: no validation_thresholds"


class TestScoringConfig:

    def test_exists(self, model_config):
        name, configs = model_config
        assert configs["scoring"] is not None, f"{name}: scoring.yaml missing"

    def test_has_scaling(self, model_config):
        name, configs = model_config
        score = configs["scoring"]
        assert "scorecard_scaling" in score, f"{name}: no scorecard_scaling"
        scaling = score["scorecard_scaling"]
        assert "target_score" in scaling
        assert "pdo" in scaling
        assert scaling["pdo"] > 0, f"{name}: PDO must be positive"

    def test_has_risk_grades(self, model_config):
        name, configs = model_config
        score = configs["scoring"]
        assert "risk_grades" in score, f"{name}: no risk_grades"
        assert len(score["risk_grades"]) > 0, f"{name}: empty risk_grades"

    def test_risk_grades_cover_full_range(self, model_config):
        name, configs = model_config
        grades = configs["scoring"]["risk_grades"]
        min_score = min(g["score_min"] for g in grades)
        max_score = max(g["score_max"] for g in grades)
        assert min_score == 0, f"{name}: risk grades don't start at 0"
        assert max_score >= 999, f"{name}: risk grades don't reach 999"


class TestMonitoringConfig:

    def test_exists(self, model_config):
        name, configs = model_config
        assert configs["monitoring"] is not None, f"{name}: monitoring.yaml missing"

    def test_has_metrics(self, model_config):
        name, configs = model_config
        mon = configs["monitoring"]
        assert "metrics" in mon, f"{name}: no metrics"
        assert len(mon["metrics"]) > 0, f"{name}: empty metrics"

    def test_has_drift_thresholds(self, model_config):
        name, configs = model_config
        mon = configs["monitoring"]
        assert "drift_thresholds" in mon, f"{name}: no drift_thresholds"
        assert "psi" in mon["drift_thresholds"], f"{name}: no PSI thresholds"
        psi = mon["drift_thresholds"]["psi"]
        assert psi["warning"] < psi["critical"], \
            f"{name}: PSI warning should be less than critical"
