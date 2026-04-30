"""Inspect the latest PL Application Scorecard training run + UC model version."""
import os
import mlflow
from mlflow.tracking import MlflowClient

# Use the DEV workspace credentials (DEV profile from ~/.databrickscfg)
os.environ["DATABRICKS_CONFIG_PROFILE"] = "DEV"

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

client = MlflowClient()

UC_MODEL = "asb_dev.retail_ml.pl_application_scorecard"

print("=" * 64)
print("UC Model: " + UC_MODEL)
print("=" * 64)

# Aliases
for alias in ("Challenger", "Champion"):
    try:
        mv = client.get_model_version_by_alias(UC_MODEL, alias)
        print(f"\n@{alias} -> v{mv.version}")
        print(f"  run_id: {mv.run_id}")
        print(f"  status: {mv.status}")
        tags = mv.tags or {}
        if isinstance(tags, dict):
            tag_items = tags.items()
        else:
            tag_items = [(t.key, t.value) for t in tags]
        for k, v in tag_items:
            print(f"  tag {k}: {v}")
    except Exception as e:
        print(f"\n@{alias} -> not set ({e})")

# Pull metrics for the latest version's run
print("\n" + "=" * 64)
print("Latest training run metrics")
print("=" * 64)

# Find the most recent version
all_versions = client.search_model_versions(f"name='{UC_MODEL}'")
latest = max(all_versions, key=lambda mv: int(mv.version))
print(f"Latest version: v{latest.version}  (run_id={latest.run_id})")

run = client.get_run(latest.run_id)
print("\nKey metrics:")
for k in sorted(run.data.metrics.keys()):
    print(f"  {k:<35} {run.data.metrics[k]}")

print("\nKey params:")
for k in ("num_features", "feature_list", "dev_rows", "dev_bad_rate",
          "lr_C", "lr_max_iter", "lr_penalty", "lr_solver"):
    if k in run.data.params:
        v = run.data.params[k]
        print(f"  {k:<25} {v[:80] + ('...' if len(v) > 80 else '')}")

print("\nTags:")
for k in sorted(run.data.tags.keys()):
    if not k.startswith("mlflow."):
        print(f"  {k:<25} {run.data.tags[k]}")
