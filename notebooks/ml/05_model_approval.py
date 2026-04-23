# Databricks notebook source

# MAGIC %md
# MAGIC # 05 - Model Approval
# MAGIC Credit Card Behaviour Scorecard
# MAGIC
# MAGIC Approval gate for promoting `@Challenger` to `@Champion`.
# MAGIC
# MAGIC - **dev / stg**: auto-approved
# MAGIC - **prod**: requires human approval via Unity Catalog tag
# MAGIC   (`approval=approved` on the model version)

# COMMAND ----------

from datetime import datetime
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "asb_dev", "Unity Catalog")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prod)")
catalog = dbutils.widgets.get("catalog").strip()
environment = dbutils.widgets.get("environment").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME = "cc_behaviour_scorecard"
UC_MODEL   = f"{catalog}.retail_ml.cc_behaviour_scorecard"

APPROVAL_TAG_KEY   = "approval"
APPROVAL_TAG_VALUE = "approved"

print(f"Model:       {MODEL_NAME}")
print(f"UC Model:    {UC_MODEL}")
print(f"Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load @Challenger + Check Evaluation Status

# COMMAND ----------

start_time = datetime.now()

mlflow.set_registry_uri("databricks-uc")
client = mlflow.tracking.MlflowClient()

challenger_info = client.get_model_version_by_alias(UC_MODEL, "Challenger")
challenger_version = challenger_info.version
_raw_tags = getattr(challenger_info, "tags", None) or {}
tags = dict(_raw_tags) if isinstance(_raw_tags, dict) else {t.key: t.value for t in _raw_tags}

print(f"Challenger: v{challenger_version}")
print(f"Tags: {tags}")

eval_status = tags.get("evaluation_status", "unknown")
if eval_status != "passed":
    raise Exception(
        f"Cannot approve v{challenger_version}: evaluation_status='{eval_status}'. Run 04_model_evaluation first."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approve

# COMMAND ----------

if environment in ("dev", "stg"):
    client.set_model_version_tag(UC_MODEL, challenger_version, APPROVAL_TAG_KEY, APPROVAL_TAG_VALUE)
    client.set_model_version_tag(UC_MODEL, challenger_version, "approved_by", f"auto_{environment}")
    client.set_model_version_tag(UC_MODEL, challenger_version, "approved_at", datetime.now().isoformat())

    method = "auto_approved"
    print(f"Auto-approved for {environment}")

else:  # prod
    # Re-fetch tags in case they were updated externally
    _raw2 = client.get_model_version_by_alias(UC_MODEL, "Challenger").tags or {}
    tags = dict(_raw2) if isinstance(_raw2, dict) else {t.key: t.value for t in _raw2}
    current = tags.get(APPROVAL_TAG_KEY, "not_set")

    if current != APPROVAL_TAG_VALUE:
        raise Exception(
            f"v{challenger_version} not approved for prod.\n"
            f"  Current '{APPROVAL_TAG_KEY}' tag: '{current}'\n"
            f"  Required: '{APPROVAL_TAG_VALUE}'\n"
            f"  Approve via UC: Catalog Explorer → Models → {UC_MODEL} → v{challenger_version} → Tags"
        )

    method = "manually_approved"
    print(f"Approval confirmed (manual): {tags.get('approved_by', 'unknown')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nAPPROVAL COMPLETE\n{'='*50}")
print(f"Version:     v{challenger_version}")
print(f"Environment: {environment}")
print(f"Method:      {method}")
print(f"Elapsed:     {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|v{challenger_version}|approval|{method}|{elapsed:.1f}s")
