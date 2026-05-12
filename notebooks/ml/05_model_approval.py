# Databricks notebook source

# MAGIC %md
# MAGIC # 05 - Model Approval
# MAGIC PL Application Scorecard
# MAGIC
# MAGIC Approval gate for promoting `@Challenger` -> `@Champion`.
# MAGIC
# MAGIC - **dev / stg**: auto-approved
# MAGIC - **prod**: requires human approval via Unity Catalog tag
# MAGIC   (`approval=approved` on the model version) — satisfies APRA's
# MAGIC   independent-validation requirement.

# COMMAND ----------

from datetime import datetime
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog (bundle passes ${var.catalog})")
dbutils.widgets.text("environment", "", "Environment (stg/prod) — bundle passes ${var.environment}")
dbutils.widgets.text("model_name", "", "UC model (auto-set by Deployment Jobs)")
dbutils.widgets.text("model_version", "", "Version (auto-set by Deployment Jobs)")

catalog = dbutils.widgets.get("catalog").strip()
environment = dbutils.widgets.get("environment").strip()
spark.sql(f"USE CATALOG {catalog}")

MODEL_NAME = "application_scorecard"
UC_MODEL_DEFAULT = f"{catalog}.pl_scorecard.application_scorecard"
UC_MODEL   = dbutils.widgets.get("model_name").strip() or UC_MODEL_DEFAULT

DEPLOY_JOB_VERSION = dbutils.widgets.get("model_version").strip()

APPROVAL_TAG_KEY   = "approval"
APPROVAL_TAG_VALUE = "approved"

print(f"Model:       {MODEL_NAME}")
print(f"UC Model:    {UC_MODEL}")
print(f"Environment: {environment}")
print(f"Trigger:     {'MLflow 3 Deployment Job (v' + DEPLOY_JOB_VERSION + ')' if DEPLOY_JOB_VERSION else 'manual (uses @Challenger alias)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load @Challenger + Verify Evaluation Passed

# COMMAND ----------

start_time = datetime.now()
mlflow.set_registry_uri("databricks-uc")
client = mlflow.tracking.MlflowClient()

if DEPLOY_JOB_VERSION:
    challenger_version = DEPLOY_JOB_VERSION
    challenger_info = client.get_model_version(UC_MODEL, challenger_version)
else:
    challenger_info = client.get_model_version_by_alias(UC_MODEL, "Challenger")
    challenger_version = challenger_info.version

_raw_tags = getattr(challenger_info, "tags", None) or {}
tags = dict(_raw_tags) if isinstance(_raw_tags, dict) else {t.key: t.value for t in _raw_tags}
print(f"Challenger v{challenger_version}  Tags: {tags}")

eval_status = tags.get("evaluation_status", "unknown")
if eval_status != "passed":
    raise Exception(
        f"Cannot approve v{challenger_version}: evaluation_status='{eval_status}'. "
        f"Run 04_model_evaluation first."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approve

# COMMAND ----------

if environment == "stg":
    client.set_model_version_tag(UC_MODEL, challenger_version, APPROVAL_TAG_KEY, APPROVAL_TAG_VALUE)
    client.set_model_version_tag(UC_MODEL, challenger_version, "approved_by", f"auto_{environment}")
    client.set_model_version_tag(UC_MODEL, challenger_version, "approved_at", datetime.now().isoformat())
    method = "auto_approved"
    print(f"Auto-approved for {environment}")
else:  # prod
    if DEPLOY_JOB_VERSION:
        _refetched = client.get_model_version(UC_MODEL, challenger_version)
    else:
        _refetched = client.get_model_version_by_alias(UC_MODEL, "Challenger")
    _raw2 = _refetched.tags or {}
    tags = dict(_raw2) if isinstance(_raw2, dict) else {t.key: t.value for t in _raw2}
    current = tags.get(APPROVAL_TAG_KEY, "not_set")
    if current != APPROVAL_TAG_VALUE:
        raise Exception(
            f"v{challenger_version} not approved for prod.\n"
            f"  Current '{APPROVAL_TAG_KEY}' tag: '{current}'\n"
            f"  Required: '{APPROVAL_TAG_VALUE}'\n"
            f"  Approve via UC: Catalog Explorer -> Models -> {UC_MODEL} -> v{challenger_version} -> Tags"
        )
    method = "manually_approved"
    print(f"Approval confirmed (manual): {tags.get('approved_by', 'unknown')}")

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*55}\nAPPROVAL COMPLETE\n{'='*55}")
print(f"Version:     v{challenger_version}")
print(f"Environment: {environment}")
print(f"Method:      {method}")
print(f"Elapsed:     {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|v{challenger_version}|approval|{method}|{elapsed:.1f}s")
