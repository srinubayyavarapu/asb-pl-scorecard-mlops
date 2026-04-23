# Databricks notebook source

# MAGIC %md
# MAGIC # 06 - Model Deployment
# MAGIC Credit Card Behaviour Scorecard
# MAGIC
# MAGIC Promote `@Challenger` → `@Champion` in Unity Catalog.
# MAGIC Gated on the `approval` tag set by notebook 05.

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
# MAGIC ## Verify Approval

# COMMAND ----------

start_time = datetime.now()

mlflow.set_registry_uri("databricks-uc")
client = mlflow.tracking.MlflowClient()

challenger_info = client.get_model_version_by_alias(UC_MODEL, "Challenger")
challenger_version = challenger_info.version
_raw_tags = getattr(challenger_info, "tags", None) or {}
tags = dict(_raw_tags) if isinstance(_raw_tags, dict) else {t.key: t.value for t in _raw_tags}

if tags.get(APPROVAL_TAG_KEY) != APPROVAL_TAG_VALUE:
    raise Exception(
        f"v{challenger_version} not approved. Tag '{APPROVAL_TAG_KEY}'='{tags.get(APPROVAL_TAG_KEY, 'not_set')}', expected '{APPROVAL_TAG_VALUE}'."
    )

print(f"Challenger v{challenger_version}: APPROVED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record Previous Champion (Audit)

# COMMAND ----------

old_champion_version = None
try:
    old_champion_info = client.get_model_version_by_alias(UC_MODEL, "Champion")
    old_champion_version = old_champion_info.version
    print(f"Previous Champion: v{old_champion_version}")
except Exception:
    print("No previous Champion — first deployment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote @Challenger → @Champion

# COMMAND ----------

client.set_registered_model_alias(UC_MODEL, "Champion", challenger_version)
print(f"Promoted: v{challenger_version} → @Champion")

# Remove @Challenger alias — a promoted version should carry only @Champion.
# A fresh training run will register the next version and set @Challenger on it.
try:
    client.delete_registered_model_alias(UC_MODEL, "Challenger")
    print(f"Removed @Challenger alias (v{challenger_version} is now @Champion only)")
except Exception as e:
    print(f"delete @Challenger note: {type(e).__name__}: {str(e)[:120]}")

now = datetime.now().isoformat()
client.set_model_version_tag(UC_MODEL, challenger_version, "promoted_at", now)
client.set_model_version_tag(UC_MODEL, challenger_version, "promoted_environment", environment)

if old_champion_version:
    client.set_model_version_tag(UC_MODEL, challenger_version, "previous_champion_version", str(old_champion_version))
    client.set_model_version_tag(UC_MODEL, old_champion_version, "replaced_at", now)
    client.set_model_version_tag(UC_MODEL, old_champion_version, "replaced_by_version", str(challenger_version))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = (datetime.now() - start_time).total_seconds()
print(f"\n{'='*50}\nDEPLOYMENT COMPLETE\n{'='*50}")
print(f"Promoted:    v{challenger_version} → @Champion")
print(f"Previous:    {'v' + str(old_champion_version) if old_champion_version else 'None'}")
print(f"Environment: {environment}")
print(f"Elapsed:     {elapsed:.1f}s")

dbutils.notebook.exit(f"SUCCESS|{MODEL_NAME}|v{challenger_version}|deployment|{elapsed:.1f}s")
