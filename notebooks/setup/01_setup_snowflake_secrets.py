# Databricks notebook source

# MAGIC %md
# MAGIC # Setup Snowflake Secret Scope (JDBC-only ingestion)
# MAGIC
# MAGIC Creates the Databricks secret scope `asb_sf` used by the ETL ingestion
# MAGIC notebook (`notebooks/etl/01_ingest_snowflake.py`) to connect to Snowflake
# MAGIC via the Spark Snowflake connector.
# MAGIC
# MAGIC **Run this manually once per workspace (dev / stg / prod) before
# MAGIC deploying the bundle.**
# MAGIC
# MAGIC Federation is not used in this project — JDBC only. No `CREATE
# MAGIC CONNECTION` / `CREATE FOREIGN CATALOG` required.
# MAGIC
# MAGIC ## Two ways to create the secret scope
# MAGIC
# MAGIC ### Option A — Databricks CLI (preferred, runs from your laptop)
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope asb_sf --profile <your-profile>
# MAGIC
# MAGIC databricks secrets put-secret asb_sf account  --string-value "CQZORVY-YZ26298"
# MAGIC databricks secrets put-secret asb_sf user     --string-value "SRINUBAYYAVARAPU3657"
# MAGIC databricks secrets put-secret asb_sf password --string-value "<password>"
# MAGIC ```
# MAGIC
# MAGIC ### Option B — from this notebook (uses workspace API client)
# MAGIC
# MAGIC Edit the SECRETS dict below and run the cell. Requires a workspace
# MAGIC user with permission to manage secret scopes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Option B) In-notebook creation
# MAGIC
# MAGIC Prefer Option A. Use this only if you don't have the CLI configured.

# COMMAND ----------

SCOPE = "asb_sf"

# Edit these values before running. DO NOT commit secrets to git.
SECRETS = {
    "account":  "CQZORVY-YZ26298",
    "user":     "SRINUBAYYAVARAPU3657",
    "password": "<paste-password-here>",
}

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create scope if missing
existing_scopes = [s.name for s in w.secrets.list_scopes()]
if SCOPE not in existing_scopes:
    w.secrets.create_scope(scope=SCOPE)
    print(f"Created secret scope: {SCOPE}")
else:
    print(f"Secret scope already exists: {SCOPE}")

# Put each secret
for key, value in SECRETS.items():
    if value.startswith("<") and value.endswith(">"):
        print(f"  SKIP {key}: placeholder value — edit the cell and rerun")
        continue
    w.secrets.put_secret(scope=SCOPE, key=key, string_value=value)
    print(f"  PUT  {SCOPE}/{key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("Scopes:")
for s in w.secrets.list_scopes():
    print(f"  {s.name}")

print(f"\nKeys in {SCOPE}:")
for k in w.secrets.list_secrets(scope=SCOPE):
    print(f"  {k.key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick connection sanity check
# MAGIC
# MAGIC Reads `SELECT CURRENT_VERSION()` from Snowflake using the scope. If this
# MAGIC fails, fix the secret values before deploying the ETL bundle.

# COMMAND ----------

sf_options = {
    "sfURL":       f"https://{dbutils.secrets.get(SCOPE, 'account')}.snowflakecomputing.com",
    "sfUser":      dbutils.secrets.get(SCOPE, "user"),
    "sfPassword":  dbutils.secrets.get(SCOPE, "password"),
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "ACCOUNTADMIN",
    "sfDatabase":  "DP_CreditApplication",
    "sfSchema":    "MART",
}

try:
    df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("query", "SELECT CURRENT_VERSION() AS sf_version, CURRENT_USER() AS sf_user")
        .load()
    )
    df.show(truncate=False)
    print("Connection OK — secret scope works.")
except Exception as e:
    print(f"Connection FAILED: {type(e).__name__}: {str(e)[:200]}")
    print("Fix secrets above or re-run with corrected values before deploying the bundle.")
