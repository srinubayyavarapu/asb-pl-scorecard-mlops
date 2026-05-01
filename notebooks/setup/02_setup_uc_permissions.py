# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Unity Catalog Cross-Environment Permissions
# MAGIC **Sets up access controls aligned with Big Book of MLOps best practices (p.24, 35, 42).**
# MAGIC
# MAGIC ## Environment Isolation Model
# MAGIC
# MAGIC | Catalog | Write Access | Read Access |
# MAGIC |---------|-------------|-------------|
# MAGIC | `dev_retail_modelling` | Data scientists, service principals | All dev users |
# MAGIC | `stg_retail_modelling` | CI/CD service principal only | Dev users (for debugging test failures) |
# MAGIC | `prod_retail_modelling` | CD service principal + admins only | Dev users (read-only for debugging & model comparison) |
# MAGIC
# MAGIC ## What This Enables
# MAGIC
# MAGIC > "Data scientists in the development environment can be granted read-only access to
# MAGIC > data and AI assets from the production environment... detect and debug model quality
# MAGIC > degradation by examining production monitoring tables, deep dive on model predictions
# MAGIC > using production inference tables, and easily compare in-development models with
# MAGIC > live production models." — Big Book of MLOps, p.11
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Must be run by a UC admin or account admin
# MAGIC - All three catalogs (dev_retail_modelling, stg_retail_modelling, prod_retail_modelling) must exist
# MAGIC - Service principal for CI/CD must exist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("dev_catalog", "dev_retail_modelling", "Development catalog")
dbutils.widgets.text("stg_catalog", "stg_retail_modelling", "Staging catalog")
dbutils.widgets.text("prod_catalog", "prod_retail_modelling", "Production catalog")
dbutils.widgets.text("cicd_sp", "", "CI/CD Service Principal (application ID or name)")
dbutils.widgets.text("ds_group", "data_scientists", "Data scientists group name")

dev_catalog = dbutils.widgets.get("dev_catalog").strip()
stg_catalog = dbutils.widgets.get("stg_catalog").strip()
prod_catalog = dbutils.widgets.get("prod_catalog").strip()
cicd_sp = dbutils.widgets.get("cicd_sp").strip()
ds_group = dbutils.widgets.get("ds_group").strip()

print(f"Dev catalog:  {dev_catalog}")
print(f"Stg catalog:  {stg_catalog}")
print(f"Prod catalog: {prod_catalog}")
print(f"CI/CD SP:     {cicd_sp or '(not set)'}")
print(f"DS group:     {ds_group}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalogs (if not exist)

# COMMAND ----------

for cat in [dev_catalog, stg_catalog, prod_catalog]:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
        print(f"Catalog ensured: {cat}")
    except Exception as e:
        print(f"Catalog {cat}: {type(e).__name__}: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schemas in Each Catalog
# MAGIC
# MAGIC Replicates the same schema structure across dev/stg/prod (Big Book of MLOps p.23).

# COMMAND ----------

schemas = ["bronze", "silver", "gold", "pl_scorecard"]

for cat in [dev_catalog, stg_catalog, prod_catalog]:
    for schema in schemas:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{schema}")
            print(f"  {cat}.{schema}")
        except Exception as e:
            print(f"  {cat}.{schema}: {type(e).__name__}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Dev Catalog Permissions
# MAGIC
# MAGIC - Data scientists: full read-write for experimentation
# MAGIC - CI/CD SP: full access for automated testing

# COMMAND ----------

if ds_group:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {dev_catalog} TO `{ds_group}`",
        f"GRANT USE SCHEMA ON CATALOG {dev_catalog} TO `{ds_group}`",
        f"GRANT SELECT ON CATALOG {dev_catalog} TO `{ds_group}`",
        f"GRANT CREATE TABLE ON CATALOG {dev_catalog} TO `{ds_group}`",
        f"GRANT CREATE MODEL ON CATALOG {dev_catalog} TO `{ds_group}`",
        f"GRANT CREATE FUNCTION ON CATALOG {dev_catalog} TO `{ds_group}`",
    ]
    print(f"\nDev catalog grants for {ds_group}:")
    for g in grants:
        try:
            spark.sql(g)
            print(f"  {g.split(' TO ')[0]}")
        except Exception as e:
            print(f"  SKIP: {type(e).__name__}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Staging Catalog Permissions
# MAGIC
# MAGIC - CI/CD SP: full write access (deploys and runs integration tests)
# MAGIC - Data scientists: read-only (for debugging integration test failures)

# COMMAND ----------

if cicd_sp:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {stg_catalog} TO `{cicd_sp}`",
        f"GRANT USE SCHEMA ON CATALOG {stg_catalog} TO `{cicd_sp}`",
        f"GRANT ALL PRIVILEGES ON CATALOG {stg_catalog} TO `{cicd_sp}`",
    ]
    print(f"\nStg catalog grants for CI/CD SP ({cicd_sp}):")
    for g in grants:
        try:
            spark.sql(g)
            print(f"  {g.split(' TO ')[0]}")
        except Exception as e:
            print(f"  SKIP: {type(e).__name__}: {str(e)[:100]}")

if ds_group:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {stg_catalog} TO `{ds_group}`",
        f"GRANT USE SCHEMA ON CATALOG {stg_catalog} TO `{ds_group}`",
        f"GRANT SELECT ON CATALOG {stg_catalog} TO `{ds_group}`",
    ]
    print(f"\nStg catalog read-only grants for {ds_group}:")
    for g in grants:
        try:
            spark.sql(g)
            print(f"  {g.split(' TO ')[0]}")
        except Exception as e:
            print(f"  SKIP: {type(e).__name__}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Production Catalog Permissions (Big Book of MLOps p.24, 42)
# MAGIC
# MAGIC - CI/CD SP: full write access (deploys jobs, runs training in prod)
# MAGIC - Data scientists: **read-only** — can examine prod models, inference tables,
# MAGIC   monitoring tables for debugging, and load @Champion for comparison
# MAGIC
# MAGIC > "Data scientists usually do not have write or compute access in the production
# MAGIC > environment. However, it is important to provide them with visibility to test
# MAGIC > results, logs, model artifacts and the status of ML pipelines in production."
# MAGIC > — Big Book of MLOps, p.42

# COMMAND ----------

if cicd_sp:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {prod_catalog} TO `{cicd_sp}`",
        f"GRANT USE SCHEMA ON CATALOG {prod_catalog} TO `{cicd_sp}`",
        f"GRANT ALL PRIVILEGES ON CATALOG {prod_catalog} TO `{cicd_sp}`",
    ]
    print(f"\nProd catalog grants for CI/CD SP ({cicd_sp}):")
    for g in grants:
        try:
            spark.sql(g)
            print(f"  {g.split(' TO ')[0]}")
        except Exception as e:
            print(f"  SKIP: {type(e).__name__}: {str(e)[:100]}")

if ds_group:
    grants = [
        # Catalog + schema USE for navigation
        f"GRANT USE CATALOG ON CATALOG {prod_catalog} TO `{ds_group}`",
        f"GRANT USE SCHEMA ON CATALOG {prod_catalog} TO `{ds_group}`",
        # Read-only: tables, models, functions
        f"GRANT SELECT ON CATALOG {prod_catalog} TO `{ds_group}`",
        # Execute models (load @Champion for comparison in dev)
        f"GRANT EXECUTE MODEL ON CATALOG {prod_catalog} TO `{ds_group}`",
    ]
    print(f"\nProd catalog read-only grants for {ds_group}:")
    for g in grants:
        try:
            spark.sql(g)
            print(f"  {g.split(' TO ')[0]}")
        except Exception as e:
            print(f"  SKIP: {type(e).__name__}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"UC PERMISSIONS SETUP COMPLETE")
print(f"{'='*60}")
print(f"""
Environment Isolation:
  {dev_catalog}  — DS: read-write, SP: full
  {stg_catalog}  — DS: read-only, SP: full (CI/CD)
  {prod_catalog} — DS: read-only, SP: full (CD)

What data scientists can do from dev workspace:
  - Read prod tables: spark.table("{prod_catalog}.gold.<table>")
  - Load prod models:  mlflow.sklearn.load_model("models:/{prod_catalog}.pl_scorecard.<model>@Champion")
  - Query monitoring:  spark.table("{prod_catalog}.pl_scorecard.<model>_monitoring_log")
  - Compare models:    Load @Champion from prod, compare vs dev @Challenger

What data scientists CANNOT do from dev:
  - Write to {stg_catalog} or {prod_catalog}
  - Register models in {prod_catalog}
  - Modify production job definitions
""")

dbutils.notebook.exit("SUCCESS")
