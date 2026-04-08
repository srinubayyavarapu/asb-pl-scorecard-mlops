# Databricks notebook source

# MAGIC %md
# MAGIC # Workspace Initialization
# MAGIC Creates Unity Catalog catalogs, schemas, and sets up permissions.
# MAGIC
# MAGIC **Run this once** when setting up a new environment (DEV/STG/PROD).
# MAGIC
# MAGIC Prerequisites:
# MAGIC - Unity Catalog metastore attached to workspace
# MAGIC - Admin privileges to create catalogs and schemas

# COMMAND ----------

# MAGIC %run ../utils/config_loader

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

logger = get_logger("init_workspace")
env = get_env_config()

logger.info(f"Initializing workspace for environment: {env['environment']}")
logger.info(f"Catalog: {env['catalog']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog

# COMMAND ----------

catalog = env["catalog"]

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

logger.info(f"Catalog '{catalog}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schemas (Medallion + ML)

# COMMAND ----------

schemas = {
    "bronze": env["bronze_schema"],
    "silver": env["silver_schema"],
    "gold": env["gold_schema"],
    "ml": env["ml_schema"],
}

for layer, schema_name in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
    logger.info(f"Schema '{catalog}.{schema_name}' ready ({layer} layer)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Schema Comments (Documentation)

# COMMAND ----------

schema_comments = {
    env["bronze_schema"]: "Raw data ingested from Snowflake. No transformations applied.",
    env["silver_schema"]: "Cleansed, conformed, and type-cast data. PII masking applied.",
    env["gold_schema"]: "Business-ready, ML-ready datasets. Aggregated and denormalized.",
    env["ml_schema"]: "ML assets: feature tables, model artifacts, scoring outputs, monitoring.",
}

for schema_name, comment in schema_comments.items():
    spark.sql(f"COMMENT ON SCHEMA {catalog}.{schema_name} IS '{comment}'")

logger.info("Schema comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create ML Sub-Tables Structure
# MAGIC These tables will be created by ML notebooks, but we set up the schema here.

# COMMAND ----------

# Feature tables, scoring output, and monitoring tables
# will be created dynamically by the ML pipeline notebooks.
# We just ensure the ml schema exists (done above).

logger.info("ML schema ready for feature tables, model registry, and scoring outputs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Setup

# COMMAND ----------

# List all schemas in the catalog
schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
log_step(logger, "Workspace Initialization", "COMPLETED")

display(schemas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Item | Status |
# MAGIC |------|--------|
# MAGIC | Catalog | Created |
# MAGIC | Bronze Schema | Created |
# MAGIC | Silver Schema | Created |
# MAGIC | Gold Schema | Created |
# MAGIC | ML Schema | Created |
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Configure Snowflake connection (JDBC secrets or Federation catalog)
# MAGIC 2. Run `01_ingest_snowflake` to start data ingestion
