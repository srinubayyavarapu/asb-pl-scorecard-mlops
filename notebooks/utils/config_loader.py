# Databricks notebook source

# MAGIC %md
# MAGIC # Config Loader
# MAGIC Shared utility to load YAML configuration files.
# MAGIC Usage: `%run ../utils/config_loader`

# COMMAND ----------

import yaml
import os

# COMMAND ----------

def load_config(config_name, config_type="models"):
    """
    Load a YAML config file from the configs/ directory.

    Args:
        config_name: Name of the config file (without .yml extension)
        config_type: Subdirectory under configs/ (models, ingestion, monitoring)

    Returns:
        dict: Parsed YAML configuration

    Example:
        config = load_config("home_loans_scorecard", "models")
        config = load_config("snowflake_tables", "ingestion")
        config = load_config("thresholds", "monitoring")
    """
    # In Databricks, configs are relative to the repo root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(repo_root, "configs", config_type, f"{config_name}.yml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config

# COMMAND ----------

def get_env_config():
    """
    Get environment-specific configuration from Databricks widgets.
    These are set by DAB via databricks.yml variables.

    Returns:
        dict: Environment configuration with catalog, schemas, etc.
    """
    # No hardcoded fallback — bundle MUST pass these via job parameters / widgets.
    # An interactive run that hasn't set them will surface the missing-widget
    # exception instead of silently targeting a stale catalog.
    return {
        "environment":  dbutils.widgets.get("environment"),
        "catalog":      dbutils.widgets.get("catalog"),
        "bronze_schema": dbutils.widgets.get("bronze_schema"),
        "silver_schema": dbutils.widgets.get("silver_schema"),
        "gold_schema":  dbutils.widgets.get("gold_schema"),
        "ml_schema":    dbutils.widgets.get("ml_schema"),
    }

# COMMAND ----------

def get_table_path(env_config, layer, table_name):
    """
    Build a fully qualified Unity Catalog table path.

    Args:
        env_config: Environment config from get_env_config()
        layer: Data layer - "bronze", "silver", "gold", or "ml"
        table_name: Name of the table

    Returns:
        str: Fully qualified path like "<catalog>.silver.customer_data"

    Example:
        path = get_table_path(env, "silver", "customer_data")
        # Returns: "<catalog>.silver.customer_data"
    """
    schema_key = f"{layer}_schema" if layer != "ml" else "ml_schema"
    schema = env_config[schema_key]
    catalog = env_config["catalog"]
    return f"{catalog}.{schema}.{table_name}"
