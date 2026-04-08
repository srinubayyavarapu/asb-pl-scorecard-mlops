# Databricks notebook source

# MAGIC %md
# MAGIC # Logger
# MAGIC Shared logging utility for all notebooks.
# MAGIC Usage: `%run ../utils/logger`

# COMMAND ----------

import logging
from datetime import datetime

# COMMAND ----------

def get_logger(name, level=logging.INFO):
    """
    Create a simple logger for notebook use.

    Args:
        name: Logger name (usually the notebook/module name)
        level: Logging level (default: INFO)

    Returns:
        logging.Logger: Configured logger

    Example:
        logger = get_logger("model_training")
        logger.info("Training started")
        logger.warning("High PSI detected")
        logger.error("Model validation failed")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if %run is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter(
            "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

# COMMAND ----------

def log_step(logger, step_name, status="STARTED"):
    """
    Log a pipeline step with consistent formatting.

    Args:
        logger: Logger instance
        step_name: Name of the step (e.g., "Data Ingestion", "WoE Calculation")
        status: Step status (STARTED, COMPLETED, FAILED, SKIPPED)

    Example:
        log_step(logger, "Feature Engineering", "STARTED")
        # ... do work ...
        log_step(logger, "Feature Engineering", "COMPLETED")
    """
    separator = "=" * 50
    logger.info(f"{separator}")
    logger.info(f"STEP: {step_name} | STATUS: {status}")
    logger.info(f"{separator}")

# COMMAND ----------

def log_dataframe_info(logger, df, name="DataFrame"):
    """
    Log basic info about a Spark DataFrame.

    Args:
        logger: Logger instance
        df: Spark DataFrame
        name: Name to identify the DataFrame in logs

    Example:
        log_dataframe_info(logger, customer_df, "Customer Data")
        # Logs: "Customer Data | Rows: 50000 | Columns: 25"
    """
    row_count = df.count()
    col_count = len(df.columns)
    logger.info(f"{name} | Rows: {row_count:,} | Columns: {col_count}")
