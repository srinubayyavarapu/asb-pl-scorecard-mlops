# Databricks notebook source

# MAGIC %md
# MAGIC # Job Utilities
# MAGIC Shared utilities for job enforcement and ETL control table management.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Execution Enforcement

# COMMAND ----------

def enforce_job_execution(required_params=None):
    """
    Enforce that this notebook is being run as part of a Databricks job,
    not interactively. Also validates required parameters.

    Args:
        required_params: List of widget parameter names that must be provided

    Raises:
        SystemExit via dbutils.notebook.exit() if validation fails
    """
    required_params = required_params or []
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    # Check if running as part of a job
    try:
        job_id = context.tags().get("jobId").getOrElse(None)
        run_id = context.tags().get("runId").getOrElse(None)
    except Exception:
        job_id = None
        run_id = None

    # Allow interactive runs ONLY if all required params are provided
    # (for testing purposes, but strongly discourage)
    missing_params = []
    for param in required_params:
        try:
            value = dbutils.widgets.get(param).strip()
            if not value:
                missing_params.append(param)
        except Exception:
            missing_params.append(param)

    if missing_params:
        error_msg = f"""
ERROR: This notebook must be run as part of a Databricks Job with required parameters.

Missing required parameters: {missing_params}

How to run this notebook correctly:
1. Create a Databricks Job with this notebook as a task
2. Configure notebook parameters in the job definition:
   - table_name: The target table name from master_table_inventory.csv

3. Run the job via:
   - Databricks UI: Jobs > Your Job > Run Now
   - CLI: databricks jobs run-now --job-id <JOB_ID>
   - API: POST /api/2.1/jobs/run-now

Do NOT run this notebook interactively without parameters.
"""
        dbutils.notebook.exit(error_msg)

    # Log execution context
    if job_id:
        print(f"Running as Job ID: {job_id}, Run ID: {run_id}")
    else:
        print("WARNING: Running interactively (not as a job). This is not recommended for production.")

    return {
        "job_id": job_id,
        "run_id": run_id,
        "is_job_run": job_id is not None
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Control Table Management

# COMMAND ----------

def get_control_table_path(catalog="asb_dev", schema="retail_bronze"):
    """Get the fully qualified path for the ETL control table."""
    return f"{catalog}.{schema}._etl_control"


def initialize_control_table(catalog="asb_dev", schema="retail_bronze"):
    """
    Create the ETL control table if it doesn't exist.
    This table tracks processing watermarks for incremental loads.
    """
    control_table = get_control_table_path(catalog, schema)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {control_table} (
            table_name STRING NOT NULL COMMENT 'Target table name (e.g., od_gdwextract_incr)',
            layer STRING NOT NULL COMMENT 'ETL layer (bronze_to_silver, silver_to_gold)',
            last_processed_watermark TIMESTAMP COMMENT 'Max watermark value processed in last successful run',
            last_run_timestamp TIMESTAMP COMMENT 'When the last run started',
            last_run_end_timestamp TIMESTAMP COMMENT 'When the last run completed',
            last_run_status STRING COMMENT 'SUCCESS or FAILED',
            rows_processed LONG COMMENT 'Number of rows processed in last run',
            run_id STRING COMMENT 'Databricks run ID or ingestion ID',
            PRIMARY KEY (table_name, layer)
        )
        USING DELTA
        COMMENT 'ETL processing control table - tracks watermarks for incremental loads'
    """)

    print(f"Control table ready: {control_table}")
    return control_table


def get_last_watermark(table_name, layer="bronze_to_silver", catalog="asb_dev", schema="retail_bronze"):
    """
    Get the last processed watermark for a table/layer combination.

    Returns:
        Timestamp of last processed watermark, or None if no previous run
    """
    control_table = get_control_table_path(catalog, schema)

    # Check if control table exists
    if not spark.catalog.tableExists(control_table):
        initialize_control_table(catalog, schema)
        return None

    result = spark.sql(f"""
        SELECT last_processed_watermark
        FROM {control_table}
        WHERE table_name = '{table_name}'
          AND layer = '{layer}'
          AND last_run_status = 'SUCCESS'
    """).collect()

    if result and result[0][0]:
        return result[0][0]
    return None


def update_watermark(table_name, layer, new_watermark, rows_processed, status="SUCCESS",
                     run_id=None, catalog="asb_dev", schema="retail_bronze"):
    """
    Update the control table with new watermark after processing.

    Args:
        table_name: Target table name
        layer: ETL layer (bronze_to_silver, silver_to_gold)
        new_watermark: The new max watermark value processed
        rows_processed: Number of rows processed in this run
        status: SUCCESS or FAILED
        run_id: Optional Databricks run ID
    """
    control_table = get_control_table_path(catalog, schema)

    # Ensure control table exists
    if not spark.catalog.tableExists(control_table):
        initialize_control_table(catalog, schema)

    now = datetime.now()
    run_id = run_id or now.strftime("%Y%m%d_%H%M%S")

    # MERGE to upsert the control record
    spark.sql(f"""
        MERGE INTO {control_table} AS target
        USING (
            SELECT
                '{table_name}' AS table_name,
                '{layer}' AS layer,
                TIMESTAMP('{new_watermark}') AS last_processed_watermark,
                TIMESTAMP('{now}') AS last_run_timestamp,
                TIMESTAMP('{now}') AS last_run_end_timestamp,
                '{status}' AS last_run_status,
                {rows_processed} AS rows_processed,
                '{run_id}' AS run_id
        ) AS source
        ON target.table_name = source.table_name AND target.layer = source.layer
        WHEN MATCHED THEN UPDATE SET
            target.last_processed_watermark = source.last_processed_watermark,
            target.last_run_timestamp = source.last_run_timestamp,
            target.last_run_end_timestamp = source.last_run_end_timestamp,
            target.last_run_status = source.last_run_status,
            target.rows_processed = source.rows_processed,
            target.run_id = source.run_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Updated control table: {table_name}/{layer} -> watermark={new_watermark}, rows={rows_processed}, status={status}")


def get_control_table_status(catalog="asb_dev", schema="retail_bronze"):
    """Display the current state of all tracked tables."""
    control_table = get_control_table_path(catalog, schema)

    if not spark.catalog.tableExists(control_table):
        print("Control table does not exist yet.")
        return None

    return spark.table(control_table).orderBy("layer", "table_name")
