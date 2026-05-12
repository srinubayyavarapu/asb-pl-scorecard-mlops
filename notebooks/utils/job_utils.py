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
# MAGIC ## Iceberg UniForm
# MAGIC Every Delta table we write must be readable by Snowflake (and any other
# MAGIC Iceberg-compatible engine). The cheapest way to do that on Databricks is
# MAGIC Delta UniForm, which writes Iceberg metadata alongside the Delta log.

# COMMAND ----------

# Properties required for Delta -> Iceberg UniForm V2.
# column-mapping = "name" is mandatory for Iceberg compat (Iceberg uses field IDs
# behind column names; Delta's name-mode column mapping provides those IDs).
# enableDeletionVectors=false is also mandatory — IcebergCompatV2 rejects DVs
# because Iceberg has no equivalent representation.
_UNIFORM_PROPS = (
    "  'delta.columnMapping.mode'             = 'name',\n"
    "  'delta.enableDeletionVectors'          = 'false',\n"
    "  'delta.enableIcebergCompatV2'          = 'true',\n"
    "  'delta.universalFormat.enabledFormats' = 'iceberg'"
)


def enable_iceberg_uniform(table_fqn, extra_props=None):
    """Enable Delta UniForm (Iceberg) on a Delta table.

    Idempotent — safe to call after every saveAsTable.

    Two-step protocol (Databricks serverless defaults DVs on, IcebergCompat V2
    rejects DVs):
      1. Disable deletion vectors AND purge any existing DV files.
      2. ALTER with the Iceberg properties + optional extras.

    Args:
        table_fqn:   Fully qualified table name (catalog.schema.table).
        extra_props: Optional dict of additional table properties to set in
                     the same ALTER (e.g. {"delta.enableChangeDataFeed": "true"}).
    """
    # Step 1: disable DVs first. Setting both at once in one ALTER fails because
    # the IcebergCompat validator inspects table state DURING the ALTER and
    # sees DVs still enabled.
    spark.sql(f"""
        ALTER TABLE {table_fqn} SET TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'false'
        )
    """)

    # Step 2: purge any existing DV files (no-op for freshly written tables).
    # REORG ... APPLY (PURGE) is idempotent.
    try:
        spark.sql(f"REORG TABLE {table_fqn} APPLY (PURGE)")
    except Exception:
        # Ok if REORG isn't supported on this table type / runtime — the
        # property change above is what IcebergCompat actually checks.
        pass

    # Step 3: set the Iceberg UniForm props (+ any extras).
    extras = ""
    if extra_props:
        extras = ",\n" + ",\n".join(
            f"  '{k}' = '{v}'" for k, v in extra_props.items()
        )
    spark.sql(f"""
        ALTER TABLE {table_fqn} SET TBLPROPERTIES (
        {_UNIFORM_PROPS}{extras}
        )
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Control Table Management

# COMMAND ----------

def _require_catalog(catalog):
    if not catalog:
        raise ValueError(
            "catalog is required and must come from the calling notebook's "
            "widget (bundle sets it via ${var.catalog}). No default."
        )


def get_control_table_path(catalog, schema="bronze"):
    """Get the fully qualified path for the ETL control table."""
    _require_catalog(catalog)
    return f"{catalog}.{schema}._etl_control"


def initialize_control_table(catalog, schema="bronze"):
    """
    Create the ETL control table if it doesn't exist.
    This table tracks processing watermarks for incremental loads.
    Created with Iceberg UniForm enabled from the start so Snowflake can
    read it without a follow-up ALTER.
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
        TBLPROPERTIES (
        {_UNIFORM_PROPS}
        )
        COMMENT 'ETL processing control table - tracks watermarks for incremental loads'
    """)

    # Idempotent — also covers tables created by an earlier seed that lacked
    # the UniForm properties.
    enable_iceberg_uniform(control_table)

    print(f"Control table ready: {control_table}")
    return control_table


def get_last_watermark(table_name, layer="bronze_to_silver", catalog=None, schema="bronze"):
    _require_catalog(catalog)
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
                     run_id=None, catalog=None, schema="bronze",
                     max_retries=6, base_delay=1.5):
    """
    Update the control table with new watermark after processing.

    All silver tasks MERGE into the same single-row-per-table _etl_control,
    so when 9 parallel silver tasks finish at similar times we get
    DELTA_CONCURRENT_APPEND optimistic-concurrency conflicts. Each task
    is updating a DIFFERENT (table_name, layer) key — they're not
    actually racing — but Delta's commit detection conflicts anyway on an
    unpartitioned table. Retry with exponential backoff + jitter clears it.

    Args:
        table_name: Target table name
        layer: ETL layer (bronze_to_silver, silver_to_gold)
        new_watermark: The new max watermark value processed
        rows_processed: Number of rows processed in this run
        status: SUCCESS or FAILED
        run_id: Optional Databricks run ID
        max_retries: How many times to retry on ConcurrentAppendException
        base_delay: Initial backoff seconds (doubles each attempt + jitter)
    """
    import random
    import time

    _require_catalog(catalog)
    control_table = get_control_table_path(catalog, schema)

    # Ensure control table exists
    if not spark.catalog.tableExists(control_table):
        initialize_control_table(catalog, schema)

    now = datetime.now()
    run_id = run_id or now.strftime("%Y%m%d_%H%M%S")

    merge_sql = f"""
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
            target.last_run_timestamp       = source.last_run_timestamp,
            target.last_run_end_timestamp   = source.last_run_end_timestamp,
            target.last_run_status          = source.last_run_status,
            target.rows_processed           = source.rows_processed,
            target.run_id                   = source.run_id
        WHEN NOT MATCHED THEN INSERT *
    """

    last_err = None
    for attempt in range(max_retries):
        try:
            spark.sql(merge_sql)
            print(
                f"Updated control table: {table_name}/{layer} -> "
                f"watermark={new_watermark}, rows={rows_processed}, "
                f"status={status} (attempt={attempt + 1})"
            )
            return
        except Exception as e:
            msg = str(e)
            is_concurrent = (
                "DELTA_CONCURRENT_APPEND" in msg
                or "ConcurrentAppendException" in msg
                or "DELTA_CONCURRENT" in msg  # also covers WRITE/UPDATE variants
            )
            if not is_concurrent or attempt == max_retries - 1:
                last_err = e
                break
            # Exponential backoff with jitter — separate the converging writers
            sleep_s = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
            print(
                f"  Control-table MERGE conflict (attempt {attempt + 1}/{max_retries}), "
                f"backing off {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)

    raise last_err


def get_control_table_status(catalog, schema="bronze"):
    _require_catalog(catalog)
    """Display the current state of all tracked tables."""
    control_table = get_control_table_path(catalog, schema)

    if not spark.catalog.tableExists(control_table):
        print("Control table does not exist yet.")
        return None

    return spark.table(control_table).orderBy("layer", "table_name")
