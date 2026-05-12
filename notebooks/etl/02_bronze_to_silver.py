# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Bronze to Silver (Metadata-Driven + SCD Type 2) — CORRECTED
# MAGIC
# MAGIC **Fully metadata-driven cleansing pipeline with incremental watermark processing.**
# MAGIC
# MAGIC ## IMPORTANT: Job Execution Required
# MAGIC This notebook **must** be run as part of a Databricks Job with the required `row_index` parameter.
# MAGIC Do NOT run this notebook interactively without parameters.
# MAGIC
# MAGIC ## Two modes based on `load_type` in master inventory:
# MAGIC
# MAGIC | Load Type | Bronze -> Silver Strategy |
# MAGIC |-----------|------------------------|
# MAGIC | **historical** | Full overwrite - clean Bronze data replaces Silver |
# MAGIC | **incremental** | SCD Type 2 with watermark - only process NEW/CHANGED Bronze rows since last run |
# MAGIC
# MAGIC ## SCD Type 2 Contract (Incremental):
# MAGIC Given 10 existing employees (emp 1–10) in Silver and emp 1–5 change in source:
# MAGIC - Emp 6–10: unchanged -> 10 rows remain as-is (_is_current=True)
# MAGIC - Emp 1–5: old rows CLOSED (_is_current=False, _effective_to=now) + new rows INSERTED (_is_current=True)
# MAGIC - Result: **15 total rows** — 10 current + 5 historical
# MAGIC
# MAGIC ## Incremental Processing Flow:
# MAGIC 1. Read `last_processed_watermark` from `_etl_control` table
# MAGIC 2. Filter Bronze: only rows WHERE `watermark_column > last_watermark`
# MAGIC 3. Deduplicate batch to ONE row per primary_key (latest by orderby_col)
# MAGIC 4. Compute MD5 hash for change detection (NULL-safe)
# MAGIC 5. Identify CHANGED keys by comparing incoming hash vs current Silver hash
# MAGIC 6. MERGE: close old records for changed keys, insert all incoming as new current
# MAGIC 7. Update `_etl_control` with new watermark

# COMMAND ----------

# MAGIC %run ../utils/config_loader

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/validators

# COMMAND ----------

# MAGIC %run ../utils/job_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
import pandas as pd
import re
import os
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Parameter Validation & Job Enforcement

# COMMAND ----------

dbutils.widgets.text("row_index", "", "Row index from master_table_inventory.csv (REQUIRED)")

job_context = enforce_job_execution(required_params=["row_index"])

ROW_INDEX = int(dbutils.widgets.get("row_index").strip())

# COMMAND ----------

logger = get_logger("bronze_to_silver", persist_to_delta=True)
env = get_env_config()

logger.log_step("Bronze -> Silver (Incremental SCD2)", "STARTED", extra_info=f"row_index={ROW_INDEX}")
logger.info(f"Processing inventory row index: {ROW_INDEX}")
if job_context["is_job_run"]:
    logger.info(f"Job ID: {job_context['job_id']}, Run ID: {job_context['run_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Master Inventory & Get Table Config

# COMMAND ----------

def get_workspace_base_path():
    """Dynamically determine the workspace base path from notebook context."""
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        parts = notebook_path.split("/")
        if "ETL" in parts:
            etl_idx = parts.index("ETL")
            return "/Workspace" + "/".join(parts[:etl_idx])
        return "/Workspace" + "/".join(parts[:-2])
    except Exception:
        return None


def find_inventory_path():
    """Find master_table_inventory.csv in various locations."""
    possible_paths = []

    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        if ".bundle" in notebook_path:
            bundle_base = "/Workspace" + notebook_path.split("/files/")[0] + "/files"
            possible_paths.append(f"{bundle_base}/configs/ingestion/master_table_inventory.csv")
    except Exception:
        pass

    base_path = get_workspace_base_path()
    if base_path:
        possible_paths.append(f"{base_path}/configs/ingestion/master_table_inventory.csv")

    for path in possible_paths:
        if os.path.exists(path):
            return path

    raise FileNotFoundError(f"Cannot find master_table_inventory.csv. Searched: {possible_paths}")


try:
    inventory_path = find_inventory_path()
    inventory_df = pd.read_csv(inventory_path)
    logger.info(f"Loaded inventory from: {inventory_path}")
except FileNotFoundError as e:
    raise e

if ROW_INDEX < 0 or ROW_INDEX >= len(inventory_df):
    raise ValueError(
        f"Row index {ROW_INDEX} out of range. "
        f"Inventory has {len(inventory_df)} rows (0 to {len(inventory_df)-1})"
    )

row = inventory_df.iloc[ROW_INDEX]

# FIX #10: Use direct column access with explicit notna guard instead of .get()
# pandas Series.get() returns NaN for missing columns (not the default), so
# NaN != "Y" would incorrectly reject valid rows.
is_active_val = row["is_active"] if "is_active" in row.index and pd.notna(row["is_active"]) else "Y"
if str(is_active_val).strip().upper() != "Y":
    raise ValueError(f"Row index {ROW_INDEX} is not active (is_active='{is_active_val}')")

TABLE_NAME = row["source_table"]
logger.info(
    f"Found table config (row_index={ROW_INDEX}): "
    f"{row['source_table']} -> {row['target_silver_table']} (load_type: {row['load_type']})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract Configuration

# COMMAND ----------

bronze_name   = row["target_bronze_table"]
silver_name   = row["target_silver_table"]
load_type     = row["load_type"]
primary_key   = row["primary_key"]   if pd.notna(row.get("primary_key"))   else None
orderby_col   = row["orderby_col"]   if pd.notna(row.get("orderby_col"))   else None

# FIX #15: Read scd2_flag and scd2_column from config.
# SCD2 path is now gated by is_scd2=="Y", not just load_type=="incremental".
# A table can be incremental (append-only fact) without needing SCD2 history.
is_scd2       = str(row.get("is_scd2", "N")).strip().upper() == "Y" \
                    if pd.notna(row.get("is_scd2")) else False
scd2_col      = row["orderby_col"]   if pd.notna(row.get("orderby_col"))   else None

# Build fully qualified table paths.
# Catalog ALWAYS comes from the env widget (bundle passes per-target ${var.catalog}) —
# CSV's target_catalog column is per-env wrong by design and only used as a hard fallback.
# No hardcoded default — fail loudly so misconfigured runs don't land in the wrong catalog.
catalog = env["catalog"] or (row["target_catalog"] if pd.notna(row.get("target_catalog")) else "")
catalog = str(catalog).strip()
if not catalog:
    raise ValueError(
        "No catalog resolved. Pass `catalog` widget (bundle does this via "
        "${var.catalog}) or set target_catalog in master_table_inventory.csv."
    )
bronze_schema = row["target_bronze_schema"] if pd.notna(row.get("target_bronze_schema")) else env["bronze_schema"]
silver_schema = row["target_silver_schema"] if pd.notna(row.get("target_silver_schema")) else env["silver_schema"]

bronze_table  = f"{catalog}.{bronze_schema}.{bronze_name}"
silver_table  = f"{catalog}.{silver_schema}.{silver_name}"

# Validate: incremental SCD2 requires primary_key and orderby_col
if load_type == "incremental" and is_scd2:
    if not primary_key:
        raise ValueError(
            f"Table {silver_name} is configured as incremental+SCD2 but has no primary_key. "
            f"Set primary_key in master_table_inventory."
        )
    if not orderby_col:
        raise ValueError(
            f"Table {silver_name} is configured as incremental+SCD2 but has no orderby_col. "
            f"Set orderby_col (e.g. LAST_MODIFIED_DATE) in master_table_inventory."
        )

print(f"Bronze       : {bronze_table}")
print(f"Silver       : {silver_table}")
print(f"Load Type    : {load_type}")
print(f"Is SCD2      : {is_scd2}")
print(f"Primary Key  : {primary_key}")
print(f"Order By Col : {orderby_col}")
print(f"SCD2 Col     : {scd2_col}")

# Switch Spark session to UC catalog — workspace default hive_metastore
# is disabled, so saveAsTable fails with UC_HIVE_METASTORE_DISABLED_EXCEPTION
# even on fully qualified names unless USE CATALOG is set explicitly.
spark.sql(f"USE CATALOG {catalog}")

# Ensure Silver schema exists (UC does not auto-create schemas on saveAsTable)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transformation Functions

# COMMAND ----------

def standardize_columns(df):
    """
    Standardize column names: lowercase, underscores, no special chars.
    Applied consistently to both Bronze batches and Silver initial creates
    so column names never drift between runs.
    SAS Equivalent: RENAME in DATA step.
    """
    rename_map = {}
    for col_name in df.columns:
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name).lower().strip('_')
        clean_name = re.sub(r'_+', '_', clean_name)
        if clean_name != col_name:
            rename_map[col_name] = clean_name

    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    return df, rename_map


def apply_rename_map(df, rename_map):
    """
    Apply a previously computed rename_map to a DataFrame.
    Used to ensure Silver table column names always match the standardized batch names.
    """
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def compute_row_hash(df, exclude_cols=None):
    """
    Compute MD5 hash of all business data columns for change detection.
    
    FIX #3 (NULL-safe): uses COALESCE so NULL values produce the literal
    string 'NULL' instead of propagating NULL through concat, which would make
    two rows that differ only by NULL look identical.
    
    FIX #4 (PK in hash): primary_key is NO LONGER excluded — if a record's
    business key changes (re-key scenario), the hash will detect it.
    The only columns excluded are internal ETL metadata columns.
    """
    if exclude_cols is None:
        exclude_cols = []

    # Metadata columns that should NEVER participate in change detection
    metadata_patterns = [
        "_ingested", "_source", "_load", "_silver",
        "_effective", "_is_current", "_row_hash", "_rn",
        "_version", "_silver_processed"
    ]

    hash_cols = [
        c for c in df.columns
        if c not in exclude_cols
        and not any(pat in c.lower() for pat in metadata_patterns)
    ]

    if not hash_cols:
        raise ValueError(
            "compute_row_hash: no business columns found to hash. "
            "Check metadata_patterns exclusion list."
        )

    # Sort for determinism — same row always produces same hash regardless of column order
    hash_cols_sorted = sorted(hash_cols)

    concat_expr = F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in hash_cols_sorted]
    )
    return df.withColumn("_row_hash", F.md5(concat_expr))


def deduplicate_batch(df, primary_key, orderby_col):
    """
    FIX #5: Reduce the incoming batch to EXACTLY ONE row per primary_key.
    
    When a source sends multiple versions of the same key in one batch
    (e.g. two salary changes for emp 1 in the same extract), we keep only
    the latest version (highest orderby_col). The intermediate versions are
    intentionally discarded — we track only the transition from the last-known
    Silver state to the newest incoming state.
    
    If you need to preserve every intermediate version, load source data at
    higher frequency so each batch contains only one change per key.
    """
    if not primary_key or primary_key not in df.columns:
        # No PK — deduplicate purely on row hash
        return df.dropDuplicates(["_row_hash"])

    window = Window.partitionBy(primary_key).orderBy(F.col(orderby_col).desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_scd2_columns(df, effective_from_col):
    """
    Add SCD2 audit columns to an incoming batch.
    All incoming rows are treated as new CURRENT records:
      _is_current    = True
      _effective_from = value of effective_from_col cast to timestamp
      _effective_to  = NULL (open-ended — this record is current)
      _silver_processed_at = current pipeline run timestamp

    FIX #2: We do NOT pre-compute _effective_to on the incoming batch.
    _effective_to is set to the MERGE execution timestamp only when an existing
    Silver record is CLOSED by a newer incoming version. Pre-computing it
    from a lead() window over the batch is wrong because:
      (a) the batch is ordered desc, so lead() points backward in time
      (b) the batch typically has one row per key, so lead() returns NULL anyway
      (c) _effective_to must equal the _effective_from of the NEXT version,
          which is the timestamp of the MERGE run, not a source column value

    FIX #9: _is_current is always True for incoming rows — the MERGE decides
    which existing Silver rows to close. We do not compute _is_current from a
    batch-only window (which would mark every row as current since each key
    appears once).
    """
    if effective_from_col not in df.columns:
        raise ValueError(
            f"add_scd2_columns: effective_from_col '{effective_from_col}' not found. "
            f"Available columns: {df.columns}"
        )

    return (
        df
        .withColumn("_is_current",          F.lit(True))
        .withColumn("_effective_from",       F.col(effective_from_col).cast("timestamp"))
        .withColumn("_effective_to",         F.lit(None).cast("timestamp"))
        .withColumn("_silver_processed_at",  F.current_timestamp())
    )


def check_quality(df, table_name, primary_key=None):
    """
    FIX #8: Cache df before quality checks to avoid re-computing the same
    DataFrame multiple times (each .count() is a full scan otherwise).
    Returns the cached DataFrame so downstream steps reuse it.
    """
    total = df.count()
    cols  = len(df.columns)

    null_cols = 0
    for c in df.columns:
        if not c.startswith("_"):
            nc = df.filter(F.col(c).isNull()).count()
            if nc > 0:
                null_cols += 1

    dupes = 0
    if primary_key and primary_key in df.columns:
        distinct = df.select(primary_key).distinct().count()
        dupes = total - distinct

    logger.info(
        f"  Quality: {total:,} rows | {cols} cols | "
        f"{null_cols} cols with nulls | {dupes} key duplicates",
        table_name=table_name, row_count=total,
        extra_info=f"cols_with_nulls={null_cols}, duplicates={dupes}"
    )
    return df, {"rows": total, "columns": cols, "null_columns": null_cols, "duplicates": dupes}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Silver Write Strategies

# COMMAND ----------

def write_silver_historical(df, silver_tbl, primary_key=None):
    """
    Historical (Full Load): Overwrite entire Silver table.
    Used for load_type='historical' only.
    """
    logger.info("    Strategy: FULL OVERWRITE")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_tbl)
    )

    enable_iceberg_uniform(silver_tbl)

    return spark.table(silver_tbl).count(), "FULL_OVERWRITE"


def write_silver_incremental_append(df, silver_tbl):
    """
    Append-only strategy for fact / event tables that never change.
    Used when load_type='incremental' and is_scd2='N' (e.g. CREDIT_PERFORMANCE).

    Each row is a point-in-time observation; updates / corrections are out of scope.
    Initial run creates the table; subsequent runs append the watermark-filtered batch.
    """
    logger.info("    Strategy: APPEND ONLY (immutable facts)")

    if not spark.catalog.tableExists(silver_tbl):
        # First run — create the table from this batch
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_tbl)
        )
        logger.info("    Initial load — created Silver table")
    else:
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(silver_tbl)
        )
        logger.info(f"    Appended {df.count():,} new rows to Silver")

    enable_iceberg_uniform(silver_tbl)

    return spark.table(silver_tbl).count(), "APPEND_ONLY"


def write_silver_incremental_scd2(df_incoming, silver_tbl, primary_key, merge_timestamp=None):
    """
    Incremental SCD Type 2 Merge — FULLY CORRECTED.

    Guarantee: Given N existing employees in Silver and K of them change:
      - Unchanged employees: 1 row each (current, untouched)
      - Changed employees  : 1 old row (closed) + 1 new row (current) = 2 rows each
      - New employees      : 1 row each (current, inserted)
      - Total              : N + K rows  (your 10 emp + 5 changed = 15 rows scenario)

    Algorithm:
    1. Compare incoming batch hash vs existing current Silver hash per key
    2. Identify three sets:
         - NEW keys    : in incoming, not in Silver
         - CHANGED keys: in incoming + Silver, hash differs
         - UNCHANGED   : in incoming + Silver, hash same (skip)
    3. For CHANGED keys: UPDATE Silver (set _is_current=False, _effective_to=now)
    4. For NEW + CHANGED keys: INSERT new rows (all with _is_current=True)

    Step 3 uses Delta MERGE (atomic).
    Step 4 uses a single append write (atomic per Delta).
    Together they are NOT a single atomic transaction, but they are idempotent
    when re-run from the same watermark because:
      - MERGE is idempotent (same key will already be closed, whenMatched condition won't fire again)
      - INSERT is guarded by the changed_keys computation above Silver state POST-merge

    FIX #1: changed_keys is computed from df_incoming vs current Silver BEFORE
    the merge, not from all-time Silver closed records after the merge.
    
    FIX #3: Change detection uses NULL-safe equality (<=> operator).
    
    FIX #12: If compare_cols is empty, raise instead of silently disabling updates.
    """
    logger.info(f"    Strategy: SCD TYPE 2 MERGE (key: {primary_key})")

    if merge_timestamp is None:
        merge_timestamp = F.current_timestamp()

    # ── INITIAL LOAD ──────────────────────────────────────────────────────────
    # If the Silver table doesn't exist yet, write everything as-is.
    # df_incoming already has _is_current=True, _effective_from set, _effective_to=NULL
    # from add_scd2_columns(), so the initial Silver is a valid SCD2 table.
    table_exists = spark.catalog.tableExists(silver_tbl)
    if not table_exists:
        logger.info("    Silver table doesn't exist — initial load")
        (
            df_incoming.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_tbl)
        )
        enable_iceberg_uniform(silver_tbl)
        count = spark.table(silver_tbl).count()
        return count, "INITIAL_LOAD"

    # ── IDENTIFY BUSINESS COLUMNS FOR CHANGE DETECTION ───────────────────────
    # Exclude all internal metadata columns — only compare actual business data.
    metadata_patterns = [
        "_ingested", "_source", "_load", "_silver",
        "_effective", "_is_current", "_row_hash", "_rn",
        "_version", "_silver_processed"
    ]
    compare_cols = [
        c for c in df_incoming.columns
        if not any(pat in c.lower() for pat in metadata_patterns)
    ]

    if not compare_cols:
        raise ValueError(
            f"write_silver_incremental_scd2: no business columns available for "
            f"change detection on table {silver_tbl}. "
            f"All columns were filtered as metadata. Columns: {df_incoming.columns}"
        )

    # ── FIX #1: IDENTIFY CHANGED KEYS BEFORE THE MERGE ───────────────────────
    # We compare the incoming batch's _row_hash against the CURRENT Silver records.
    # This gives us exactly which keys changed in THIS run — not all-time history.
    current_silver = (
        spark.table(silver_tbl)
        .filter(F.col("_is_current") == True)
        .select(primary_key, F.col("_row_hash").alias("_silver_hash"))
    )

    # Incoming joined to current silver
    incoming_with_silver = (
        df_incoming.alias("inc")
        .join(current_silver.alias("sil"), primary_key, "left")
    )

    # CHANGED: key exists in Silver AND hash is different
    # FIX #3: use eqNullSafe for NULL-safe hash comparison
    changed_keys_df = (
        incoming_with_silver
        .filter(
            F.col("_silver_hash").isNotNull() &                        # exists in Silver
            (~F.col("inc._row_hash").eqNullSafe(F.col("_silver_hash")))  # hash changed
        )
        .select(F.col(f"inc.{primary_key}").alias(primary_key))
        .distinct()
    )

    # NEW: key does not exist in Silver at all
    new_keys_df = (
        incoming_with_silver
        .filter(F.col("_silver_hash").isNull())
        .select(F.col(f"inc.{primary_key}").alias(primary_key))
        .distinct()
    )

    changed_count = changed_keys_df.count()
    new_count     = new_keys_df.count()

    logger.info(f"    Incoming batch: {df_incoming.count()} rows")
    logger.info(f"    Changed keys  : {changed_count}")
    logger.info(f"    New keys      : {new_count}")

    if changed_count == 0 and new_count == 0:
        logger.info("    No new or changed records — skipping merge")
        total   = spark.table(silver_tbl).count()
        current = spark.table(silver_tbl).filter(F.col("_is_current") == True).count()
        return current, "NO_OP"

    # ── STEP A: CLOSE OLD RECORDS FOR CHANGED KEYS (Delta MERGE) ─────────────
    # For every key that changed: set _is_current=False and _effective_to=now.
    # Keys that are brand new are not in Silver, so the MERGE won't touch them.
    # Unchanged keys are not in changed_keys_df, so they won't be closed.
    if changed_count > 0:
        silver_delta = DeltaTable.forName(spark, silver_tbl)

        (
            silver_delta.alias("existing")
            .merge(
                changed_keys_df.alias("chg"),
                f"existing.{primary_key} = chg.{primary_key} AND existing._is_current = true"
            )
            .whenMatchedUpdate(set={
                "_is_current":  F.lit(False),
                "_effective_to": merge_timestamp,
            })
            .execute()
        )
        logger.info(f"    Closed {changed_count} old record(s) in Silver")

    # ── STEP B: INSERT NEW CURRENT ROWS (NEW + CHANGED) ──────────────────────
    # Both brand-new employees and employees who just changed need a fresh
    # current row inserted. We union both key sets and join back to df_incoming.
    keys_to_insert = new_keys_df.union(changed_keys_df).distinct()

    rows_to_insert = (
        df_incoming.alias("inc")
        .join(keys_to_insert.alias("ins"), primary_key, "inner")
        .select("inc.*")
        # Ensure _effective_from is set; if it came through as null (edge case),
        # fall back to the merge timestamp so we never insert a null _effective_from.
        .withColumn(
            "_effective_from",
            F.coalesce(F.col("_effective_from"), merge_timestamp)
        )
    )

    insert_count = rows_to_insert.count()
    if insert_count > 0:
        (
            rows_to_insert.write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_tbl)
        )
        enable_iceberg_uniform(silver_tbl)
        logger.info(f"    Inserted {insert_count} new current row(s) into Silver")

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    total_after     = spark.table(silver_tbl).count()
    current_after   = spark.table(silver_tbl).filter(F.col("_is_current") == True).count()
    historical_after = total_after - current_after

    logger.info(
        f"    SCD2 complete: {current_after:,} current | "
        f"{historical_after:,} historical | {total_after:,} total"
    )
    logger.info(
        f"    This run: {new_count} new inserts | "
        f"{changed_count} updates (closed + re-inserted)"
    )

    return current_after, "SCD2_MERGE"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Process Table with Watermark Tracking

# COMMAND ----------

start_time = datetime.now()
run_id = job_context.get("run_id") or start_time.strftime("%Y%m%d_%H%M%S")

last_watermark = None
result_status  = "FAILED"
result         = {}

try:
    logger.info(
        f"Processing: {bronze_table} -> {silver_table} [{load_type}]",
        step_name="Transform", step_status="STARTED", table_name=silver_name
    )

    if not spark.catalog.tableExists(bronze_table):
        raise ValueError(f"Bronze table {bronze_table} does not exist. Run ingestion first.")

    # ── WATERMARK FILTER ──────────────────────────────────────────────────────
    # FIX #6: Use the source watermark_column (orderby_col / LAST_MODIFIED_DATE)
    # as the watermark filter column, NOT _ingested_at.
    #
    # _ingested_at = when the row arrived in Bronze (pipeline time)
    # orderby_col  = when the record changed in the source system (business time)
    #
    # Using _ingested_at causes two problems:
    #   (a) Late-arriving records (changed before last watermark but ingested after)
    #       are processed even though they predate the last watermark.
    #   (b) Records changed in source but not yet ingested are silently skipped.
    #
    # Using orderby_col (LAST_MODIFIED_DATE) correctly captures all source changes.

    if load_type == "incremental":
        last_watermark = get_last_watermark(
            table_name=silver_name,
            layer="bronze_to_silver",
            catalog=catalog,
            schema=bronze_schema
        )

        if last_watermark:
            logger.info(f"Last processed watermark: {last_watermark}")

            if is_scd2 and orderby_col:
                # FIX #6: filter by business watermark column (source change timestamp)
                filter_col = orderby_col
            else:
                # Non-SCD2 incremental: fall back to _ingested_at
                filter_col = "_ingested_at"

            logger.info(f"Watermark filter column: {filter_col}")
            df = (
                spark.table(bronze_table)
                .filter(F.col(filter_col) > F.lit(last_watermark))
            )
            bronze_count = df.count()
            logger.info(f"New Bronze rows since last run: {bronze_count:,}")

            if bronze_count == 0:
                logger.info("No new data to process — skipping")
                update_watermark(
                    table_name=silver_name,
                    layer="bronze_to_silver",
                    new_watermark=last_watermark,
                    rows_processed=0,
                    status="SUCCESS",
                    run_id=run_id,
                    catalog=catalog,
                    schema=bronze_schema
                )
                dbutils.notebook.exit(f"SUCCESS|{silver_name}|0|NO_NEW_DATA")
        else:
            logger.info("No previous watermark — processing all Bronze data (initial run)")
            df = spark.table(bronze_table)
            bronze_count = df.count()
            logger.info(f"Bronze rows (initial load): {bronze_count:,}")
    else:
        df = spark.table(bronze_table)
        bronze_count = df.count()
        logger.info(f"Bronze rows (historical full load): {bronze_count:,}")

    # Compute new watermark BEFORE any transformation
    # FIX #6: watermark is now derived from the business column (orderby_col),
    # not _ingested_at. We take the MAX so the next run picks up from where
    # this batch ended in source-time.
    watermark_source_col = orderby_col if (is_scd2 and orderby_col) else "_ingested_at"
    max_watermark_row = df.select(F.max(watermark_source_col)).collect()[0][0]

    # ── TRANSFORMATIONS ───────────────────────────────────────────────────────
    # FIX #14: Capture the rename_map from standardize_columns so it can be
    # applied consistently to Silver on re-reads. Column names in the Silver
    # table were established at initial load; subsequent batches must use the
    # same names or the MERGE will create new columns instead of updating.
    df, rename_map = standardize_columns(df)

    # After standardization, resolve primary_key and orderby_col to their
    # standardized (lowercased) equivalents if they were renamed.
    pk_std = rename_map.get(primary_key, primary_key)
    ob_std = rename_map.get(orderby_col, orderby_col) if orderby_col else None

    if pk_std not in df.columns:
        raise ValueError(
            f"Primary key column '{pk_std}' not found after standardization. "
            f"Available: {df.columns}"
        )

    # Compute row hash (FIX #4: PK is now included in hash; only metadata excluded)
    df = compute_row_hash(df)

    # FIX #5: Deduplicate to ONE row per primary_key using latest orderby_col.
    # This handles the case where the same employee appears twice in one batch.
    if load_type == "incremental" and is_scd2:
        df = deduplicate_batch(df, pk_std, ob_std)

    # FIX #2 / #9: add_scd2_columns sets _is_current=True, _effective_from=orderby_col,
    # _effective_to=NULL for all incoming rows. The MERGE closes old records and sets
    # their _effective_to to the merge timestamp. We do NOT pre-compute _effective_to
    # on the batch (lead() over descending window was wrong and meaningless).
    if load_type == "incremental" and is_scd2:
        df = add_scd2_columns(df, effective_from_col=ob_std)
    elif load_type == "historical":
        # Historical loads must also include SCD2 columns so the Silver schema
        # is ready for future incremental SCD2 runs against the same table.
        df = (
            df
            .withColumn("_is_current", F.lit(True))
            .withColumn("_effective_from", F.current_timestamp())
            .withColumn("_effective_to", F.lit(None).cast("timestamp"))
            .withColumn("_silver_processed_at", F.current_timestamp())
        )

    # FIX #8: quality checks without caching (serverless doesn't support PERSIST)
    df, quality = check_quality(df, silver_name, pk_std)

    # ── WRITE TO SILVER ───────────────────────────────────────────────────────
    # Three strategies, dispatched by (load_type, is_scd2):
    #   historical          -> full overwrite
    #   incremental + SCD2  -> SCD2 merge (close old, insert new current)
    #   incremental + !SCD2 -> append only (immutable facts e.g. credit performance)
    if load_type == "incremental" and is_scd2 and pk_std:
        silver_count, strategy = write_silver_incremental_scd2(
            df_incoming=df,
            silver_tbl=silver_table,
            primary_key=pk_std
        )
    elif load_type == "incremental" and not is_scd2:
        silver_count, strategy = write_silver_incremental_append(df, silver_table)
    else:
        silver_count, strategy = write_silver_historical(df, silver_table, pk_std)

    # ── UPDATE WATERMARK ──────────────────────────────────────────────────────
    if load_type == "incremental" and max_watermark_row:
        update_watermark(
            table_name=silver_name,
            layer="bronze_to_silver",
            new_watermark=max_watermark_row,
            rows_processed=bronze_count,
            status="SUCCESS",
            run_id=run_id,
            catalog=catalog,
            schema=bronze_schema
        )
        logger.info(f"Updated watermark to: {max_watermark_row} (column: {watermark_source_col})")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"SUCCESS: {silver_table} - {silver_count:,} rows [{strategy}] in {elapsed:.1f}s",
        step_name="Transform", step_status="COMPLETED",
        table_name=silver_name, row_count=silver_count,
        extra_info=f"strategy={strategy}, bronze_rows={bronze_count}, elapsed={elapsed:.1f}s"
    )

    result_status = "SUCCESS"
    result = {
        "table_name":      silver_name,
        "bronze_table":    bronze_table,
        "silver_table":    silver_table,
        "load_type":       load_type,
        "is_scd2":         is_scd2,
        "strategy":        strategy,
        "bronze_rows":     bronze_count,
        "silver_rows":     silver_count,
        "status":          "SUCCESS",
        "elapsed_seconds": elapsed
    }

except Exception as e:
    elapsed   = (datetime.now() - start_time).total_seconds()
    error_msg = str(e)[:500]

    logger.error(
        f"FAILED: {silver_name} - {error_msg}",
        step_name="Transform", step_status="FAILED",
        table_name=silver_name, extra_info=error_msg
    )

    # FIX #13: On failure, update control table with the LAST KNOWN GOOD watermark
    # (not the current run's max). This ensures the next run re-processes from
    # the last successful point. If last_watermark is None (first-ever run),
    # do NOT write datetime.now() as that would permanently skip un-processed data.
    if load_type == "incremental":
        try:
            if last_watermark is not None:
                update_watermark(
                    table_name=silver_name,
                    layer="bronze_to_silver",
                    new_watermark=last_watermark,   # stay at last good point
                    rows_processed=0,
                    status="FAILED",
                    run_id=run_id,
                    catalog=catalog,
                    schema=bronze_schema
                )
                logger.info(
                    f"Watermark retained at last-good-value: {last_watermark} "
                    f"(will reprocess from here on next run)"
                )
            else:
                # First-ever run failed before writing any data.
                # Do not write any watermark — next run will start fresh.
                logger.info(
                    "First-ever run failed before any data written. "
                    "Watermark not updated — next run will reprocess all Bronze data."
                )
        except Exception as wm_err:
            logger.error(f"Could not update watermark on failure: {wm_err}")

    result_status = "FAILED"
    result = {
        "table_name":      silver_name,
        "status":          f"FAILED: {error_msg}",
        "elapsed_seconds": elapsed
    }

    raise   # re-raise so Databricks job marks the run as failed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Summary & Control Table Status

# COMMAND ----------

logger.log_step(
    "Bronze -> Silver", result_status,
    table_name=silver_name,
    row_count=result.get("silver_rows", 0),
    extra_info=f"strategy={result.get('strategy', 'N/A')}"
)

logger.flush()

print("\n" + "=" * 60)
print("ETL CONTROL TABLE STATUS")
print("=" * 60)
control_status = get_control_table_status(catalog, bronze_schema)
if control_status:
    display(control_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Behaviour Reference
# MAGIC
# MAGIC ### Your scenario: 10 employees already in Silver, emp 1–5 change
# MAGIC
# MAGIC **Before this run (Silver has 10 rows, all current):**
# MAGIC
# MAGIC | employee_id | salary | _is_current | _effective_from | _effective_to |
# MAGIC |-------------|--------|-------------|----------------|---------------|
# MAGIC | EMP001      | 50000  | True        | 2024-01-01      | NULL          |
# MAGIC | ...         | ...    | True        | ...             | NULL          |
# MAGIC | EMP010      | 70000  | True        | 2024-01-01      | NULL          |
# MAGIC
# MAGIC **After this run (emp 1–5 salary changed):**
# MAGIC
# MAGIC | employee_id | salary | _is_current | _effective_from | _effective_to     |
# MAGIC |-------------|--------|-------------|----------------|-------------------|
# MAGIC | EMP001      | 50000  | **False**   | 2024-01-01      | **2024-08-15 10:00** |
# MAGIC | EMP001      | 60000  | **True**    | **2024-08-15**  | NULL              |
# MAGIC | ...         | ...    | ...         | ...             | ...               |
# MAGIC | EMP005      | 45000  | **False**   | 2024-01-01      | **2024-08-15 10:00** |
# MAGIC | EMP005      | 55000  | **True**    | **2024-08-15**  | NULL              |
# MAGIC | EMP006      | 65000  | True        | 2024-01-01      | NULL              |  ← untouched
# MAGIC | ...         | ...    | True        | ...             | NULL              |
# MAGIC | EMP010      | 70000  | True        | 2024-01-01      | NULL              |  ← untouched
# MAGIC
# MAGIC **Total: 15 rows — 10 current + 5 historical ✓**
# MAGIC
# MAGIC ### Query patterns:
# MAGIC ```sql
# MAGIC -- Current state of all employees
# MAGIC SELECT * FROM silver.employee_master WHERE _is_current = true;
# MAGIC
# MAGIC -- Point-in-time snapshot (e.g. what did emp 1's record look like on 2024-06-01?)
# MAGIC SELECT * FROM silver.employee_master
# MAGIC WHERE employee_id = 'EMP001'
# MAGIC   AND _effective_from <= '2024-06-01'
# MAGIC   AND (_effective_to > '2024-06-01' OR _effective_to IS NULL);
# MAGIC
# MAGIC -- Full history of a specific employee
# MAGIC SELECT * FROM silver.employee_master
# MAGIC WHERE employee_id = 'EMP001'
# MAGIC ORDER BY _effective_from;
# MAGIC ```

# COMMAND ----------

result_str = (
    f"{result_status}|{silver_name}"
    f"|{result.get('silver_rows', 0)}"
    f"|{result.get('strategy', 'N/A')}"
)
dbutils.notebook.exit(result_str)
