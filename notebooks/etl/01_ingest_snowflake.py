# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Ingest from Snowflake to Bronze (JDBC only)
# MAGIC **Generic, metadata-driven ingestion notebook.**
# MAGIC
# MAGIC Pass `table_name` parameter -> reads config from `master_table_inventory.csv` ->
# MAGIC ingests that table into Bronze via the Spark Snowflake connector (JDBC).
# MAGIC
# MAGIC **Supports:**
# MAGIC - `historical` - Full load (overwrite Bronze)
# MAGIC - `incremental` - Watermark-based (append only new/changed rows)
# MAGIC
# MAGIC **Prereq:** Databricks secret scope `asb_sf` must exist with keys
# MAGIC `account`, `user`, `password`. See notebooks/setup/01_setup_snowflake_secrets.py.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import csv
import os

# COMMAND ----------

# MAGIC %run ../utils/job_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Source table name from master CSV")
dbutils.widgets.text("force_full_load", "N", "Force full load for incremental tables (Y/N)")
# Per-env target catalog — bundle passes ${var.catalog}. Empty = fall back to CSV.
dbutils.widgets.text("catalog", "", "Target Unity Catalog (overrides CSV target_catalog)")

TABLE_NAME = dbutils.widgets.get("table_name").strip()
FORCE_FULL = dbutils.widgets.get("force_full_load").strip().upper() == "Y"
CATALOG_OVERRIDE = dbutils.widgets.get("catalog").strip()

if not TABLE_NAME:
    raise ValueError("table_name parameter is required")

print(f"Table: {TABLE_NAME}")
print(f"Force full load: {FORCE_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Table Config from Master CSV

# COMMAND ----------

def read_master_csv():
    """Read master_table_inventory.csv and return as list of dicts.
    Bundle-aware: derives the inventory path from the running notebook's location."""
    possible_paths = []

    # Bundle deployment: notebook lives under
    # /Workspace/Users/<user>/.bundle/<bundle>/<target>/files/notebooks/etl/...
    # The CSV lives at <files>/configs/ingestion/master_table_inventory.csv
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        if "/files/" in notebook_path:
            bundle_files = "/Workspace" + notebook_path.split("/files/")[0] + "/files"
            possible_paths.append(f"{bundle_files}/configs/ingestion/master_table_inventory.csv")
    except Exception:
        pass

    # Legacy / non-bundle fallbacks
    possible_paths.extend([
        "/Workspace/ASB-Migration/configs/ingestion/master_table_inventory.csv",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath("")))),
            "configs", "ingestion", "master_table_inventory.csv"
        ),
    ])

    for path in possible_paths:
        try:
            rows = []
            with open(path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            print(f"Loaded master CSV from: {path} ({len(rows)} tables)")
            return rows
        except FileNotFoundError:
            continue

    raise FileNotFoundError(
        f"Cannot find master_table_inventory.csv. Searched: {possible_paths}"
    )

# COMMAND ----------

# Find the requested table in master CSV
master_data = read_master_csv()

table_config = None
for row in master_data:
    if (row["source_table"].upper() == TABLE_NAME.upper() or
        row["target_bronze_table"].lower() == TABLE_NAME.lower()):
        if row.get("is_active", "Y") == "Y":
            table_config = row
            break

if not table_config:
    raise ValueError(f"Table '{TABLE_NAME}' not found or not active in master CSV")

# Print config
print(f"\n{'='*50}")
print(f"TABLE CONFIG")
print(f"{'='*50}")
for key, val in table_config.items():
    if val:
        print(f"  {key:25s}: {val}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract Config Values

# COMMAND ----------

# Source (Snowflake) — DB and schema come straight from CSV (different rows can target different DBs)
SOURCE_DB     = table_config["source_database"]
SOURCE_SCHEMA = table_config["source_schema"]
SOURCE_TABLE  = table_config["source_table"]
LOAD_TYPE     = table_config.get("load_type", "historical")
PRIMARY_KEY   = table_config.get("primary_key", "").strip() or None
WATERMARK_COL = table_config.get("watermark_column", "").strip() or None

# Target (Databricks) — bundle catalog var (widget) is the source of truth.
# CSV `target_catalog` is a secondary fallback for non-bundle runs. No hardcoded
# default — fail loudly so misconfigured runs don't silently land in a stale catalog.
TARGET_CATALOG = CATALOG_OVERRIDE or table_config.get("target_catalog", "").strip()
if not TARGET_CATALOG:
    raise ValueError(
        "No target catalog resolved. Pass `catalog` widget (bundle does this via "
        "${var.catalog}) or set target_catalog in master_table_inventory.csv."
    )
if CATALOG_OVERRIDE:
    print(f"  Target catalog override (per-env): {CATALOG_OVERRIDE}")
BRONZE_SCHEMA = table_config.get("target_bronze_schema", "").strip() or "bronze"
BRONZE_TABLE  = table_config["target_bronze_table"]

# Fully qualified Bronze path
BRONZE_FQN = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

print(f"\nSource: {SOURCE_DB}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
print(f"Target: {BRONZE_FQN}")
print(f"Load:   {LOAD_TYPE}")

# Switch Spark session to UC catalog — Celebal workspace default is
# hive_metastore (disabled), which causes saveAsTable to fail with
# UC_HIVE_METASTORE_DISABLED_EXCEPTION even with fully qualified names.
spark.sql(f"USE CATALOG {TARGET_CATALOG}")

# Ensure Bronze schema exists (UC does not auto-create schemas on saveAsTable)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Snowflake Connection (JDBC via secret scope)

# COMMAND ----------

# Credentials from Databricks secret scope `asb_sf`.
# ASB-prod note: replace password with key-pair auth (sfPrivateKey option).
sf_options = {
    "sfURL":       f"https://{dbutils.secrets.get('asb_sf', 'account')}.snowflakecomputing.com",
    "sfUser":      dbutils.secrets.get("asb_sf", "user"),
    "sfPassword":  dbutils.secrets.get("asb_sf", "password"),
    "sfDatabase":  SOURCE_DB,
    "sfSchema":    SOURCE_SCHEMA,
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "ACCOUNTADMIN",
}


def read_snowflake(query=None, dbtable=None):
    """Read from Snowflake via the Spark Snowflake connector. Pass either
    `dbtable` for a full-table read or `query` for a filtered pushdown."""
    reader = spark.read.format("snowflake").options(**sf_options)
    if query:
        reader = reader.option("query", query)
    else:
        reader = reader.option("dbtable", dbtable)
    return reader.load()


def add_ingestion_metadata(df, load_kind):
    """Stamp the standard `_ingested_at` / `_source_*` / `_load_type` audit columns."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("snowflake"))
        .withColumn("_source_table", F.lit(f"{SOURCE_DB}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"))
        .withColumn("_load_type", F.lit(load_kind))
        .withColumn("_ingestion_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ingest to Bronze

# COMMAND ----------

def ingest_historical():
    """Historical: full overwrite from Snowflake."""
    print(f"  Mode: JDBC | Load: HISTORICAL (full overwrite)")

    df = read_snowflake(dbtable=SOURCE_TABLE)
    source_count = df.count()
    print(f"  Source rows: {source_count:,}")

    df_with_meta = add_ingestion_metadata(df, load_kind="historical")

    (
        df_with_meta.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_FQN)
    )

    enable_iceberg_uniform(BRONZE_FQN)

    bronze_count = spark.table(BRONZE_FQN).count()
    print(f"  Bronze rows: {bronze_count:,}")
    print(f"  Match: {'YES' if source_count == bronze_count else 'NO'}")
    return bronze_count


def ingest_incremental():
    """Incremental: read only rows where watermark_column > max(watermark) in Bronze.
    First run (Bronze missing) -> falls back to a full load."""
    print(f"  Mode: JDBC | Load: INCREMENTAL (watermark: {WATERMARK_COL})")

    if not spark.catalog.tableExists(BRONZE_FQN) or FORCE_FULL:
        reason = "Force full load" if FORCE_FULL else "Bronze does not exist (initial load)"
        print(f"  {reason} -> falling back to full load")
        return ingest_historical()

    last_watermark = spark.table(BRONZE_FQN).select(F.max(WATERMARK_COL)).collect()[0][0]
    print(f"  Last watermark: {last_watermark}")
    if last_watermark is None:
        return ingest_historical()

    # Pushdown: WHERE executes on Snowflake side, not Spark
    query = (
        f"SELECT * FROM {SOURCE_DB}.{SOURCE_SCHEMA}.{SOURCE_TABLE} "
        f"WHERE {WATERMARK_COL} > '{last_watermark}'"
    )
    df_new = read_snowflake(query=query)
    new_count = df_new.count()
    print(f"  New rows since {last_watermark}: {new_count:,}")

    if new_count == 0:
        return spark.table(BRONZE_FQN).count()

    df_with_meta = add_ingestion_metadata(df_new, load_kind="incremental")

    (
        df_with_meta.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(BRONZE_FQN)
    )

    enable_iceberg_uniform(BRONZE_FQN)

    total = spark.table(BRONZE_FQN).count()
    print(f"  Appended: {new_count:,} | Total Bronze: {total:,}")
    return total

# COMMAND ----------

# Run ingestion based on config
start_time = datetime.now()

if LOAD_TYPE == "incremental" and WATERMARK_COL and not FORCE_FULL:
    row_count = ingest_incremental()
else:
    row_count = ingest_historical()

elapsed = (datetime.now() - start_time).total_seconds()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validation

# COMMAND ----------

bronze_df = spark.table(BRONZE_FQN)

print(f"\n{'='*50}")
print(f"BRONZE VALIDATION: {BRONZE_FQN}")
print(f"{'='*50}")
print(f"  Rows:    {bronze_df.count():,}")
print(f"  Columns: {len(bronze_df.columns)}")
print(f"  Time:    {elapsed:.1f}s")

# Check metadata columns exist
meta_cols = ["_ingested_at", "_source_system", "_source_table", "_load_type", "_ingestion_id"]
for mc in meta_cols:
    status = "OK" if mc in bronze_df.columns else "MISSING"
    print(f"  {mc}: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

result = f"SUCCESS|{BRONZE_TABLE}|{row_count}|{LOAD_TYPE}|{elapsed:.1f}s"
print(f"\n{result}")
dbutils.notebook.exit(result)
