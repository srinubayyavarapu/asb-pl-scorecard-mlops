# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Ingest from Snowflake to Bronze
# MAGIC **Generic, metadata-driven ingestion notebook.**
# MAGIC
# MAGIC Pass `table_name` parameter -> reads config from `master_table_inventory.csv` -> ingests that table into Bronze.
# MAGIC
# MAGIC **Supports:**
# MAGIC - `historical` - Full load (overwrite Bronze)
# MAGIC - `incremental` - Watermark-based (append only new/changed rows)
# MAGIC - `federation` - Lakehouse Federation (CTAS from foreign catalog)
# MAGIC - `jdbc` - Spark Snowflake connector (direct JDBC read)
# MAGIC
# MAGIC **Usage:**
# MAGIC - Set widget `table_name` = source table name (e.g., `HLACCTBASE_FINAL`)
# MAGIC - Or pass via job parameter: `{"table_name": "HLACCTBASE_FINAL"}`

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import csv
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Source table name from master CSV")
dbutils.widgets.text("force_full_load", "N", "Force full load for incremental tables (Y/N)")

TABLE_NAME = dbutils.widgets.get("table_name").strip()
FORCE_FULL = dbutils.widgets.get("force_full_load").strip().upper() == "Y"

if not TABLE_NAME:
    dbutils.notebook.exit("ERROR: table_name parameter is required")

print(f"Table: {TABLE_NAME}")
print(f"Force full load: {FORCE_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Table Config from Master CSV

# COMMAND ----------

def read_master_csv():
    """Read master_table_inventory.csv and return as list of dicts."""
    # In Databricks workspace, files are accessible via workspace path
    # Try multiple paths for flexibility
    possible_paths = [
        "/Workspace/ASB-Migration/configs/ingestion/master_table_inventory.csv",
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath("")))), "configs", "ingestion", "master_table_inventory.csv"),
    ]

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

    # Fallback: read as Spark (works if uploaded as workspace file)
    try:
        df = spark.read.option("header", "true").csv("dbfs:/Workspace/ASB-Migration/configs/ingestion/master_table_inventory.csv")
        rows = [row.asDict() for row in df.collect()]
        print(f"Loaded master CSV via Spark ({len(rows)} tables)")
        return rows
    except Exception:
        pass

    dbutils.notebook.exit("ERROR: Cannot find master_table_inventory.csv")

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
    dbutils.notebook.exit(f"ERROR: Table '{TABLE_NAME}' not found or not active in master CSV")

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

# Source (Snowflake)
SOURCE_DB = table_config["source_database"]
SOURCE_SCHEMA = table_config["source_schema"]
SOURCE_TABLE = table_config["source_table"]
SOURCE_TYPE = table_config.get("source_type", "TABLE")
INGESTION_MODE = table_config.get("ingestion_mode", "federation")
LOAD_TYPE = table_config.get("load_type", "historical")
PRIMARY_KEY = table_config.get("primary_key", "").strip() or None
WATERMARK_COL = table_config.get("watermark_column", "").strip() or None

# Target (Databricks)
TARGET_CATALOG = table_config.get("target_catalog", "").strip() or "asb_dev"
BRONZE_SCHEMA = table_config.get("target_bronze_schema", "").strip() or "retail_bronze"
BRONZE_TABLE = table_config["target_bronze_table"]

# Fully qualified paths
SOURCE_FQN = f"snowflake_asb.{SOURCE_SCHEMA.lower()}.{SOURCE_TABLE.lower()}"
BRONZE_FQN = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

print(f"\nSource: {SOURCE_FQN}")
print(f"Target: {BRONZE_FQN}")
print(f"Mode: {INGESTION_MODE} | Load: {LOAD_TYPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Ingest to Bronze

# COMMAND ----------

def ingest_federation_historical(source_fqn, bronze_fqn):
    """
    Federation + Historical: Read from Snowflake foreign catalog, overwrite Bronze.
    This is the default and most common path.
    """
    print(f"  Mode: FEDERATION | Load: HISTORICAL (full overwrite)")

    df = spark.table(source_fqn)
    source_count = df.count()
    print(f"  Source rows: {source_count:,}")

    # Add ingestion metadata
    df_with_meta = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("snowflake"))
        .withColumn("_source_table", F.lit(source_fqn))
        .withColumn("_load_type", F.lit("historical"))
        .withColumn("_ingestion_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )

    # Write: full overwrite
    (
        df_with_meta.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_fqn)
    )

    bronze_count = spark.table(bronze_fqn).count()
    print(f"  Bronze rows: {bronze_count:,}")
    print(f"  Match: {'YES' if source_count == bronze_count else 'NO'}")

    return bronze_count


def ingest_federation_incremental(source_fqn, bronze_fqn, watermark_col, primary_key):
    """
    Federation + Incremental: Read only new/changed rows using watermark column.
    First run (table doesn't exist) -> full load.
    Subsequent runs -> append rows WHERE watermark > last_value.
    """
    print(f"  Mode: FEDERATION | Load: INCREMENTAL (watermark: {watermark_col})")

    table_exists = spark.catalog.tableExists(bronze_fqn)

    if not table_exists or FORCE_FULL:
        reason = "Force full load" if FORCE_FULL else "Bronze table doesn't exist (initial load)"
        print(f"  {reason} -> falling back to full load")
        return ingest_federation_historical(source_fqn, bronze_fqn)

    # Get last watermark value from existing Bronze
    last_watermark = spark.table(bronze_fqn).select(F.max(watermark_col)).collect()[0][0]
    print(f"  Last watermark: {last_watermark}")

    if last_watermark is None:
        print(f"  Watermark is NULL -> full load")
        return ingest_federation_historical(source_fqn, bronze_fqn)

    # Read only new rows from Snowflake
    df_new = (
        spark.table(source_fqn)
        .filter(F.col(watermark_col) > F.lit(str(last_watermark)))
    )

    new_count = df_new.count()
    print(f"  New rows since {last_watermark}: {new_count:,}")

    if new_count == 0:
        print(f"  No new data to ingest")
        total = spark.table(bronze_fqn).count()
        return total

    # Add metadata and append
    df_with_meta = (
        df_new
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("snowflake"))
        .withColumn("_source_table", F.lit(source_fqn))
        .withColumn("_load_type", F.lit("incremental"))
        .withColumn("_ingestion_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )

    (
        df_with_meta.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_fqn)
    )

    total = spark.table(bronze_fqn).count()
    print(f"  Appended: {new_count:,} | Total Bronze: {total:,}")

    return total

# COMMAND ----------

# Run ingestion based on config
start_time = datetime.now()

if INGESTION_MODE == "federation":
    if LOAD_TYPE == "incremental" and WATERMARK_COL and not FORCE_FULL:
        row_count = ingest_federation_incremental(SOURCE_FQN, BRONZE_FQN, WATERMARK_COL, PRIMARY_KEY)
    else:
        row_count = ingest_federation_historical(SOURCE_FQN, BRONZE_FQN)

elif INGESTION_MODE == "jdbc":
    # JDBC uses same federation path since we have foreign catalog set up
    # In production with actual JDBC, replace with spark.read.format("snowflake")
    if LOAD_TYPE == "incremental" and WATERMARK_COL and not FORCE_FULL:
        row_count = ingest_federation_incremental(SOURCE_FQN, BRONZE_FQN, WATERMARK_COL, PRIMARY_KEY)
    else:
        row_count = ingest_federation_historical(SOURCE_FQN, BRONZE_FQN)

else:
    dbutils.notebook.exit(f"ERROR: Unknown ingestion_mode '{INGESTION_MODE}'")

elapsed = (datetime.now() - start_time).total_seconds()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validation

# COMMAND ----------

# Validate Bronze table
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

# Return result for downstream tasks
result = f"SUCCESS|{BRONZE_TABLE}|{row_count}|{LOAD_TYPE}|{elapsed:.1f}s"
print(f"\n{result}")
dbutils.notebook.exit(result)
