# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Bronze to Silver (Metadata-Driven + 
#  Type 2)
# MAGIC
# MAGIC **Fully metadata-driven cleansing pipeline.**
# MAGIC
# MAGIC Reads master inventory CSV and processes Bronze -> Silver for each table.
# MAGIC
# MAGIC **Two modes based on `load_type` in master inventory:**
# MAGIC
# MAGIC | Load Type | Bronze -> Silver Strategy |
# MAGIC |-----------|------------------------|
# MAGIC | **historical** | Full overwrite - clean Bronze data replaces Silver |
# MAGIC | **incremental** | SCD Type 2 - merge new records, track history with effective dates |
# MAGIC
# MAGIC **What happens for EVERY table:**
# MAGIC - Column name standardization (lowercase, underscores)
# MAGIC - Deduplication (using primary key from master inventory)
# MAGIC - Data quality checks (nulls, row count, duplicates)
# MAGIC - Silver metadata columns added
# MAGIC
# MAGIC **How to use:**
# MAGIC - To process ALL tables: run without parameters
# MAGIC - To process ONE table: pass `table_name` widget parameter
# MAGIC
# MAGIC **SAS Equivalent:** DATA step cleaning + PROC SORT NODUPKEY + SCD logic

# COMMAND ----------

# MAGIC %run ../utils/config_loader

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/validators

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
import pandas as pd
import re
import os

# COMMAND ----------

logger = get_logger("bronze_to_silver")
env = get_env_config()

log_step(logger, "Bronze -> Silver (Metadata-Driven + SCD2)", "STARTED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Master Inventory & Filter

# COMMAND ----------

# Widget: optionally specify a single table
dbutils.widgets.text("table_name", "", "Table to process (blank = all active)")

target_table = dbutils.widgets.get("table_name").strip()

# Load master inventory
repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
inventory_path = os.path.join(repo_root, "configs", "ingestion", "master_table_inventory.csv")

inventory_df = pd.read_csv(inventory_path)
inventory_df = inventory_df[inventory_df["is_active"] == "Y"]

if target_table:
    inventory_df = inventory_df[
        (inventory_df["source_table"].str.upper() == target_table.upper()) |
        (inventory_df["target_bronze_table"] == target_table.lower())
    ]
    if inventory_df.empty:
        dbutils.notebook.exit(f"Table '{target_table}' not found in inventory")

logger.info(f"Tables to process: {len(inventory_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transformation Functions

# COMMAND ----------

def standardize_columns(df):
    """
    Standardize column names: lowercase, underscores, no special chars.
    SAS Equivalent: RENAME in DATA step
    """
    for col_name in df.columns:
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name).lower().strip('_')
        clean_name = re.sub(r'_+', '_', clean_name)
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
    return df


def remove_duplicates(df, primary_key=None):
    """
    Remove duplicates. If primary key given, keep latest per key.
    SAS Equivalent: PROC SORT NODUPKEY
    """
    if primary_key and primary_key in df.columns:
        order_col = "_ingested_at" if "_ingested_at" in df.columns else primary_key
        window = Window.partitionBy(primary_key).orderBy(F.col(order_col).desc())
        df = (
            df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    else:
        df = df.dropDuplicates()
    return df


def add_silver_metadata(df):
    """Add Silver layer audit columns."""
    return (
        df
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_is_current", F.lit(True))
        .withColumn("_effective_from", F.current_timestamp())
        .withColumn("_effective_to", F.lit(None).cast("timestamp"))
    )


def check_quality(df, table_name, primary_key=None):
    """Run data quality checks and return summary."""
    total = df.count()
    cols = len(df.columns)

    # Null check on non-metadata columns
    null_cols = 0
    for c in df.columns:
        if not c.startswith("_"):
            nc = df.filter(F.col(c).isNull()).count()
            if nc > 0:
                null_cols += 1

    # Duplicate check
    dupes = 0
    if primary_key and primary_key in df.columns:
        distinct = df.select(primary_key).distinct().count()
        dupes = total - distinct

    logger.info(f"  Quality: {total:,} rows | {cols} cols | {null_cols} cols with nulls | {dupes} duplicates")

    return {"rows": total, "columns": cols, "null_columns": null_cols, "duplicates": dupes}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Write Strategies

# COMMAND ----------

def write_silver_historical(df, silver_table, primary_key=None):
    """
    Historical (Full Load): Overwrite entire Silver table.

    Used when Bronze is fully refreshed each run.
    Simple, no SCD logic needed.
    """
    logger.info(f"    Strategy: FULL OVERWRITE")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    return spark.table(silver_table).count()


def write_silver_incremental_scd2(df_new, silver_table, primary_key):
    """
    Incremental (SCD Type 2): Merge new records into Silver with history tracking.

    Logic:
    - If record exists and has CHANGED -> close old record (_is_current=False,
      _effective_to=now) and insert new record (_is_current=True)
    - If record is NEW -> insert as current
    - If record has NOT CHANGED -> do nothing

    SAS Equivalent: PROC SQL UPDATE + INSERT pattern for slowly changing dimensions
    """
    logger.info(f"    Strategy: SCD TYPE 2 (key: {primary_key})")

    table_exists = spark.catalog.tableExists(silver_table)

    if not table_exists:
        # First run - just write as initial load
        logger.info(f"    Silver table doesn't exist - initial load")
        return write_silver_historical(df_new, silver_table, primary_key)

    # Existing Silver table
    silver_delta = DeltaTable.forName(spark, silver_table)

    # Get non-metadata columns for change detection
    compare_cols = [c for c in df_new.columns if not c.startswith("_")]

    # Build change detection condition:
    # Row has changed if ANY non-metadata column differs
    change_conditions = [
        f"existing.{c} != incoming.{c}" for c in compare_cols
        if c != primary_key
    ]
    has_changed = " OR ".join(change_conditions) if change_conditions else "1=0"

    now = F.current_timestamp()

    # MERGE: SCD Type 2
    (
        silver_delta.alias("existing")
        .merge(
            df_new.alias("incoming"),
            f"existing.{primary_key} = incoming.{primary_key} AND existing._is_current = true"
        )
        # MATCHED + CHANGED -> close the old record
        .whenMatchedUpdate(
            condition=has_changed,
            set={
                "_is_current": F.lit(False),
                "_effective_to": now,
            }
        )
        # NOT MATCHED -> new record, insert as current
        .whenNotMatchedInsertAll()
        .execute()
    )

    # For changed records, we also need to INSERT the new version
    # (MERGE above only updated the old record, didn't insert the new one)
    # Re-read to find changed records that need new current rows
    existing_df = spark.table(silver_table)
    closed_keys = (
        existing_df
        .filter(
            (F.col("_is_current") == False) &
            (F.col("_effective_to").isNotNull())
        )
        .select(primary_key)
    )

    # New current records for changed rows
    new_current = (
        df_new.alias("incoming")
        .join(closed_keys.alias("closed"), primary_key, "inner")
        .select("incoming.*")
    )

    if new_current.count() > 0:
        (
            new_current.write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_table)
        )

    total = spark.table(silver_table).count()
    current = spark.table(silver_table).filter(F.col("_is_current") == True).count()
    historical = total - current

    logger.info(f"    SCD2 complete: {current:,} current | {historical:,} historical | {total:,} total")

    return current

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Process Each Table (Metadata-Driven Loop)

# COMMAND ----------

results = []

for _, row in inventory_df.iterrows():
    bronze_name = row["target_bronze_table"]
    silver_name = row["target_silver_table"]
    load_type = row["load_type"]
    primary_key = row["primary_key"] if pd.notna(row["primary_key"]) else None

    # Build fully qualified table paths
    catalog = row["target_catalog"] if pd.notna(row["target_catalog"]) else env["catalog"]
    bronze_schema = row["target_bronze_schema"] if pd.notna(row["target_bronze_schema"]) else env["bronze_schema"]
    silver_schema = row["target_silver_schema"] if pd.notna(row["target_silver_schema"]) else env["silver_schema"]

    bronze_table = f"{catalog}.{bronze_schema}.{bronze_name}"
    silver_table = f"{catalog}.{silver_schema}.{silver_name}"

    try:
        logger.info(f"Processing: {bronze_table} -> {silver_table} [{load_type}]")

        # Read from Bronze
        df = spark.table(bronze_table)
        bronze_count = df.count()

        # Apply transformations
        df = standardize_columns(df)
        df = remove_duplicates(df, primary_key)
        df = add_silver_metadata(df)

        # Data quality check
        quality = check_quality(df, silver_name, primary_key)

        # Write to Silver based on load type
        if load_type == "incremental" and primary_key:
            silver_count = write_silver_incremental_scd2(df, silver_table, primary_key)
            strategy = "SCD2"
        else:
            silver_count = write_silver_historical(df, silver_table, primary_key)
            strategy = "FULL_OVERWRITE"

        results.append({
            "source_table": row["source_table"],
            "bronze_table": bronze_name,
            "silver_table": silver_name,
            "load_type": load_type,
            "strategy": strategy,
            "primary_key": primary_key or "N/A",
            "bronze_rows": bronze_count,
            "silver_rows": silver_count,
            "status": "SUCCESS",
        })

        logger.info(f"  OK {silver_table} - {silver_count:,} rows [{strategy}]")

    except Exception as e:
        logger.error(f"  FAIL FAILED: {bronze_name} - {str(e)}")
        results.append({
            "source_table": row["source_table"],
            "bronze_table": bronze_name,
            "silver_table": silver_name,
            "load_type": load_type,
            "strategy": "FAILED",
            "primary_key": primary_key or "N/A",
            "bronze_rows": 0,
            "silver_rows": 0,
            "status": f"FAILED: {str(e)[:200]}",
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary

# COMMAND ----------

results_pdf = pd.DataFrame(results)

log_step(logger, "Bronze -> Silver", "COMPLETED")
logger.info(f"Total: {len(results)} | Success: {len(results_pdf[results_pdf['status'] == 'SUCCESS'])} | Failed: {len(results_pdf[results_pdf['status'] != 'SUCCESS'])}")

# Show results
display(spark.createDataFrame(results_pdf))

# COMMAND ----------

# MAGIC %md
# MAGIC ## How SCD Type 2 Works (For the Team)
# MAGIC
# MAGIC **Example: Account default flag changes from N to Y**
# MAGIC
# MAGIC | account_key | default_flag | _is_current | _effective_from | _effective_to |
# MAGIC |-------------|-------------|-------------|----------------|---------------|
# MAGIC | ACC001 | N | False | 2024-01-01 | 2024-08-15 |
# MAGIC | ACC001 | Y | True | 2024-08-15 | NULL |
# MAGIC
# MAGIC **Query current data:** `WHERE _is_current = True`
# MAGIC **Query point-in-time:** `WHERE _effective_from <= '2024-06-01' AND (_effective_to > '2024-06-01' OR _effective_to IS NULL)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC | Scenario | How |
# MAGIC |----------|-----|
# MAGIC | Process all tables | Run notebook with blank `table_name` |
# MAGIC | Process one table | Set `table_name = "hlacctbase_final"` |
# MAGIC | Onboard new table | Add row to `master_table_inventory.csv`, run notebook |
# MAGIC | Switch table from historical to incremental | Update `load_type` in CSV + add `primary_key` and `watermark_column` |
