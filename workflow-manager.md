# ASB Bank SAS-to-Databricks Migration: Workflow Manager

Reference guide for running the metadata-driven ETL ingestion framework end to end.

---

## Architecture Overview

```
Snowflake (ASB_ANALYTICS)
    |
    | Lakehouse Federation / JDBC
    v
Bronze (bronze)        -- raw data + ingestion metadata
    |
    | 02_bronze_to_silver.py
    v
Silver (silver)        -- cleansed, SCD Type 2 tracked
    |
    v
Gold   (gold)          -- business-ready, ML-ready
```

---

## Phase 0: One-Time Setup

### 0.1 Create Snowflake Database and Schemas

- **Where:** Snowflake worksheet
- **Script:** `scripts/setup_snowflake_trial.sql`
- **Creates:** Database `ASB_ANALYTICS`, schemas `CREDIT_RISK` and `GDW`, warehouse `COMPUTE_WH`

### 0.2 Load Synthetic Test Data into Snowflake

- **Where:** Local terminal
- **Script:** `scripts/generate_synthetic_data.py`
- **Command:**
  ```bash
  python scripts/generate_synthetic_data.py \
    --account <snowflake_account_id> \
    --user <username> \
    --password <password>
  ```
- **Generates:** Home loans (5K), credit cards (3K), personal loans (2K), overdraft (1K), SME (500), GDW extracts (36 months), default flags (~5% rate)

### 0.3 Set Databricks Environment Variables

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

### 0.4 Deploy Databricks Asset Bundle

```bash
databricks bundle deploy --target dev
```

- **Config:** `databricks.yml`
- **Targets:** `dev` (dev_retail_modelling), `stg` (stg_retail_modelling), `prod` (prod_retail_modelling)

### 0.5 Initialize Unity Catalog

- **Where:** Databricks workspace
- **Notebook:** `notebooks/setup/00_init_workspace.py`
- **Creates:** Catalog + schemas (`bronze`, `silver`, `gold`, `pl_scorecard`)

---

## Phase 1: Snowflake to Bronze

**Notebook:** `notebooks/etl/01_ingest_snowflake.py`
**Parameter:** `table_name` (e.g., `HLACCTBASE_FINAL`)

| Step | Action |
|------|--------|
| 1.1 | Receives `table_name` parameter |
| 1.2 | Reads `configs/ingestion/master_table_inventory.csv` to find matching row |
| 1.3 | Extracts config: source DB/schema/table, ingestion mode, load type, watermark column |
| 1.4 | **Historical load:** reads full table from Snowflake foreign catalog, overwrites Bronze |
| 1.5 | **Incremental load:** reads only rows where `watermark_column > last ingested value`, appends to Bronze |
| 1.6 | Adds metadata columns: `_ingested_at`, `_source_system`, `_source_table`, `_load_type`, `_ingestion_id` |
| 1.7 | Writes to Bronze (e.g., `dev_retail_modelling.bronze.hlacctbase_final`) |
| 1.8 | Validates row count match and metadata column presence |
| 1.9 | Returns `SUCCESS\|table_name\|row_count\|load_type\|elapsed` |

---

## Phase 2: Bronze to Silver

**Notebook:** `notebooks/etl/02_bronze_to_silver.py`
**Parameter:** `table_name`

### Step 2.1 -- Config Loading

- Reads `master_table_inventory.csv`
- Extracts: `primary_key`, `load_type`, `hist_tablename`, `orderby_col`, target catalog/schemas

### Step 2.2 -- Strategy Selection

| Condition | Strategy |
|-----------|----------|
| Silver table does not exist | `INITIAL_LOAD` |
| `load_type = historical` or no primary key | `FULL_OVERWRITE` |
| `load_type = incremental` + primary key present | `SCD_TYPE_2` |

### Step 2.3 -- Column Standardization

- Lowercase all column names
- Replace special characters with underscores
- Collapse multiple underscores

### Step 2.4a -- INITIAL_LOAD / FULL_OVERWRITE Path

| Step | Action |
|------|--------|
| Dedup | `dropDuplicates` by primary key (or all columns if no key) |
| Add SCD columns | `_is_current=true`, `_effective_from=now`, `_effective_to=null`, `_row_hash=null` |
| Write | Full overwrite to Silver with `overwriteSchema=true` |

### Step 2.4b -- SCD_TYPE_2 Path (History Merge)

| Step | Action |
|------|--------|
| 2.4b.1 | Load silver schema as reference |
| 2.4b.2 | Convert binary columns to hex in Bronze |
| 2.4b.3 | **`align_schema()`**: clean column names, add missing columns as NULL, cast mismatched date/timestamp types using multiple format patterns |
| 2.4b.4 | Load history tables (from `hist_tablename` in CSV), stamp synthetic GoldenGate metadata (`gg_op_type='H'`, dummy timestamps), align to silver schema |
| 2.4b.5 | **Union** bronze + all aligned history DataFrames |
| 2.4b.6 | Add audit columns: `dbr_load_time_silver`, `dbr_load_date_silver` |
| 2.4b.7 | Trim key columns and order-by columns |
| 2.4b.8 | **Dedup**: `ROW_NUMBER()` partitioned by primary key, ordered by `orderby_col` DESC, keep rank 1 |
| 2.4b.9 | **Build `_row_hash`**: MD5 hash of all data columns (excludes keys, SCD metadata, GG metadata, audit columns) |
| 2.4b.10 | Add SCD columns: `_is_current=true`, `_effective_from=now`, `_effective_to=null`, `_silver_processed_at=now` |
| 2.4b.11 | If silver table lacks SCD columns: `ALTER TABLE ADD COLUMNS`, backfill existing rows as current, backfill `_row_hash` |
| 2.4b.12 | **Merge Step 1** (match on `primary_key + _is_current=true`): |
| | -- Changed + not DELETE: close old row (`_is_current=false`, `_effective_to=now`) |
| | -- DELETE operation: close old row (soft delete, no replacement) |
| | -- Not matched + not DELETE: insert as new current row |
| 2.4b.13 | **Merge Step 2**: find keys closed in Step 1 that have no current row, insert new current versions from incoming data |

### Step 2.5 -- Data Quality Check

- Count current rows (`_is_current = true`)
- Count columns with null values (on current records only)
- Report column count (non-metadata)

### Step 2.6 -- Silver Validation

- Compare bronze vs silver row counts
- Verify all SCD columns exist: `_silver_processed_at`, `_is_current`, `_effective_from`, `_effective_to`, `_row_hash`
- Report elapsed time
- Return `SUCCESS|table_name|row_count|strategy|elapsed`

---

## Phase 3: Orchestration (Per-Table Jobs)

Each table in `master_table_inventory.csv` has its own dedicated Databricks job. Every job contains a two-task chain with a dependency:

```
┌─────────────────────┐       ┌─────────────────────┐
│  ingest_to_bronze   │──────>│  bronze_to_silver    │
│  (01_ingest_snowflake)│      │  (02_bronze_to_silver)│
└─────────────────────┘       └─────────────────────┘
       Task 1                        Task 2
                              (depends on Task 1)
```

This design means:
- Each table's ingestion is independently runnable, schedulable, and monitorable
- A failure in one table's job does not block other tables
- You can view task-level outputs (Bronze result, Silver result) separately in the Databricks Jobs UI

### Job Definitions

**File:** `resources/etl_ingestion_job.yml`

| Job Name | DAB Key | Table | Load Type | Description |
|----------|---------|-------|-----------|-------------|
| `asb-etl-hlacctbase-final-${env}` | `etl_hlacctbase_final` | HLACCTBASE_FINAL | historical | Home loan account base full load (212 GB) |
| `asb-etl-hlacctbase-final-incr-${env}` | `etl_hlacctbase_final_incr` | HLACCTBASE_FINAL | incremental | Home loan account base incremental SCD2 |

Each job is tagged with `source_table`, `load_type`, and `domain` for filtering in the Databricks Jobs UI.

### Running Jobs

```bash
# Deploy all jobs to the target environment
databricks bundle deploy --target dev

# Run a specific table's job
databricks bundle run etl_hlacctbase_final --target dev
databricks bundle run etl_hlacctbase_final_incr --target dev

# Override parameters at runtime
databricks bundle run etl_hlacctbase_final --target dev --params force_full_load=Y
```

### Monitoring Job Outputs

Each job run shows two task outputs in the Databricks Jobs UI:

| Task | Output Format | Example |
|------|--------------|---------|
| `ingest_to_bronze` | `SUCCESS\|table\|rows\|load_type\|elapsed` | `SUCCESS\|hlacctbase_final\|1500000\|historical\|45.2s` |
| `bronze_to_silver` | `SUCCESS\|table\|rows\|strategy\|elapsed` | `SUCCESS\|hlacctbase_final\|1500000\|INITIAL_LOAD\|32.1s` |

To check job status via CLI:

```bash
# List recent runs for a specific job
databricks jobs list --output JSON | grep "asb-etl-hlacctbase"

# Get run details
databricks runs get --run-id <run_id>
```

### Adding a New Table's Job

When adding a new table to the framework, a corresponding job must be added to `resources/etl_ingestion_job.yml`:

1. Add row to `configs/ingestion/master_table_inventory.csv`
2. Add a new job block to `resources/etl_ingestion_job.yml` following this template:

```yaml
    etl_<table_name_lowercase>:
      name: "asb-etl-<table-name-kebab>-${var.environment}"
      description: "<description>"
      tags:
        source_table: "<SOURCE_TABLE>"
        load_type: "<historical|incremental>"
        domain: "<domain>"

      parameters:
        - name: table_name
          default: "<SOURCE_TABLE>"
        - name: force_full_load
          default: "N"

      tasks:
        - task_key: ingest_to_bronze
          notebook_task:
            notebook_path: ./notebooks/etl/01_ingest_snowflake
            base_parameters:
              table_name: "{{job.parameters.table_name}}"
              force_full_load: "{{job.parameters.force_full_load}}"
          environment_key: default

        - task_key: bronze_to_silver
          depends_on:
            - task_key: ingest_to_bronze
          notebook_task:
            notebook_path: ./notebooks/etl/02_bronze_to_silver
            base_parameters:
              table_name: "{{job.parameters.table_name}}"
          environment_key: default

      environments:
        - environment_key: default
          spec:
            client: "2"
```

3. Deploy: `databricks bundle deploy --target dev`
4. Run: `databricks bundle run etl_<table_name_lowercase> --target dev`

### Option B: Python Pipeline Script

```bash
python scripts/run_pipeline.py
```

Runs 3 tables sequentially (hlacctbase_final, hl_gdwextract, add_default_flag), then gold layer joins.

### Option C: Manual Notebook Execution

Run notebooks individually in Databricks workspace, passing `table_name` as widget parameter.

---

## Configuration Files

### master_table_inventory.csv

**Path:** `configs/ingestion/master_table_inventory.csv`

| Column | Purpose |
|--------|---------|
| `source_database` | Snowflake database name |
| `source_schema` | Snowflake schema name |
| `source_table` | Snowflake table name |
| `source_type` | TABLE or VIEW |
| `ingestion_mode` | `federation` or `jdbc` |
| `load_type` | `historical` (full) or `incremental` (watermark) |
| `primary_key` | Comma-separated key columns (drives dedup + SCD2 merge) |
| `watermark_column` | Column for incremental filtering |
| `target_catalog` | Unity Catalog name (empty = default from DAB) |
| `target_bronze_schema` | Bronze schema (default: `bronze`) |
| `target_silver_schema` | Silver schema (default: `silver`) |
| `target_bronze_table` | Bronze table name |
| `target_silver_table` | Silver table name |
| `description` | Human-readable description |
| `is_active` | `Y` or `N` (inactive rows are skipped) |
| `hist_tablename` | Comma-separated FQN of history tables for SCD2 merge (optional) |
| `orderby_col` | Column(s) for dedup ordering in SCD2 (optional, falls back to primary key) |

### snowflake_tables.yml

**Path:** `configs/ingestion/snowflake_tables.yml`

Contains Snowflake connection details (host, user, password via Databricks secrets scope `asb-secrets`), federation catalog name, and per-table metadata (ingestion mode, partition strategy, sizes).

---

## SCD Type 2 Columns on Silver Tables

| Column | Type | Description |
|--------|------|-------------|
| `_is_current` | BOOLEAN | `true` for the active version of each key |
| `_effective_from` | TIMESTAMP | When this version became current |
| `_effective_to` | TIMESTAMP | When this version was closed (`null` if current) |
| `_row_hash` | STRING | MD5 hash of data columns for change detection |
| `_silver_processed_at` | TIMESTAMP | When the row was last processed |

---

## Verification Queries

```sql
-- Bronze row count
SELECT COUNT(*) FROM dev_retail_modelling.bronze.hlacctbase_final;

-- Silver SCD2 summary
SELECT _is_current, COUNT(*) AS rows
FROM dev_retail_modelling.silver.hlacctbase_final
GROUP BY _is_current;

-- Change history for a specific key
SELECT account_key, _is_current, _effective_from, _effective_to, _row_hash
FROM dev_retail_modelling.silver.hlacctbase_final
WHERE account_key = '<some_key>'
ORDER BY _effective_from;

-- Data quality: null distribution on current records
SELECT
  COUNT(*) AS total_current,
  COUNT(account_key) AS non_null_key
FROM dev_retail_modelling.silver.hlacctbase_final
WHERE _is_current = true;
```

---

## Adding a New Table to the Framework

1. Add a row to `configs/ingestion/master_table_inventory.csv` with all column values
2. Optionally add table metadata to `configs/ingestion/snowflake_tables.yml`
3. Add a dedicated job block to `resources/etl_ingestion_job.yml` (see template in Phase 3 above)
4. Deploy: `databricks bundle deploy --target dev`
5. Run: `databricks bundle run etl_<table_name> --target dev`
6. First run auto-selects `INITIAL_LOAD` strategy (silver table does not exist yet)
7. Subsequent incremental runs use `SCD_TYPE_2` if `load_type=incremental` and `primary_key` is set

---

## Troubleshooting

| Issue | Check |
|-------|-------|
| Table not found in master CSV | Verify `source_table` or `target_bronze_table` matches the `table_name` parameter (case-insensitive) |
| SCD columns missing after merge | The notebook auto-adds them via `ALTER TABLE`; check Databricks cluster has write permissions |
| History tables not merging | Verify `hist_tablename` in CSV contains valid fully-qualified table names and tables exist |
| Schema alignment errors | Check silver table schema matches expected types; `align_schema()` handles date/timestamp casting but other type mismatches may need manual review |
| Incremental not picking up new rows | Verify `watermark_column` is set and `_ingestion_id` increments correctly |
| Dedup not working as expected | Verify `primary_key` and `orderby_col` are set correctly in the CSV |
