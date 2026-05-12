"""Sanity tests for configs/ingestion/master_table_inventory.csv.

Pure-Python — no Spark or Databricks needed. Catches the kind of CSV
drift that has bitten this project (wrong catalog defaults, missing
SCD2 flags, mismatched source DB names) before it reaches a job run.
"""
import csv
import os

import pytest

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "configs", "ingestion",
    "master_table_inventory.csv",
)


EXPECTED_TABLES = [
    # (source_database,            source_table,                          is_scd2, load_type)
    ("DP_CreditApplication",       "DIM_SMApplicationRequest",            "Y",    "incremental"),
    ("DP_CreditApplication",       "DIM_SMOnyxApplication",               "Y",    "incremental"),
    ("DP_CreditApplication",       "DIM_SMApplicationRequestSummary",     "Y",    "incremental"),
    ("DP_CreditApplication",       "FACT_SMApplicationRequest",           "Y",    "incremental"),
    ("DP_CreditApplication",       "FACT_SMBridgeapplicationfacility",    "Y",    "incremental"),
    ("DP_CreditManagement",        "DIM_product",                         "N",    "historical"),
    ("DP_CreditManagement",        "DIM_facility",                        "Y",    "incremental"),
    ("DP_CreditManagement",        "DIM_snapshotdate",                    "N",    "historical"),
    ("DP_CreditManagement",        "FACT_CreditFacility",                 "N",    "incremental"),
]

REQUIRED_COLUMNS = {
    "source_database", "source_schema", "source_table", "source_type",
    "ingestion_mode", "load_type", "primary_key", "watermark_column",
    "target_catalog", "target_bronze_schema", "target_silver_schema",
    "target_bronze_table", "target_silver_table", "description",
    "is_active", "orderby_col", "is_scd2",
}


@pytest.fixture(scope="module")
def rows():
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_csv_loads_with_exactly_9_rows(rows):
    """Sanity: master inventory has the 9 client-side source tables."""
    assert len(rows) == 9, f"expected 9 rows, got {len(rows)}"


def test_required_columns_present(rows):
    """Every column we depend on must exist (catches CSV header drift)."""
    missing = REQUIRED_COLUMNS - set(rows[0].keys())
    assert not missing, f"missing required columns: {sorted(missing)}"


def test_each_row_active(rows):
    """All 9 rows should be active for the demo."""
    for r in rows:
        assert r["is_active"] == "Y", f"{r['source_table']} is not active"


def test_source_db_table_load_type_match(rows):
    """Source DB + table name + SCD2 + load_type all match the spec."""
    actual = [
        (r["source_database"], r["source_table"], r["is_scd2"], r["load_type"])
        for r in rows
    ]
    assert actual == EXPECTED_TABLES, (
        f"CSV rows differ from spec.\n  expected: {EXPECTED_TABLES}\n  actual:   {actual}"
    )


def test_all_rows_use_jdbc_ingestion(rows):
    """We removed federation — every row must say jdbc."""
    for r in rows:
        assert r["ingestion_mode"] == "jdbc", (
            f"{r['source_table']}: ingestion_mode={r['ingestion_mode']!r}, expected 'jdbc'"
        )


def test_no_dev_catalog_leakage(rows):
    """target_catalog must be empty (bundle sets it via ${var.catalog});
    no stale dev_retail_modeling / dev_retail_modelling lurking."""
    for r in rows:
        v = r["target_catalog"].strip()
        assert v == "", f"{r['source_table']}: target_catalog should be blank, got {v!r}"


def test_scd2_rows_have_primary_key_and_orderby(rows):
    """SCD2 needs a PK and an orderby column to merge correctly."""
    for r in rows:
        if r["is_scd2"] != "Y":
            continue
        assert r["primary_key"].strip(), f"{r['source_table']} is SCD2 but no primary_key"
        assert r["orderby_col"].strip(), f"{r['source_table']} is SCD2 but no orderby_col"


def test_incremental_rows_have_watermark(rows):
    """Every incremental row needs SOMETHING to filter on — watermark or orderby."""
    for r in rows:
        if r["load_type"] != "incremental":
            continue
        has_watermark = bool(r["watermark_column"].strip())
        has_orderby   = bool(r["orderby_col"].strip())
        assert has_watermark or has_orderby, (
            f"{r['source_table']}: incremental but neither watermark_column nor orderby_col set"
        )


def test_bronze_silver_target_names_pl_prefixed_or_dim_fact(rows):
    """Bronze/silver table names should follow the established convention
    (lowercase, snake_case, *_bronze / *_silver suffix)."""
    import re
    pattern = re.compile(r"^[a-z][a-z0-9_]*_(bronze|silver)$")
    for r in rows:
        assert pattern.match(r["target_bronze_table"]), (
            f"target_bronze_table {r['target_bronze_table']!r} doesn't match convention"
        )
        assert pattern.match(r["target_silver_table"]), (
            f"target_silver_table {r['target_silver_table']!r} doesn't match convention"
        )
