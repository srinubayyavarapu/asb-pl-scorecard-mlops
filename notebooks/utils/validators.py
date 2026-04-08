# Databricks notebook source

# MAGIC %md
# MAGIC # Validators
# MAGIC Shared data validation utilities for ETL and ML notebooks.
# MAGIC Usage: `%run ../utils/validators`

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def validate_row_count(df, expected_min, table_name="DataFrame"):
    """
    Check that DataFrame has at least expected_min rows.

    Args:
        df: Spark DataFrame
        expected_min: Minimum expected row count
        table_name: Name for logging

    Returns:
        tuple: (passed: bool, actual_count: int)

    Example:
        passed, count = validate_row_count(df, 10000, "training_data")
    """
    actual = df.count()
    passed = actual >= expected_min

    if not passed:
        print(f"[FAIL] {table_name}: Expected >= {expected_min:,} rows, got {actual:,}")
    else:
        print(f"[PASS] {table_name}: {actual:,} rows (>= {expected_min:,})")

    return passed, actual


def validate_no_nulls(df, columns, table_name="DataFrame"):
    """
    Check that specified columns have no null values.

    Args:
        df: Spark DataFrame
        columns: List of column names to check
        table_name: Name for logging

    Returns:
        dict: {column_name: null_count} for columns with nulls

    Example:
        nulls = validate_no_nulls(df, ["account_id", "default_flag"])
    """
    null_report = {}

    for col_name in columns:
        if col_name not in df.columns:
            print(f"[WARN] {table_name}: Column '{col_name}' not found, skipping")
            continue

        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            total = df.count()
            pct = round(null_count / total * 100, 2)
            null_report[col_name] = null_count
            print(f"[FAIL] {table_name}.{col_name}: {null_count:,} nulls ({pct}%)")
        else:
            print(f"[PASS] {table_name}.{col_name}: No nulls")

    return null_report


def validate_schema(df, expected_columns, table_name="DataFrame"):
    """
    Check that DataFrame contains all expected columns.

    Args:
        df: Spark DataFrame
        expected_columns: List of column names that must exist
        table_name: Name for logging

    Returns:
        tuple: (passed: bool, missing_columns: list)

    Example:
        passed, missing = validate_schema(df, ["account_id", "default_flag"])
    """
    actual_cols = set(df.columns)
    expected_cols = set(expected_columns)
    missing = expected_cols - actual_cols

    if missing:
        print(f"[FAIL] {table_name}: Missing columns: {sorted(missing)}")
        return False, sorted(missing)
    else:
        print(f"[PASS] {table_name}: All {len(expected_columns)} expected columns present")
        return True, []


def validate_unique(df, key_columns, table_name="DataFrame"):
    """
    Check that key columns form a unique key (no duplicates).

    SAS Equivalent: PROC SORT NODUPKEY check

    Args:
        df: Spark DataFrame
        key_columns: List of columns that should be unique together
        table_name: Name for logging

    Returns:
        tuple: (passed: bool, duplicate_count: int)
    """
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    duplicates = total - distinct

    if duplicates > 0:
        print(f"[FAIL] {table_name}: {duplicates:,} duplicate rows on key {key_columns}")
    else:
        print(f"[PASS] {table_name}: Unique on {key_columns}")

    return duplicates == 0, duplicates


def validate_value_range(df, column, min_val=None, max_val=None, table_name="DataFrame"):
    """
    Check that values in a column fall within expected range.

    Args:
        df: Spark DataFrame
        column: Column name to check
        min_val: Minimum allowed value (inclusive)
        max_val: Maximum allowed value (inclusive)
        table_name: Name for logging

    Returns:
        tuple: (passed: bool, stats: dict)
    """
    stats_row = df.select(
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val"),
        F.avg(column).alias("avg_val"),
    ).collect()[0]

    stats = {
        "min": stats_row["min_val"],
        "max": stats_row["max_val"],
        "avg": stats_row["avg_val"],
    }

    passed = True
    if min_val is not None and stats["min"] is not None and stats["min"] < min_val:
        print(f"[FAIL] {table_name}.{column}: Min value {stats['min']} < expected {min_val}")
        passed = False
    if max_val is not None and stats["max"] is not None and stats["max"] > max_val:
        print(f"[FAIL] {table_name}.{column}: Max value {stats['max']} > expected {max_val}")
        passed = False

    if passed:
        print(f"[PASS] {table_name}.{column}: Range [{stats['min']}, {stats['max']}], Avg: {stats['avg']}")

    return passed, stats


def validate_good_bad_ratio(df, target_column, min_bad_pct=1.0, max_bad_pct=50.0, table_name="DataFrame"):
    """
    Check that the good/bad ratio is within acceptable range for model training.

    Args:
        df: Spark DataFrame
        target_column: Binary target column (0=good, 1=bad)
        min_bad_pct: Minimum bad rate percentage
        max_bad_pct: Maximum bad rate percentage
        table_name: Name for logging

    Returns:
        tuple: (passed: bool, stats: dict)
    """
    total = df.count()
    bad_count = df.filter(F.col(target_column) == 1).count()
    good_count = total - bad_count
    bad_pct = round(bad_count / total * 100, 2) if total > 0 else 0

    stats = {
        "total": total,
        "good": good_count,
        "bad": bad_count,
        "bad_pct": bad_pct,
        "good_bad_ratio": round(good_count / bad_count, 1) if bad_count > 0 else float("inf"),
    }

    passed = min_bad_pct <= bad_pct <= max_bad_pct

    if passed:
        print(f"[PASS] {table_name}: Good={good_count:,}, Bad={bad_count:,}, Bad%={bad_pct}%, Ratio={stats['good_bad_ratio']}:1")
    else:
        print(f"[FAIL] {table_name}: Bad%={bad_pct}% not in [{min_bad_pct}%, {max_bad_pct}%]")

    return passed, stats


def run_all_validations(df, table_name, key_columns=None, required_columns=None, target_column=None):
    """
    Run a standard set of validations on a DataFrame and return summary.

    Args:
        df: Spark DataFrame
        table_name: Name for logging
        key_columns: Columns that should be unique (optional)
        required_columns: Columns that must exist and have no nulls (optional)
        target_column: Binary target column for good/bad ratio check (optional)

    Returns:
        dict: Summary of all validation results
    """
    print(f"\n{'='*60}")
    print(f"VALIDATIONS: {table_name}")
    print(f"{'='*60}")

    results = {}

    # Row count check
    passed, count = validate_row_count(df, 1, table_name)
    results["row_count"] = {"passed": passed, "count": count}

    # Schema check
    if required_columns:
        passed, missing = validate_schema(df, required_columns, table_name)
        results["schema"] = {"passed": passed, "missing": missing}

        # Null check on required columns
        nulls = validate_no_nulls(df, required_columns, table_name)
        results["nulls"] = {"columns_with_nulls": len(nulls), "details": nulls}

    # Uniqueness check
    if key_columns:
        passed, dupes = validate_unique(df, key_columns, table_name)
        results["uniqueness"] = {"passed": passed, "duplicates": dupes}

    # Good/bad ratio check
    if target_column and target_column in df.columns:
        passed, stats = validate_good_bad_ratio(df, target_column, table_name=table_name)
        results["good_bad_ratio"] = {"passed": passed, **stats}

    # Overall pass/fail
    all_passed = all(
        v.get("passed", True) if isinstance(v, dict) else True
        for v in results.values()
    )
    results["overall"] = "PASS" if all_passed else "FAIL"

    print(f"\nOVERALL: {results['overall']}")
    print(f"{'='*60}\n")

    return results
