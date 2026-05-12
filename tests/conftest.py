"""Pytest configuration for ASB PL Scorecard unit tests.

These tests are intentionally pure-Python where possible (no real Spark /
Snowflake / Databricks calls) so they run fast in CI on a Databricks
serverless task.
"""
import os
import sys

# Make `notebooks/...` importable as plain Python modules.
# Notebook files use `# COMMAND ----------` markers (which are valid Python
# comments) and reference `dbutils` / `spark` only inside function bodies,
# so module-level import works fine.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
