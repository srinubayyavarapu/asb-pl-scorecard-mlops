# Databricks notebook source

# MAGIC %md
# MAGIC # Unit Tests Runner
# MAGIC
# MAGIC Runs the pure-Python `pytest` suite under `tests/` against the helpers
# MAGIC in `notebooks/utils/` and the master inventory CSV. Designed to be
# MAGIC executed as the **`ml_pl_unit_tests`** bundle job from CI — fails
# MAGIC the notebook (and therefore the job, and therefore CI) if any test
# MAGIC fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locate the bundle root
# MAGIC
# MAGIC When deployed as a bundle, this notebook lives under
# MAGIC `/Workspace/Users/<u>/.bundle/<bundle>/<target>/files/notebooks/tests/...`
# MAGIC The repo root is two levels above `notebooks/tests/`.

# COMMAND ----------

import os
import sys

notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get()
)
# notebook_path ends with .../files/notebooks/tests/run_unit_tests
bundle_root = "/Workspace" + notebook_path.split("/files/")[0] + "/files"
print(f"Bundle root: {bundle_root}")

# Make tests/ + notebooks/utils discoverable
if bundle_root not in sys.path:
    sys.path.insert(0, bundle_root)
tests_dir = os.path.join(bundle_root, "tests")
print(f"tests/ dir : {tests_dir} ({len(os.listdir(tests_dir))} entries)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run pytest

# COMMAND ----------

import pytest

# Plain pytest run, verbose, fail on first error so the surface area in
# CI logs stays small.
exit_code = pytest.main([
    tests_dir,
    "-v",
    "--tb=short",
    "--no-header",
    "--color=no",
])

print(f"\npytest exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail the notebook on test failure

# COMMAND ----------

if exit_code != 0:
    raise RuntimeError(
        f"Unit tests FAILED (pytest exit code {exit_code}). "
        "See output above for the failing assertions."
    )

dbutils.notebook.exit(f"SUCCESS|unit_tests|pytest exit 0")
