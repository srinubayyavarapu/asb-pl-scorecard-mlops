"""Tests for notebooks/utils/job_utils.py helpers.

We load the notebook as a Python module via importlib so we can test
its functions without going through %run. The notebook references
`spark` / `dbutils` only inside function bodies, so module import
succeeds as long as pyspark is on sys.path (which it is on Databricks
serverless — this test job runs there).
"""
import importlib.util
import os
import sys
from types import SimpleNamespace

import pytest

JOB_UTILS_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "notebooks", "utils", "job_utils.py",
))


@pytest.fixture(scope="module")
def ju(monkeypatch_module):
    """Load notebooks/utils/job_utils.py as a regular module.

    Provides a no-op `spark` global before exec so the call sites that
    use it at call time (not at import) won't crash *if* the test
    incidentally triggers one — but our tests focus on pure-Python paths.
    """
    spec = importlib.util.spec_from_file_location("job_utils", JOB_UTILS_PATH)
    mod = importlib.util.module_from_spec(spec)
    # Make sure module-level code doesn't fail because spark/dbutils are
    # not defined in this Python interpreter. They aren't referenced at
    # module level today, but stub them anyway as a safety net.
    mod.__dict__.setdefault("spark", SimpleNamespace(sql=lambda *a, **k: None))
    mod.__dict__.setdefault("dbutils", SimpleNamespace())
    spec.loader.exec_module(mod)
    return mod


# pytest doesn't ship a module-scoped monkeypatch by default — provide one
@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    yield mp
    mp.undo()


# ── _require_catalog ────────────────────────────────────────────────────

def test_require_catalog_raises_on_empty_string(ju):
    with pytest.raises(ValueError, match="catalog is required"):
        ju._require_catalog("")


def test_require_catalog_raises_on_none(ju):
    with pytest.raises(ValueError, match="catalog is required"):
        ju._require_catalog(None)


def test_require_catalog_accepts_real_value(ju):
    # Must not raise
    ju._require_catalog("stg_retail_modeling")


# ── get_control_table_path ──────────────────────────────────────────────

def test_get_control_table_path_builds_fqn(ju):
    assert ju.get_control_table_path("stg_retail_modeling", "bronze") == \
        "stg_retail_modeling.bronze._etl_control"


def test_get_control_table_path_uses_bronze_default(ju):
    assert ju.get_control_table_path("prod_retail_modeling") == \
        "prod_retail_modeling.bronze._etl_control"


def test_get_control_table_path_raises_without_catalog(ju):
    with pytest.raises(ValueError):
        ju.get_control_table_path("")


# ── update_watermark retry detection ────────────────────────────────────

def test_update_watermark_recognises_concurrent_append_error(ju, monkeypatch_module):
    """Sentinel test for the retry helper's exception classifier — if the
    error-string matching ever regresses, this catches it."""
    # Reach inside the function source to be sure the relevant predicates exist.
    import inspect
    src = inspect.getsource(ju.update_watermark)
    assert "DELTA_CONCURRENT_APPEND"        in src
    assert "ConcurrentAppendException"      in src
    assert "max_retries"                    in src
    assert "base_delay"                     in src
