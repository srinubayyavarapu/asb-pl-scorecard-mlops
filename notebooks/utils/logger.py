# Databricks notebook source

# MAGIC %md
# MAGIC # Logger
# MAGIC Shared logging utility for all notebooks.
# MAGIC Usage: `%run ../utils/logger`
# MAGIC
# MAGIC Two APIs supported:
# MAGIC - Simple: `get_logger("name").info("msg")` — stock Python logger.
# MAGIC - Rich:   `get_logger("name").info("msg", table_name=..., row_count=..., extra_info=...)`
# MAGIC           plus `logger.log_step(...)` and `logger.flush()` for ETL notebooks.

# COMMAND ----------

import logging
from datetime import datetime

# COMMAND ----------

class _NotebookLogger:
    """
    Wrapper around stdlib logger that accepts ETL-style structured kwargs
    (table_name, row_count, step_name, step_status, extra_info, ...) and
    folds them into the formatted message.

    persist_to_delta is accepted for API compatibility; it currently no-ops
    on Databricks serverless (Delta persistence can be wired up later if
    a structured ETL audit table is needed).
    """

    _META_KEYS = (
        "table_name", "row_count",
        "step_name", "step_status",
        "extra_info",
    )

    def __init__(self, name, level=logging.INFO, persist_to_delta=False):
        self._stdlib = logging.getLogger(name)
        self._stdlib.setLevel(level)
        self._persist_to_delta = persist_to_delta

        # Avoid duplicate handlers if %run is called multiple times in a session
        if not self._stdlib.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(level)
            handler.setFormatter(logging.Formatter(
                "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            self._stdlib.addHandler(handler)

    # ── message formatting ─────────────────────────────────────
    def _format(self, msg, **kwargs):
        parts = []
        for k in self._META_KEYS:
            if k in kwargs and kwargs[k] not in (None, ""):
                v = kwargs[k]
                if k == "row_count" and isinstance(v, int):
                    parts.append(f"{k}={v:,}")
                else:
                    parts.append(f"{k}={v}")
        suffix = f"  [{' | '.join(parts)}]" if parts else ""
        return f"{msg}{suffix}"

    # ── pass-through level methods (accept structured kwargs) ──
    def info(self, msg, **kwargs):
        self._stdlib.info(self._format(msg, **kwargs))

    def warning(self, msg, **kwargs):
        self._stdlib.warning(self._format(msg, **kwargs))

    def error(self, msg, **kwargs):
        self._stdlib.error(self._format(msg, **kwargs))

    def debug(self, msg, **kwargs):
        self._stdlib.debug(self._format(msg, **kwargs))

    # ── ETL-style step logging ──────────────────────────────────
    def log_step(self, step_name, status="STARTED", **kwargs):
        sep = "=" * 50
        self._stdlib.info(sep)
        self._stdlib.info(self._format(
            f"STEP: {step_name} | STATUS: {status}", **kwargs
        ))
        self._stdlib.info(sep)

    # ── No-op for the simple logger (Delta sink is a TODO) ─────
    def flush(self):
        # Placeholder — if persist_to_delta is wired up, this would force
        # a write of buffered records to the audit Delta table.
        return None


def get_logger(name, level=logging.INFO, persist_to_delta=False):
    """
    Create a notebook logger.

    Args:
        name: Logger name
        level: Logging level (default INFO)
        persist_to_delta: Reserved for ETL-audit Delta sink (currently no-op)

    Returns:
        _NotebookLogger — supports both simple (.info(msg)) and rich
        (.info(msg, table_name=..., row_count=...)) calling conventions.
    """
    return _NotebookLogger(name, level=level, persist_to_delta=persist_to_delta)

# COMMAND ----------

# Backward-compat module-level helpers (some older notebooks may still use these)

def log_step(logger, step_name, status="STARTED"):
    """Module-level step logger (older API)."""
    if isinstance(logger, _NotebookLogger):
        logger.log_step(step_name, status)
    else:
        sep = "=" * 50
        logger.info(sep)
        logger.info(f"STEP: {step_name} | STATUS: {status}")
        logger.info(sep)


def log_dataframe_info(logger, df, name="DataFrame"):
    """Log basic info about a Spark DataFrame."""
    msg = f"{name} | Rows: {df.count():,} | Columns: {len(df.columns)}"
    logger.info(msg)
