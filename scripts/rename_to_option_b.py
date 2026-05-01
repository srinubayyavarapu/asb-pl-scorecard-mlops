"""Refactor demo catalog/schema layout to align with Option B (Catalog per Team per Env).

Renames applied across the codebase, in a specific order so longer patterns are
matched before shorter ones:

  1. retail_gold.pl_woe_iv          -> pl_scorecard.woe_iv          (move feature-eng output)
  2. retail_gold.pl_scored_output   -> pl_scorecard.scored_output   (move scoring output)
  3. retail_ml.pl_feature_store     -> pl_scorecard.feature_store
  4. retail_ml.pl_monitoring_log    -> pl_scorecard.monitoring_log
  5. retail_ml.pl_evaluation_results-> pl_scorecard.evaluation_results
  6. retail_ml.pl_application_scorecard (registered model name) -> pl_scorecard.application_scorecard
  7. retail_ml.pl_banding_lookup    -> pl_scorecard.banding_lookup
  8. retail_bronze                  -> bronze
  9. retail_silver                  -> silver
 10. retail_gold                    -> gold
 11. retail_ml                      -> pl_scorecard
 12. asb_dev                        -> dev_retail_modelling
 13. asb_stg                        -> stg_retail_modelling
 14. asb_prd / asb_prod             -> prod_retail_modelling

The ordering matters: longer-prefixed patterns (e.g. retail_ml.pl_feature_store)
must run BEFORE generic retail_ml so the qualified rename wins.

Files in scope: notebooks, configs, scripts, resources, docs, CI/CD pipelines, root markdown.
"""
from pathlib import Path

ROOT = Path(
    r"C:\Users\SrinuBayyavarapu\OneDrive - Celebal Technologies Private Limited"
    r"\ASB-Bank-SAS-to-Dbx-Migration"
)

# (search, replace) pairs in apply order
RENAMES = [
    # tables that MOVE between schemas (must run before retail_gold/retail_ml renames)
    ("retail_gold.pl_woe_iv",                   "pl_scorecard.woe_iv"),
    ("retail_gold.pl_scored_output",            "pl_scorecard.scored_output"),
    # retail_ml.<asset> -> pl_scorecard.<asset> (drop pl_ prefix because schema is the use case)
    ("retail_ml.pl_feature_store",              "pl_scorecard.feature_store"),
    ("retail_ml.pl_monitoring_log",             "pl_scorecard.monitoring_log"),
    ("retail_ml.pl_evaluation_results",         "pl_scorecard.evaluation_results"),
    ("retail_ml.pl_application_scorecard",      "pl_scorecard.application_scorecard"),
    ("retail_ml.pl_banding_lookup",             "pl_scorecard.banding_lookup"),
    # generic schema renames
    ("retail_bronze",                           "bronze"),
    ("retail_silver",                           "silver"),
    ("retail_gold",                             "gold"),
    ("retail_ml",                               "pl_scorecard"),
    # catalog renames (after all schema work)
    ("asb_prod",                                "prod_retail_modelling"),
    ("asb_prd",                                 "prod_retail_modelling"),
    ("asb_stg",                                 "stg_retail_modelling"),
    ("asb_dev",                                 "dev_retail_modelling"),
]

# File globs to rewrite
GLOBS = [
    "databricks.yml",
    "configs/ingestion/*.csv",
    "configs/ingestion/*.yml",
    "notebooks/etl/*.py",
    "notebooks/ml/*.py",
    "notebooks/setup/*.py",
    "notebooks/utils/*.py",
    "resources/*.yml",
    "scripts/*.py",
    "scripts/*.json",
    ".azure-pipelines/*.yml",
    ".github/workflows/*.yml",
    "workflow-manager.md",
    "CLAUDE.md",
]

EXCLUDE_NAMES = {
    "rename_to_option_b.py",          # this script itself
    "extend_design_doc_catalog_section.py",  # design doc tooling -- keep examples literal
}


def apply_renames(text: str) -> tuple[str, int]:
    """Apply all rename pairs in order. Return (new_text, total_replacements)."""
    total = 0
    out = text
    for old, new in RENAMES:
        if old in out:
            count = out.count(old)
            out = out.replace(old, new)
            total += count
    return out, total


def main():
    files_changed = 0
    files_seen = 0
    total_replacements = 0
    file_summaries = []

    for glob in GLOBS:
        for path in ROOT.glob(glob):
            if path.name in EXCLUDE_NAMES:
                continue
            files_seen += 1
            try:
                original = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                original = path.read_text(encoding="latin-1")
            new, count = apply_renames(original)
            if count > 0:
                path.write_text(new, encoding="utf-8")
                files_changed += 1
                total_replacements += count
                file_summaries.append((str(path.relative_to(ROOT)), count))

    print(f"\nProcessed: {files_seen} files")
    print(f"Modified:  {files_changed} files")
    print(f"Total replacements: {total_replacements}\n")
    print("Per-file changes:")
    for rel, n in sorted(file_summaries, key=lambda x: -x[1]):
        print(f"  {rel:<70s} {n:>4d}")


if __name__ == "__main__":
    main()
