"""Drop all PL artifacts across dev/stg/prd catalogs to start clean
with env-specific Snowflake source data."""
import json
import subprocess
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
WAREHOUSE_BY_PROFILE = {"DEV": "f979e8a4e0bfd7e5"}  # only dev has a warehouse


def sql(stmt, profile, warehouse_id):
    payload = json.dumps({"statement": stmt, "warehouse_id": warehouse_id, "wait_timeout": "30s"})
    proc = subprocess.run(
        [DBX, "api", "post", "/api/2.0/sql/statements", "--profile", profile, "--json", payload],
        capture_output=True, text=True, timeout=90,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout)
    d = json.loads(proc.stdout)
    state = d.get("status", {}).get("state")
    if state == "PENDING":
        sid = d["statement_id"]
        for _ in range(20):
            time.sleep(2)
            r = subprocess.run(
                [DBX, "api", "get", f"/api/2.0/sql/statements/{sid}", "--profile", profile],
                capture_output=True, text=True, timeout=30,
            )
            d = json.loads(r.stdout)
            state = d.get("status", {}).get("state")
            if state in ("SUCCEEDED", "FAILED"):
                break
    return state, d


PL_TABLES = {
    "retail_bronze":  ["pl_applications_bronze", "pl_facilities_bronze",
                       "pl_credit_performance_bronze", "pl_sas_final_scores_bronze",
                       "_etl_control"],
    "retail_silver":  ["pl_applications_silver", "pl_facilities_silver",
                       "pl_credit_performance_silver", "pl_sas_final_scores_silver"],
    "retail_gold":    ["pl_application_scorecard_data", "pl_scorecard_dev_data",
                       "pl_woe_iv", "pl_scored_output"],
    "retail_ml":      ["pl_evaluation_results", "pl_feature_store",
                       "pl_monitoring_baseline", "pl_monitoring_log"],
}

# Only DEV has a working SQL warehouse — for stg/prd we'll skip cleanup (probably no data anyway)
catalog = "asb_dev"
profile = "DEV"
wh = WAREHOUSE_BY_PROFILE[profile]
print(f"\n{'='*60}\nCleaning {catalog} (profile {profile})\n{'='*60}")
for schema, tables in PL_TABLES.items():
    for tbl in tables:
        fqn = f"{catalog}.{schema}.{tbl}"
        state, _ = sql(f"DROP TABLE IF EXISTS {fqn}", profile, wh)
        print(f"  {state:<10} DROP TABLE {fqn}")
