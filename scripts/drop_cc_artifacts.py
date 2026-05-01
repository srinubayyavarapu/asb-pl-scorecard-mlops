"""Drop all leftover cc_* / behaviour-scorecard artifacts in DEV catalog."""
import json
import subprocess
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
PROFILE = "DEV"
WAREHOUSE_ID = "6bca2db269a782c7"


def sql(stmt):
    payload = json.dumps({"statement": stmt, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "30s"})
    proc = subprocess.run(
        [DBX, "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", payload],
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
                [DBX, "api", "get", f"/api/2.0/sql/statements/{sid}", "--profile", PROFILE],
                capture_output=True, text=True, timeout=30,
            )
            d = json.loads(r.stdout)
            state = d.get("status", {}).get("state")
            if state in ("SUCCEEDED", "FAILED"):
                break
    return state, d


targets = [
    "dev_retail_modelling.pl_scorecard.cc_evaluation_results",
    "dev_retail_modelling.pl_scorecard.cc_feature_store",
    "dev_retail_modelling.pl_scorecard.cc_monitoring_baseline",
    "dev_retail_modelling.pl_scorecard.cc_monitoring_log",
    "dev_retail_modelling.gold.cc_scorecard_dev_data",
    "dev_retail_modelling.gold.cc_scored_output",
    "dev_retail_modelling.gold.cc_woe_iv",
    "dev_retail_modelling.silver.cc_customer_data",
]

for tbl in targets:
    state, d = sql(f"DROP TABLE IF EXISTS {tbl}")
    err = d.get("status", {}).get("error", {}).get("message", "") if state != "SUCCEEDED" else ""
    print(f"  {state:<10}  DROP TABLE {tbl}{(' — ' + err) if err else ''}")

# Drop the UC model registry entry if present
print()
state, d = sql("DROP MODEL IF EXISTS dev_retail_modelling.pl_scorecard.cc_behaviour_scorecard")
err = d.get("status", {}).get("error", {}).get("message", "") if state != "SUCCEEDED" else ""
print(f"  {state:<10}  DROP MODEL dev_retail_modelling.pl_scorecard.cc_behaviour_scorecard{(' — ' + err) if err else ''}")
