"""Find any leftover cc_* artifacts in DEV catalog (tables, models, schemas)."""
import json
import subprocess
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
PROFILE = "DEV"
WAREHOUSE_ID = "5cbb1fb77a1d43ff"


def sql(stmt):
    payload = json.dumps({
        "statement":   stmt,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "30s",
    })
    proc = subprocess.run(
        [DBX, "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True, timeout=90,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"CLI failed: {proc.stderr or proc.stdout}")
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
    if state != "SUCCEEDED":
        raise RuntimeError(f"Query failed: {d.get('status', {}).get('error', {})}")
    return d.get("result", {}).get("data_array", []) or []


print("\n[1] Schemas in dev_retail_modelling:")
rows = sql("SHOW SCHEMAS IN dev_retail_modelling")
for r in rows:
    print(f"  {r[0]}")

for sch in ("pl_scorecard", "gold", "bronze", "silver"):
    print(f"\n[Tables in dev_retail_modelling.{sch}]")
    try:
        rows = sql(f"SHOW TABLES IN dev_retail_modelling.{sch}")
        if not rows:
            print("  (empty)")
        for r in rows:
            # SHOW TABLES returns: database, tableName, isTemporary
            print(f"  {r[1]}")
    except Exception as e:
        print(f"  ERROR: {e}")
