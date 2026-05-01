"""Create the Option B catalog and schemas in a Databricks workspace.

Usage:
    python scripts/create_option_b_catalogs.py --profile DEV --catalog dev_retail_modelling
    python scripts/create_option_b_catalogs.py --profile STG --catalog stg_retail_modelling
    python scripts/create_option_b_catalogs.py --profile PRD --catalog prod_retail_modelling
"""
import argparse
import json
import subprocess
import sys
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"

WAREHOUSES = {
    "DEV": "5cbb1fb77a1d43ff",
    "STG": None,  # filled in at runtime if needed
    "PRD": None,
}


def run_sql(profile, warehouse_id, statement, label):
    payload = json.dumps({
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
    })
    print(f"  {label}")
    proc = subprocess.run(
        [DBX, "api", "post", "/api/2.0/sql/statements", "--profile", profile, "--json", payload],
        capture_output=True, text=True, timeout=90,
    )
    if proc.returncode != 0:
        print(f"    FAIL: {proc.stderr or proc.stdout}")
        return False
    out = proc.stdout.strip()
    if not out:
        print("    FAIL: empty response")
        return False
    try:
        d = json.loads(out)
    except json.JSONDecodeError as e:
        print(f"    FAIL: invalid JSON ({e}): {out[:200]}")
        return False
    state = d.get("status", {}).get("state")
    if state == "PENDING":
        sid = d.get("statement_id")
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
    if state == "SUCCEEDED":
        print(f"    OK")
        return True
    err = d.get("status", {}).get("error", {})
    print(f"    FAIL: {state} -- {err}")
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse-id", default=None,
                    help="SQL warehouse ID. Defaults from internal map per profile.")
    args = ap.parse_args()

    warehouse_id = args.warehouse_id or WAREHOUSES.get(args.profile.upper())
    if not warehouse_id:
        print(f"No warehouse_id for profile {args.profile}; pass --warehouse-id")
        sys.exit(1)

    catalog = args.catalog
    print(f"\nProvisioning Option B layout in catalog: {catalog}")
    print(f"  Profile: {args.profile}")
    print(f"  Warehouse: {warehouse_id}")
    print()

    statements = [
        (f"CREATE CATALOG IF NOT EXISTS {catalog}",
         f"CREATE CATALOG {catalog}"),
        (f"COMMENT ON CATALOG {catalog} IS 'Retail Modelling team -- Option B target state'",
         f"comment catalog"),
        (f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze "
         f"COMMENT 'Raw ingested data from Snowflake'",
         f"CREATE SCHEMA bronze"),
        (f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver "
         f"COMMENT 'Cleansed and validated data with SCD2 history'",
         f"CREATE SCHEMA silver"),
        (f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold "
         f"COMMENT 'Feature-engineered model-ready datasets'",
         f"CREATE SCHEMA gold"),
        (f"CREATE SCHEMA IF NOT EXISTS {catalog}.pl_scorecard "
         f"COMMENT 'PL Application Scorecard use-case schema'",
         f"CREATE SCHEMA pl_scorecard"),
    ]

    fails = 0
    for stmt, label in statements:
        ok = run_sql(args.profile, warehouse_id, stmt, label)
        if not ok:
            fails += 1

    print()
    if fails == 0:
        print("All statements succeeded.")
    else:
        print(f"{fails} statements failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
