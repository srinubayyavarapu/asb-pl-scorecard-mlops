"""In-place rename of an ASB demo workspace to Option B layout.

Usage:
    python scripts/rename_workspace_to_option_b.py \
        --profile STG --old-catalog asb_stg --new-catalog stg_retail_modelling

What it does:
  1. Create / reuse a SQL warehouse (size 2X-Small, serverless PRO, auto-stop 60m).
  2. Rename catalog (UC API call -- preserves storage credential).
  3. Rename the four schemas inside (retail_bronze->bronze, retail_silver->silver,
     retail_gold->gold, retail_ml->pl_scorecard).
  4. Move gold.pl_woe_iv -> pl_scorecard.woe_iv (drop pl_ prefix on use-case tables).
  5. Move gold.pl_scored_output -> pl_scorecard.scored_output.
  6. Rename pl_scorecard.pl_<asset> -> pl_scorecard.<asset> for every table in the schema.
  7. Rename registered model pl_application_scorecard -> application_scorecard.
  8. Create /Shared/ml/pl_scorecard workspace folder for the new MLflow experiment path.

The script is idempotent: existing warehouses are reused, and renames that have
already happened are skipped without error.
"""
import argparse
import json
import subprocess
import sys
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"


def cli(args, capture=True, timeout=90):
    p = subprocess.run([DBX] + args, capture_output=capture, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def get_or_create_warehouse(profile):
    """Return (warehouse_id) of an existing or freshly-created PRO serverless warehouse."""
    rc, out, _ = cli(["warehouses", "list", "--profile", profile, "--output", "json"])
    if rc == 0 and out.strip():
        try:
            warehouses = json.loads(out)
            for w in warehouses:
                if w.get("state") in ("RUNNING", "STOPPED", "STARTING"):
                    print(f"  Using existing warehouse {w['id']} ({w['name']}, {w['state']})")
                    return w["id"]
        except json.JSONDecodeError:
            pass
    print(f"  No existing warehouse, creating new one")
    rc, out, err = cli([
        "warehouses", "create", "--profile", profile, "--json",
        json.dumps({
            "name": "asb-demo-wh",
            "cluster_size": "2X-Small",
            "auto_stop_mins": 60,
            "warehouse_type": "PRO",
            "enable_serverless_compute": True,
            "min_num_clusters": 1,
            "max_num_clusters": 1,
        }),
    ], timeout=180)
    if rc != 0:
        raise RuntimeError(f"Failed to create warehouse: {err or out}")
    d = json.loads(out)
    print(f"  Created warehouse {d['id']}")
    return d["id"]


def sql(profile, warehouse_id, statement, label):
    payload = json.dumps({
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
    })
    rc, out, err = cli([
        "api", "post", "/api/2.0/sql/statements",
        "--profile", profile, "--json", payload,
    ])
    if rc != 0:
        print(f"    {label}: CLI fail -- {err[:120] or out[:120]}")
        return False
    if not out.strip():
        print(f"    {label}: empty response")
        return False
    try:
        d = json.loads(out)
    except json.JSONDecodeError:
        print(f"    {label}: bad JSON -- {out[:120]}")
        return False
    state = d.get("status", {}).get("state")
    if state == "PENDING":
        sid = d.get("statement_id")
        for _ in range(20):
            time.sleep(2)
            rc2, o2, _ = cli([
                "api", "get", f"/api/2.0/sql/statements/{sid}",
                "--profile", profile,
            ], timeout=30)
            if rc2 == 0 and o2.strip():
                d = json.loads(o2)
                state = d.get("status", {}).get("state")
                if state in ("SUCCEEDED", "FAILED"):
                    break
    if state == "SUCCEEDED":
        print(f"    {label}: OK")
        return True
    err = d.get("status", {}).get("error", {})
    print(f"    {label}: {state} -- {str(err)[:160]}")
    return False


def rename_catalog(profile, old, new):
    print(f"\n[1] Rename catalog {old} -> {new}")
    # Check whether already renamed
    rc, out, _ = cli(["catalogs", "get", new, "--profile", profile])
    if rc == 0 and out.strip():
        try:
            d = json.loads(out)
            if d.get("name") == new:
                print(f"  catalog {new} already exists -- skipping rename")
                return True
        except json.JSONDecodeError:
            pass
    rc, out, err = cli([
        "catalogs", "update", old, "--profile", profile,
        "--json", json.dumps({"new_name": new}),
    ])
    if rc != 0:
        print(f"  FAIL: {err or out}")
        return False
    print(f"  OK")
    return True


def rename_schemas(profile, catalog):
    print(f"\n[2] Rename schemas inside {catalog}")
    pairs = [
        ("retail_bronze", "bronze"),
        ("retail_silver", "silver"),
        ("retail_gold",   "gold"),
        ("retail_ml",     "pl_scorecard"),
    ]
    ok = True
    for old, new in pairs:
        # If new already exists, skip
        rc, out, _ = cli(["schemas", "get", f"{catalog}.{new}", "--profile", profile])
        if rc == 0 and out.strip() and json.loads(out).get("name") == new:
            print(f"  {old} -> {new}: already renamed, skipping")
            continue
        rc, out, err = cli([
            "schemas", "update", f"{catalog}.{old}", "--profile", profile,
            "--json", json.dumps({"new_name": new}),
        ])
        if rc != 0:
            print(f"  {old} -> {new}: FAIL -- {err or out}")
            ok = False
        else:
            print(f"  {old} -> {new}: OK")
    return ok


def rename_tables(profile, catalog, warehouse_id):
    print(f"\n[3] Rename tables inside pl_scorecard (drop pl_ prefix) + move from gold")

    # Discover tables currently in pl_scorecard with pl_ prefix
    payload = json.dumps({
        "statement": f"SHOW TABLES IN {catalog}.pl_scorecard",
        "warehouse_id": warehouse_id, "wait_timeout": "30s",
    })
    rc, out, _ = cli([
        "api", "post", "/api/2.0/sql/statements",
        "--profile", profile, "--json", payload,
    ])
    pl_tables = []
    if rc == 0 and out.strip():
        d = json.loads(out)
        rows = d.get("result", {}).get("data_array", []) or []
        pl_tables = [r[1] for r in rows if r[1].startswith("pl_")]

    for tbl in pl_tables:
        new = tbl[len("pl_"):]
        sql(profile, warehouse_id,
            f"ALTER TABLE {catalog}.pl_scorecard.{tbl} RENAME TO {catalog}.pl_scorecard.{new}",
            f"pl_scorecard.{tbl} -> pl_scorecard.{new}")

    # Move use-case tables that landed in gold during earlier runs
    payload = json.dumps({
        "statement": f"SHOW TABLES IN {catalog}.gold",
        "warehouse_id": warehouse_id, "wait_timeout": "30s",
    })
    rc, out, _ = cli([
        "api", "post", "/api/2.0/sql/statements",
        "--profile", profile, "--json", payload,
    ])
    gold_movers = []
    if rc == 0 and out.strip():
        d = json.loads(out)
        rows = d.get("result", {}).get("data_array", []) or []
        for r in rows:
            t = r[1]
            if t in ("pl_woe_iv", "pl_scored_output"):
                gold_movers.append(t)

    for tbl in gold_movers:
        new = tbl[len("pl_"):]
        sql(profile, warehouse_id,
            f"ALTER TABLE {catalog}.gold.{tbl} RENAME TO {catalog}.pl_scorecard.{new}",
            f"gold.{tbl} -> pl_scorecard.{new}")


def rename_model(profile, catalog):
    print(f"\n[4] Rename registered model")
    old_fqn = f"{catalog}.pl_scorecard.pl_application_scorecard"
    new_name = "application_scorecard"
    new_fqn = f"{catalog}.pl_scorecard.{new_name}"

    # If new already exists, skip
    rc, out, _ = cli(["registered-models", "get", new_fqn, "--profile", profile])
    if rc == 0 and out.strip():
        try:
            if json.loads(out).get("name") == new_name:
                print(f"  {new_fqn} already exists -- skipping")
                return True
        except json.JSONDecodeError:
            pass

    # Check whether old exists
    rc, out, _ = cli(["registered-models", "get", old_fqn, "--profile", profile])
    if rc != 0 or not out.strip():
        print(f"  No model at {old_fqn} -- skipping (will be created on first training run)")
        return True

    rc, out, err = cli([
        "registered-models", "update", old_fqn, "--profile", profile,
        "--json", json.dumps({"new_name": new_name}),
    ])
    if rc != 0:
        print(f"  FAIL: {err or out}")
        return False
    print(f"  OK -- {old_fqn} -> {new_fqn}")
    return True


def ensure_workspace_dir(profile, path):
    print(f"\n[5] Ensure workspace folder {path} exists")
    rc, out, err = cli([
        "api", "post", "/api/2.0/workspace/mkdirs",
        "--profile", profile,
        "--json", json.dumps({"path": path}),
    ])
    if rc == 0:
        print(f"  OK")
    else:
        print(f"  WARN: {err or out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--old-catalog", required=True)
    ap.add_argument("--new-catalog", required=True)
    args = ap.parse_args()

    print(f"\n=== Renaming workspace to Option B ===")
    print(f"  Profile: {args.profile}")
    print(f"  {args.old_catalog} -> {args.new_catalog}")

    print(f"\n[0] Warehouse")
    warehouse_id = get_or_create_warehouse(args.profile)

    if not rename_catalog(args.profile, args.old_catalog, args.new_catalog):
        print("Catalog rename failed; aborting")
        sys.exit(1)

    rename_schemas(args.profile, args.new_catalog)
    rename_tables(args.profile, args.new_catalog, warehouse_id)
    rename_model(args.profile, args.new_catalog)
    ensure_workspace_dir(args.profile, "/Shared/ml/pl_scorecard")

    print(f"\nDONE: {args.new_catalog} on profile {args.profile}")
    print(f"Warehouse ID for verification: {warehouse_id}")


if __name__ == "__main__":
    main()
