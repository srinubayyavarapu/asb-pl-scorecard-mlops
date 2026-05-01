"""One-off: push the custom Lakeview dashboard to the DEV workspace.
Reads pl_monitoring_dashboard.lvdash.json and creates a dashboard via API."""
import json
import subprocess

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
PROFILE = "DEV"
WAREHOUSE_ID = "91ae4dd9138a7a49"  # asb-demo-wh on dev workspace
DASHBOARD_FILE = r"scripts\pl_monitoring_dashboard.lvdash.json"
PARENT_PATH = "/Workspace/Users/srinu.bayyavarapu@celebaltech.com"

with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
    serialized = f.read()

# Validate JSON parses cleanly before sending
json.loads(serialized)

req = {
    "display_name":         "ASB PL Scorecard - Monitoring (DEV)",
    "warehouse_id":         WAREHOUSE_ID,
    "parent_path":          PARENT_PATH,
    "serialized_dashboard": serialized,
}

# Use a temp file because the JSON has many embedded quotes/newlines
import tempfile, os
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
    json.dump(req, f)
    tmp = f.name

try:
    print(f"Pushing dashboard to {PARENT_PATH} via warehouse {WAREHOUSE_ID}...")
    proc = subprocess.run(
        [DBX, "lakeview", "create", "--json", f"@{tmp}", "--profile", PROFILE],
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        print("ERROR:", proc.stderr or proc.stdout)
        raise SystemExit(1)
    out = json.loads(proc.stdout)
    dashboard_id = out.get("dashboard_id")
    print(f"\n  Dashboard ID:    {dashboard_id}")
    print(f"  Path:            {out.get('path')}")
    print(f"  Open URL:        https://adb-7405618935161265.5.azuredatabricks.net/dashboardsv3/{dashboard_id}")
finally:
    os.unlink(tmp)
