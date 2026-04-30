"""Query the Gold layer directly via Databricks SQL Statements API."""
import json
import subprocess
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
PROFILE = "DEV"
WAREHOUSE_ID = "6bca2db269a782c7"


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
    out = proc.stdout.strip()
    if not out:
        raise RuntimeError("Empty CLI output")
    d = json.loads(out)
    state = d.get("status", {}).get("state")
    if state != "SUCCEEDED":
        # Poll if pending
        sid = d.get("statement_id")
        for _ in range(20):
            time.sleep(2)
            r = subprocess.run(
                [DBX, "api", "get", f"/api/2.0/sql/statements/{sid}", "--profile", PROFILE],
                capture_output=True, text=True, timeout=30,
            )
            d = json.loads(r.stdout)
            state = d.get("status", {}).get("state")
            if state == "SUCCEEDED":
                break
            if state == "FAILED":
                raise RuntimeError(d.get("status", {}).get("error", {}))
    return d.get("result", {}).get("data_array", []), [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]


def show(stmt, label):
    print(f"\n# {label}")
    print(f"  {stmt}")
    rows, cols = sql(stmt)
    if cols:
        widths = [max(len(c), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
        print("  " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
        print("  " + "-+-".join("-" * w for w in widths))
        for r in rows:
            print("  " + " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))


def main():
    print("=" * 64)
    print("Gold layer verification — pl_application_scorecard_data")
    print("=" * 64)

    show(
        "SELECT COUNT(*) AS rows FROM asb_dev.retail_gold.pl_application_scorecard_data",
        "Row count",
    )
    show(
        "SELECT target_flag, COUNT(*) AS n FROM asb_dev.retail_gold.pl_application_scorecard_data GROUP BY 1 ORDER BY 1",
        "Target distribution",
    )
    show(
        "SELECT sample_flag, COUNT(*) AS n FROM asb_dev.retail_gold.pl_application_scorecard_data GROUP BY 1 ORDER BY 1",
        "Sample flag distribution",
    )
    show(
        """SELECT target_flag, sample_flag, COUNT(*) AS n
           FROM asb_dev.retail_gold.pl_application_scorecard_data
           GROUP BY 1, 2 ORDER BY 1, 2""",
        "target_flag x sample_flag",
    )
    show(
        """SELECT
             COUNT(facility_id)         AS rows_with_facility,
             COUNT(sas_final_p_good)    AS rows_with_sas_score,
             COUNT(max_arrears_days_24mo) AS rows_with_perf_data
           FROM asb_dev.retail_gold.pl_application_scorecard_data""",
        "Join coverage (NULLs after left joins)",
    )


if __name__ == "__main__":
    main()
