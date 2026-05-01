"""End-to-end DEV state verification — ETL tables, Gold, model, scored output, monitoring."""
import json
import subprocess
import time

DBX = r"C:\Users\SrinuBayyavarapu\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe"
PROFILE = "DEV"
WAREHOUSE_ID = "91ae4dd9138a7a49"


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
    if state != "SUCCEEDED":
        raise RuntimeError(d.get("status", {}).get("error", {}))
    return d.get("result", {}).get("data_array", []) or []


def section(title):
    print(f"\n{'='*70}\n{title}\n{'='*70}")


def show(stmt, label=None):
    if label:
        print(f"\n[{label}]")
    rows = sql(stmt)
    for r in rows:
        print("  " + " | ".join(str(v) for v in r))


# ─────────────────────────────────────────────────────────────────
section("[1] Catalog inventory — all PL tables across layers")
for sch in ("bronze", "silver", "gold", "pl_scorecard"):
    rows = sql(f"SHOW TABLES IN dev_retail_modelling.{sch}")
    pl = [r[1] for r in rows if r[1].startswith("pl_") or r[1] == "_etl_control"]
    print(f"\n  {sch:<14} ({len(pl)} tables)")
    for t in pl:
        print(f"    {t}")

# ─────────────────────────────────────────────────────────────────
section("[2] Bronze rowcounts (raw from Snowflake JDBC)")
show("""SELECT 'pl_applications_bronze' AS tbl, COUNT(*) FROM dev_retail_modelling.bronze.pl_applications_bronze
       UNION ALL SELECT 'pl_facilities_bronze', COUNT(*) FROM dev_retail_modelling.bronze.pl_facilities_bronze
       UNION ALL SELECT 'pl_credit_performance_bronze', COUNT(*) FROM dev_retail_modelling.bronze.pl_credit_performance_bronze
       UNION ALL SELECT 'pl_sas_final_scores_bronze', COUNT(*) FROM dev_retail_modelling.bronze.pl_sas_final_scores_bronze""")

# ─────────────────────────────────────────────────────────────────
section("[3] Silver rowcounts + SCD2 metadata")
show("""SELECT 'pl_applications_silver' AS tbl,
              SUM(CASE WHEN _is_current THEN 1 ELSE 0 END) AS current_rows,
              COUNT(*) AS total_rows
       FROM dev_retail_modelling.silver.pl_applications_silver
       UNION ALL SELECT 'pl_facilities_silver',
              SUM(CASE WHEN _is_current THEN 1 ELSE 0 END), COUNT(*)
       FROM dev_retail_modelling.silver.pl_facilities_silver
       UNION ALL SELECT 'pl_sas_final_scores_silver',
              SUM(CASE WHEN _is_current THEN 1 ELSE 0 END), COUNT(*)
       FROM dev_retail_modelling.silver.pl_sas_final_scores_silver
       UNION ALL SELECT 'pl_credit_performance_silver',
              NULL, COUNT(*)
       FROM dev_retail_modelling.silver.pl_credit_performance_silver""")

# ─────────────────────────────────────────────────────────────────
section("[4] Gold — pl_application_scorecard_data (target derivation)")
show("""SELECT target_flag, sample_flag, COUNT(*) AS n
       FROM dev_retail_modelling.gold.pl_application_scorecard_data
       GROUP BY 1, 2 ORDER BY 1, 2""")

# ─────────────────────────────────────────────────────────────────
section("[5] Gold — pl_scorecard_dev_data (population split)")
show("""SELECT _population, target_flag, COUNT(*) AS n
       FROM dev_retail_modelling.gold.pl_scorecard_dev_data
       GROUP BY 1, 2 ORDER BY 1, 2""")

# ─────────────────────────────────────────────────────────────────
section("[6] WoE/IV per feature")
show("""SELECT feature, var_kind, ROUND(SUM(iv), 4) AS total_iv,
              CASE WHEN SUM(iv) >= 0.10 THEN 'STRONG'
                   WHEN SUM(iv) >= 0.02 THEN 'WEAK'
                   ELSE 'EXCLUDED' END AS bucket
       FROM dev_retail_modelling.pl_scorecard.woe_iv
       GROUP BY 1, 2 ORDER BY 3 DESC""")

# ─────────────────────────────────────────────────────────────────
section("[7] Evaluation results — all model versions")
show("""SELECT challenger_version, eval_rows, gini, weighted_gini_by_dp3,
              weighted_gini_by_pop, sas_correlation, comparison_result
       FROM dev_retail_modelling.pl_scorecard.evaluation_results
       ORDER BY challenger_version DESC""")

# ─────────────────────────────────────────────────────────────────
section("[8] Scored output — class distribution + risk grades")
show("""SELECT goodbadflag_inferred, sample_flag, COUNT(*) AS n,
              ROUND(AVG(final_p_good), 4) AS avg_p_good
       FROM dev_retail_modelling.pl_scorecard.scored_output
       GROUP BY 1, 2 ORDER BY 2, 1""")

print("")
show("""SELECT risk_grade, COUNT(*) AS n, ROUND(AVG(final_p_good), 4) AS avg_p_good
       FROM dev_retail_modelling.pl_scorecard.scored_output
       GROUP BY 1 ORDER BY 1""", "Risk grade distribution")

# ─────────────────────────────────────────────────────────────────
section("[9] Monitoring log — latest run metrics")
show("""SELECT population, metric, ROUND(value, 4) AS value, alert_level
       FROM dev_retail_modelling.pl_scorecard.monitoring_log
       WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM dev_retail_modelling.pl_scorecard.monitoring_log)
       ORDER BY metric, population""")

print("\n" + "=" * 70)
print("DEV STATE VERIFICATION COMPLETE")
print("=" * 70)
