"""
Run the ETL pipeline on Databricks: Bronze -> Silver -> Gold (3 tables)
"""
import requests
import base64
import time
import json
import os

HOST = os.environ.get("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def upload_notebook(path, content):
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    requests.post(f"{HOST}/api/2.0/workspace/import", headers=HEADERS, json={
        "path": path, "language": "PYTHON", "overwrite": True,
        "content": encoded, "format": "SOURCE",
    })


def run_and_wait(path, desc, timeout_min=5):
    print(f"\nRunning: {desc}")
    resp = requests.post(f"{HOST}/api/2.1/jobs/runs/submit", headers=HEADERS, json={
        "run_name": desc,
        "tasks": [{"task_key": "main", "notebook_task": {"notebook_path": path}, "environment_key": "default"}],
        "environments": [{"environment_key": "default", "spec": {"client": "2"}}],
    })
    run_id = resp.json().get("run_id")
    print(f"  Run ID: {run_id}")

    for i in range(timeout_min * 12):
        check = requests.get(f"{HOST}/api/2.1/jobs/runs/get?run_id={run_id}", headers=HEADERS).json()
        state = check.get("state", {}).get("life_cycle_state", "UNKNOWN")
        result = check.get("state", {}).get("result_state", "")
        if i % 4 == 0:
            print(f"  [{i*5}s] {state} {result}")
        if state == "TERMINATED":
            if result == "SUCCESS":
                print(f"  [OK] {desc}")
                return True
            else:
                out = requests.get(f"{HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}", headers=HEADERS).json()
                print(f"  [FAIL] {out.get('error', 'N/A')[:300]}")
                return False
        elif state == "INTERNAL_ERROR":
            return False
        time.sleep(5)
    return False


# ══════════════════════════════════════
# STEP 1: Silver — 3 tables, one at a time
# ══════════════════════════════════════
print("=" * 50)
print("STEP 1: Bronze -> Silver")
print("=" * 50)

tables_silver = [
    ("hlacctbase_final", '"account_key"'),
    ("hl_gdwextract_199607_202408", '"account_key", "observation_date"'),
    ("add_default_flag_202408", '"account_key"'),
]

silver_ok = True
for tbl, keys in tables_silver:
    nb = f'''# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F
src = "dev_retail_modelling.bronze.{tbl}"
tgt = "dev_retail_modelling.silver.{tbl}"
df = spark.table(src)
b = df.count()
df2 = (df.dropDuplicates([{keys}])
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_is_current", F.lit(True))
    .withColumn("_effective_from", F.current_timestamp())
    .withColumn("_effective_to", F.lit(None).cast("timestamp")))
df2.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(tgt)
s = spark.table(tgt).count()
print(f"{tbl}: {{b}} -> {{s}}")
'''
    path = f"/ASB-Migration/notebooks/_silver_{tbl}"
    upload_notebook(path, nb)
    if not run_and_wait(path, f"Silver: {tbl}"):
        silver_ok = False
        break

# ══════════════════════════════════════
# STEP 2: Gold — join into HL training data
# ══════════════════════════════════════
if silver_ok:
    print("\n" + "=" * 50)
    print("STEP 2: Silver -> Gold (HL Training Data)")
    print("=" * 50)

    nb_gold = '''# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

CAT = "dev_retail_modelling"

hl = spark.table(f"{CAT}.silver.hlacctbase_final")
gdw = spark.table(f"{CAT}.silver.hl_gdwextract_199607_202408")
defaults = spark.table(f"{CAT}.silver.add_default_flag_202408")

print(f"HL: {hl.count()}, GDW: {gdw.count()}, Defaults: {defaults.count()}")

# COMMAND ----------
# Latest GDW per account
w = Window.partitionBy("account_key").orderBy(F.col("observation_date").desc())
gdw_latest = gdw.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# Join
gold = (hl
    .join(gdw_latest.select("account_key","arrears_days","outstanding_balance","monthly_payment","payment_status","exposure_amount"), "account_key", "left")
    .join(defaults.select("account_key","default_flag","max_arrears_days"), "account_key", "left")
    .filter(F.col("account_status") == "ACTIVE")
    .withColumn("_gold_created_at", F.current_timestamp())
    .withColumn("_product_type", F.lit("home_loans")))

gold.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CAT}.gold.hl_scorecard_training")

g = spark.table(f"{CAT}.gold.hl_scorecard_training")
total = g.count()
bad = g.filter("default_flag = 1").count()
print(f"Gold: {total} rows | Bad: {bad} ({round(bad/total*100,2)}%) | Cols: {len(g.columns)}")
'''
    upload_notebook("/ASB-Migration/notebooks/_pipeline_gold", nb_gold)
    gold_ok = run_and_wait("/ASB-Migration/notebooks/_pipeline_gold", "Gold: HL Training Data")
else:
    gold_ok = False

# ══════════════════════════════════════
# STEP 3: Verify
# ══════════════════════════════════════
print("\n" + "=" * 50)
print("STEP 3: Verify All Layers")
print("=" * 50)

nb_verify = '''# Databricks notebook source
# COMMAND ----------
print("=== BRONZE ===")
for t in ["hlacctbase_final", "hl_gdwextract_199607_202408", "add_default_flag_202408"]:
    c = spark.table(f"dev_retail_modelling.bronze.{t}").count()
    print(f"  {t}: {c}")

print("\\n=== SILVER ===")
for t in ["hlacctbase_final", "hl_gdwextract_199607_202408", "add_default_flag_202408"]:
    c = spark.table(f"dev_retail_modelling.silver.{t}").count()
    print(f"  {t}: {c}")

print("\\n=== GOLD ===")
g = spark.table("dev_retail_modelling.gold.hl_scorecard_training")
total = g.count()
bad = g.filter("default_flag = 1").count()
cols = len(g.columns)
print(f"  hl_scorecard_training: {total} rows | Bad: {bad} | Columns: {cols}")
print("\\n=== PIPELINE COMPLETE ===")
'''
upload_notebook("/ASB-Migration/notebooks/_pipeline_verify", nb_verify)
run_and_wait("/ASB-Migration/notebooks/_pipeline_verify", "Verify All Layers")

# Summary
print("\n" + "=" * 50)
print("FINAL RESULTS")
print(f"  Bronze -> Silver: {'SUCCESS' if silver_ok else 'FAILED'}")
print(f"  Silver -> Gold:   {'SUCCESS' if gold_ok else 'FAILED'}")
print("=" * 50)
