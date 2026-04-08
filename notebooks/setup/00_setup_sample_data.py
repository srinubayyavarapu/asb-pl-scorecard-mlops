# Databricks notebook source

# MAGIC %md
# MAGIC # Setup - Generate Sample Data (One-Time)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random, numpy as np

catalog = "asb_dev"
spark.sql(f"USE CATALOG {catalog}")
spark.sql("CREATE SCHEMA IF NOT EXISTS retail_gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS retail_ml")

random.seed(42)
np.random.seed(42)
n = 2000

data = []
for i in range(n):
    acct = f"HL{str(i+1).zfill(6)}"
    cs = int(np.random.normal(650, 100))
    lvr = round(np.random.uniform(30, 95), 2)
    inc = round(np.random.uniform(40000, 250000), 2)
    loan = round(np.random.uniform(100000, 1500000), 2)
    ir = round(np.random.uniform(3.5, 8.5), 2)
    mob = int(np.random.uniform(1, 240))
    pv = round(np.random.uniform(200000, 3000000), 2)
    emp = random.choice(["SALARY", "SELF_EMPLOYED", "CONTRACT", "RETIRED", "OTHER"])
    pt = random.choice(["RESIDENTIAL", "INVESTMENT", "COMMERCIAL", "RURAL"])
    reg = random.choice(["AUCKLAND", "WELLINGTON", "CHRISTCHURCH", "HAMILTON", "TAURANGA"])
    risk = ((900-cs)/600*0.3 + lvr/100*0.2 + (1-inc/250000)*0.15 + ir/10*0.15 + (1-mob/240)*0.1 + (0.1 if emp in ["CONTRACT","OTHER"] else 0) + random.uniform(-0.1,0.1))
    df_flag = 1 if risk > 0.65 else 0
    obs = f"{random.choice([2021,2022,2023,2024])}-{str(random.randint(1,12)).zfill(2)}-01"
    data.append((acct, cs, lvr, inc, loan, ir, mob, pv, emp, pt, reg, df_flag, obs, "ACTIVE"))

schema = StructType([
    StructField("account_key", StringType()), StructField("credit_score", IntegerType()),
    StructField("lvr", DoubleType()), StructField("annual_income", DoubleType()),
    StructField("loan_amount", DoubleType()), StructField("interest_rate", DoubleType()),
    StructField("months_on_book", IntegerType()), StructField("property_value", DoubleType()),
    StructField("employment_type", StringType()), StructField("property_type", StringType()),
    StructField("region", StringType()), StructField("default_flag", IntegerType()),
    StructField("observation_date", StringType()), StructField("account_status", StringType()),
])

df = spark.createDataFrame(data, schema).withColumn("observation_date", F.to_date("observation_date"))
sample_table = f"{catalog}.retail_silver.hl_sample_data"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(sample_table)

total = df.count()
bad = df.filter("default_flag = 1").count()
print(f"Written: {sample_table} | {total:,} rows | Bad rate: {bad/total:.2%}")
dbutils.notebook.exit(f"SUCCESS|{total}")
