# Databricks notebook source

# MAGIC %md
# MAGIC # Setup - Credit Card Dataset (One-Time)
# MAGIC Loads the Kaggle Credit Card dataset into Silver for ML framework testing.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

dbutils.widgets.text("catalog", "dev_retail_modelling", "Unity Catalog")
catalog = dbutils.widgets.get("catalog").strip()
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# Dataset schema matching the Kaggle CSV
schema = StructType([
    StructField("customer_id", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("marital_status", StringType()),
    StructField("education_level", StringType()),
    StructField("employment_status", StringType()),
    StructField("annual_income", IntegerType()),
    StructField("credit_score", IntegerType()),
    StructField("number_of_credit_lines", IntegerType()),
    StructField("credit_utilization_ratio", DoubleType()),
    StructField("debt_to_income_ratio", DoubleType()),
    StructField("number_of_late_payments", IntegerType()),
    StructField("tenure_in_years", IntegerType()),
    StructField("total_transactions_last_year", IntegerType()),
    StructField("total_spend_last_year", IntegerType()),
    StructField("defaulted", IntegerType()),
    StructField("clv", IntegerType()),
    StructField("total_transactions", IntegerType()),
    StructField("avg_transaction_amount", DoubleType()),
    StructField("max_transaction_amount", DoubleType()),
    StructField("min_transaction_amount", DoubleType()),
    StructField("fraud_transactions", IntegerType()),
    StructField("unique_merchant_categories", IntegerType()),
    StructField("unique_transaction_cities", IntegerType()),
])

# Read from uploaded CSV (upload cc_dataset.csv to DBFS or Volumes first)
# Option 1: If uploaded to DBFS
# df = spark.read.csv("dbfs:/FileStore/cc_dataset.csv", header=True, schema=schema)

# Option 2: Generate the same data inline for portability
import random
import numpy as np

random.seed(42)
np.random.seed(42)
n = 10000

genders = ["Male", "Female"]
marital = ["Single", "Married", "Divorced"]
education = ["High School", "Bachelor", "Master", "PhD"]
employment = ["Employed", "Self-Employed", "Unemployed"]

data = []
for i in range(n):
    age = random.randint(21, 70)
    income = random.randint(20000, 150000)
    cs = random.randint(300, 850)
    credit_lines = random.randint(1, 10)
    util_ratio = round(random.uniform(0.05, 0.95), 2)
    dti = round(random.uniform(0.05, 0.80), 2)
    late_payments = random.randint(0, 10)
    tenure = random.randint(1, 30)
    txn_last_year = random.randint(5, 200)
    spend_last_year = random.randint(1000, 50000)
    total_txn = random.randint(10, 500)
    avg_txn = round(random.uniform(10, 1000), 2)
    max_txn = round(random.uniform(avg_txn, 2000), 2)
    min_txn = round(random.uniform(1, avg_txn), 2)
    fraud_txn = random.choice([0, 0, 0, 0, 0, 0, 0, 1, 1, 2])
    merchants = random.randint(3, 15)
    cities = random.randint(1, 20)

    # Default probability based on features
    risk = (
        (850 - cs) / 550 * 0.25 +
        dti * 0.20 +
        util_ratio * 0.15 +
        late_payments / 10 * 0.20 +
        (1 - income / 150000) * 0.10 +
        fraud_txn * 0.10 +
        random.uniform(-0.15, 0.15)
    )
    defaulted = 1 if risk > 0.55 else 0
    clv = int(income * tenure * 0.01 * (1 - risk))

    data.append((
        f"CUST_{i+1:05d}", age, random.choice(genders), random.choice(marital),
        random.choice(education), random.choice(employment), income, cs,
        credit_lines, util_ratio, dti, late_payments, tenure,
        txn_last_year, spend_last_year, defaulted, clv, total_txn,
        avg_txn, max_txn, min_txn, fraud_txn, merchants, cities
    ))

df = spark.createDataFrame(data, schema)

# COMMAND ----------

# Write to Silver
silver_table = f"{catalog}.silver.cc_customer_data"
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

total = df.count()
bad = df.filter("defaulted = 1").count()
print(f"Written: {silver_table}")
print(f"Rows: {total:,} | Default rate: {bad/total:.2%}")

display(df.groupBy("defaulted").count())
dbutils.notebook.exit(f"SUCCESS|{total}")
