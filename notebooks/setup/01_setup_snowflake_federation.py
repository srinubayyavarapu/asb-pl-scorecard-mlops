# Databricks notebook source

# MAGIC %md
# MAGIC # Setup Snowflake Lakehouse Federation
# MAGIC Creates connection and foreign catalog for Snowflake access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Snowflake Connection

# COMMAND ----------

# Connection parameters
SNOWFLAKE_ACCOUNT = "fsultcl-ht17125"
SNOWFLAKE_USER = "SRINUBAYYAVARAPU"
SNOWFLAKE_PASSWORD = "Srinubayyavarapu5657"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# COMMAND ----------

# Create connection
try:
    spark.sql(f"""
        CREATE CONNECTION IF NOT EXISTS snowflake_asb_connection
        TYPE snowflake
        OPTIONS (
            host '{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com',
            user '{SNOWFLAKE_USER}',
            password '{SNOWFLAKE_PASSWORD}',
            warehouse '{SNOWFLAKE_WAREHOUSE}'
        )
    """)
    print("Connection created successfully!")
except Exception as e:
    print(f"Connection creation failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Connection

# COMMAND ----------

spark.sql("SHOW CONNECTIONS").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Foreign Catalog

# COMMAND ----------

try:
    spark.sql("""
        CREATE FOREIGN CATALOG IF NOT EXISTS snowflake_asb
        USING CONNECTION snowflake_asb_connection
        OPTIONS (database 'ASB_ANALYTICS')
    """)
    print("Foreign catalog created successfully!")
except Exception as e:
    print(f"Foreign catalog creation failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Foreign Catalog Access

# COMMAND ----------

# List schemas in the foreign catalog
try:
    spark.sql("SHOW SCHEMAS IN snowflake_asb").display()
except Exception as e:
    print(f"Cannot access foreign catalog: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Target Schemas in Dev Catalog

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS dev.bronze COMMENT 'Bronze layer - raw data from Snowflake'")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev.silver COMMENT 'Silver layer - cleansed and conformed data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS dev.gold COMMENT 'Gold layer - business-ready aggregates'")

print("Target schemas created in dev catalog")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN dev").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test Query on Foreign Table

# COMMAND ----------

# Test reading from Snowflake via federation
try:
    df = spark.sql("SELECT COUNT(*) as cnt FROM snowflake_asb.gdw.od_gdwextract_201201_202408")
    df.display()
    print("Federation query successful!")
except Exception as e:
    print(f"Federation query failed: {e}")
