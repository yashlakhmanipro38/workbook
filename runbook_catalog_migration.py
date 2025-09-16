# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Migration: Validation and Access Checklist
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook is intended to validate catalog access after migration from the legacy Hive Metastore to Unity Catalog. It provides structured steps to verify that data assets (catalogs, schemas, and tables) are accessible, and that permissions have been correctly migrated.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Sections
# MAGIC 1. Prerequisites
# MAGIC 2. Validation Checklist
# MAGIC 3. Migration Steps Summary
# MAGIC 4. Known Issues and Troubleshooting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Pre checks
# MAGIC - Unity Catalog enabled in the Databricks workspace.
# MAGIC - The user running the notebook must have appropriate Unity Catalog privileges:
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `SELECT` on tables or views
# MAGIC - Cluster must be Unity Catalog-enabled (shared or single-user mode)
# MAGIC - Ensure external locations, volumes, and credential passthrough (if used) are properly configured
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Validation Checklist
# MAGIC
# MAGIC ### Catalog Access
# MAGIC - [ ] Run `SHOW CATALOGS` to ensure the Unity Catalog catalogs are listed
# MAGIC - [ ] Run `SHOW SCHEMAS IN <catalog>` to validate schema visibility
# MAGIC - [ ] Run `SHOW TABLES IN <catalog>.<schema>` to check table listing
# MAGIC
# MAGIC ### Table Read Access
# MAGIC - [ ] Query sample rows from key tables to confirm read access:
# MAGIC   ```sql
# MAGIC   SELECT * FROM <catalog>.<schema>.<table> LIMIT 10;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("CATALOG_NAME", "main")
dbutils.widgets.text("DB_NAME", "default")
dbutils.widgets.text("table_name", "my_table")
dbutils.widgets.text("hive_db", "")
dbutils.widgets.text("hive_table", "")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("destination_path", "")

# COMMAND ----------

CATALOG_NAME = dbutils.widgets.get("CATALOG_NAME")
DB_NAME = dbutils.widgets.get("DB_NAME")
table_name = dbutils.widgets.get("table_name")
hive_db = dbutils.widgets.get("hive_db")
hive_table = dbutils.widgets.get("hive_table")
source_path = dbutils.widgets.get("source_path")
destination_path = dbutils.widgets.get("destination_path")

# COMMAND ----------

# DBTITLE 1,declare parameters
# declare parameters for this script
CATALOG_NAME = "mktuatkcd"
DB_NAME = "cpra" 
storage_account_name = ["stssemktuatkcd01", "stssemktuatkcd02"]
storage_container_name = "clx-datalake-all"

# COMMAND ----------

# DBTITLE 1,Validate catalogs, schemas
display(spark.sql(f"""SHOW CATALOGS"""))
display(spark.sql(f"""SHOW SCHEMAS in {CATALOG_NAME}"""))

# COMMAND ----------

# DBTITLE 1,create temp table
# To validate write access to catalog.schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a test table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.test_tbl (
        id INT,
        name STRING
    )
    USING DELTA
""")

# define schema and insert data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

data = [(1, "Test_user1"), (2, "Test_user2")]
df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").saveAsTable("test_tbl")
display(spark.sql(f"""select * from {CATALOG_NAME}.{DB_NAME}.test_tbl"""))

# COMMAND ----------

# DBTITLE 1,delete temp table
# If all looks good in above cell, delete temp table
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DB_NAME}.test_tbl")

# COMMAND ----------

# DBTITLE 1,Writing to External Table
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# from pyspark.sql import Row

# # Sample struct schema
# address_schema = StructType([
#     StructField("city", StringType(), True),
#     StructField("zip", StringType(), True)
# ])

# # Sample data
# data = [
#     Row(name="Alice", age=30, address=Row(city="New York", zip="10001")),
#     Row(name="Bob", age=40, address=Row(city="San Francisco", zip="94105"))
# ]

# df = spark.createDataFrame(data, schema=StructType([
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True),
#     StructField("address", address_schema, True)
# ]))

# df.display()

# # Define ADLS path for external Delta location
# container_name = "clx-datalake-all"
# storage_account_name = "stssemktuatkcd01"
# external_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/uc/uc-data/test"

# print(f"Delta external path: {external_path}")

# # Write DataFrame to Delta format at external location
# df.write.format("delta").mode("overwrite").save(external_path)

# # Create external Delta table
# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DB_NAME}.new_tbl
# USING DELTA
# LOCATION '{external_path}'
# """)



# COMMAND ----------

# DBTITLE 1,validate access to key vault
# List all scopes
scopes = dbutils.secrets.listScopes()
print(f"Scopes - {scopes}")

# Loop through scopes and list their keys
for scope in scopes:
    print(f"Scope: {scope.name}")
    try:
        secrets = dbutils.secrets.list(scope.name)
        for secret in secrets:
            print(f"  - Key: {secret.key}")
    except Exception as e:
        print(f"Error accessing scope '{scope.name}': {e}")


# COMMAND ----------

# DBTITLE 1,Validate access to storage account
for storage_acct in storage_account_name:
    try:
        path = f"abfss://{storage_container_name}@{storage_acct}.dfs.core.windows.net/"
        print(f"Reading from: {path}")
        
        files = dbutils.fs.ls(path)
        if files:
            print(f"Files found in {storage_container_name}:")
            for f in files:
                print(f"  - {f.name}")
        else:
            print(f"No files in {storage_container_name}")
    except Exception as e:
        print(f"Error reading from container '{storage_container_name}': {e}")

# TODO: create a dummy file (read and write)


# COMMAND ----------

# DBTITLE 1,steps for mount point
# # Your values
# storage_account_name = "stssemktuatkcd01"
# container_name = "clx-datalake-all"
# storage_account_key = "5mdOS8g+6kF7kX1BrNA+aGb0F1C2Qft/nvc/cHDPw7vmXuDwGpPn+JAsL/nVbJb5wjUCa8pALfn4+AStCcLcHg=="

# # Mount path and source
# mount_point = f"/mnt/{container_name}"
# source_uri = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

# # Mount using wasbs
# dbutils.fs.mount(
#     source = source_uri,
#     mount_point = mount_point,
#     extra_configs = {
#         f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
#     }
# )

# print(f"✅ Mounted successfully using wasbs: {mount_point}")


# COMMAND ----------

# DBTITLE 1,Copying delta files using mount point from UAT01 to UAT01--work with hive cluster
from pyspark.sql import Row

# Function to get total size in MB for a given path
def get_total_size(path):
    try:
        files = dbutils.fs.ls(path)
        total_size = sum(f.size for f in files if not f.path.endswith("/"))
        return round(total_size / (1024 * 1024), 2)  # Return MB, rounded
    except Exception as e:
        return f"Error: {e}"

# Measure sizes before copy
src_size_before = get_total_size(source_path)

# Attempt to copy data
try:
    dbutils.fs.cp(source_path, destination_path, recurse=True)
    copy_status = "Success"
except Exception as e:
    copy_status = f"Failed: {str(e)}"

# Measure sizes after copy
src_size_after = get_total_size(source_path)
dst_size_after = get_total_size(destination_path)

# Determine if sizes match within 1MB tolerance
if isinstance(src_size_after, (float, int)) and isinstance(dst_size_after, (float, int)):
    size_match = abs(src_size_after - dst_size_after) < 1
    validation_status = "Match" if size_match else "Mismatch"
else:
    validation_status = "Could not validate"

# Create DataFrame for tabular output
results = [
    Row(Metric="Source Size Before (MB)", Value=src_size_before),
    Row(Metric="Source Size After (MB)", Value=src_size_after),
    Row(Metric="Destination Size After (MB)", Value=dst_size_after),
    Row(Metric="Copy Status", Value=copy_status),
    Row(Metric="Validation Result", Value=validation_status)
]

results_df = spark.createDataFrame(results)

# Display as a table in notebook
display(results_df)


# COMMAND ----------

# DBTITLE 1,Copying delta files using mount point from UAT02 to UAT02
# # Using Spark to copy data from one mount point to another
# dbutils.fs.cp("/mnt/glad_events_clx/Metadata/", "/mnt/clx-datalake-all02/uc/uc-data/test_uc_1", recurse=True)

# COMMAND ----------

# DBTITLE 1,Migrating External Tables
from pyspark.sql import Row

hive_full_table = f"hive_metastore.{hive_db}.{hive_table}"
uc_full_table = f"{CATALOG_NAME}.{DB_NAME}.{table_name}"

# --- Step 1: Read from Hive Table ---
df_hive = spark.table(hive_full_table)
hive_count = df_hive.count()
hive_schema = df_hive.schema

# 2. Migrate table to Unity Catalog
target_path = "abfss://clx-datalake-all@stssemktuatkcd01.dfs.core.windows.net/uc/uc-data/Curated/Consumer/Multi_Party/Kcd_dataaxle/On-going exports"

# Run the CREATE TABLE statement using f-string
migration_query =f"""
    CREATE TABLE IF NOT EXISTS {uc_full_table}
    USING DELTA
    LOCATION '{target_path}'
"""

spark.sql(migration_query)
print(f"✅ Table migrated to UC: {uc_full_table}")

# --- Step 3: Read from UC Table ---
df_uc = spark.table(uc_full_table)
uc_count = df_uc.count()
uc_schema = df_uc.schema

# --- Step 4: Validation ---
row_count_match = hive_count == uc_count
schema_match = hive_schema == uc_schema
migration_status = "✅ Success" if row_count_match and schema_match else "❌ Failed"

# --- Step 5: Format schemas for display ---
def format_schema(schema):
    return ", ".join([f"{f.name}:{f.dataType.simpleString()}" for f in schema])

hive_schema_str = format_schema(hive_schema)
uc_schema_str = format_schema(uc_schema)

# --- Step 6: Create Summary Table ---
summary = [
    Row(Metric="Hive Table Name", Value=hive_full_table),
    Row(Metric="UC Table Name", Value=uc_full_table),
    Row(Metric="Hive Row Count", Value=str(hive_count)),
    Row(Metric="UC Row Count", Value=str(uc_count)),
    Row(Metric="Row Count Match", Value="✅ Yes" if row_count_match else "❌ No"),
    Row(Metric="Schema Match", Value="✅ Yes" if schema_match else "❌ No"),
    Row(Metric="Migration Status", Value=migration_status),
    Row(Metric="Hive Schema", Value=hive_schema_str),
    Row(Metric="UC Schema", Value=uc_schema_str)
]

summary_df = spark.createDataFrame(summary)

# --- Step 7: Display in Notebook ---
display(summary_df)

# COMMAND ----------

# DBTITLE 1,Migrating External Table with with csv file
df = spark.read.format("csv") \
    .option("header", "true") \        
    .option("inferSchema", "true") \   
    .load("/FileStore/tables/optpaths.csv")
# dbfs:/FileStore/tables/optpaths.csv
# Display the DataFrame
display(df)

# COMMAND ----------

# add volume creation command

# Once migration complete and signed off, drop the hive meta store db and tables, and storage containers/files

# COMMAND ----------

# TODO: 
# ------------
# copy file from existing hive metastore to unity catalog (Can we do that from hive meta store cluster) or do we need manually copy in storage explorer

# steps to copy tables from hive metastore to unity catalog
    # steps for copying managed tables and external tables
    # validate the counts before and after 

# ------------
# create a notebook to run from ADF to validate access





# COMMAND ----------

# DBTITLE 1,Migrating managed Tables
from pyspark.sql import Row

# --- Table Names ---
hive_full_table = f"hive_metastore.{hive_db}.{hive_table}"
uc_full_table = f"{CATALOG_NAME}.{DB_NAME}.{table_name}"

# --- Step 1: Read from Hive Table ---
df_hive = spark.table(hive_full_table)
hive_count = df_hive.count()
hive_schema = df_hive.schema

# 2. Migrate table to Unity Catalog
migration_query = f"""
CREATE OR REPLACE TABLE {uc_full_table} AS
SELECT * FROM {hive_full_table}
"""
spark.sql(migration_query)
print(f"✅ Table migrated to UC: {uc_full_table}")

# --- Step 3: Read from UC Table ---
df_uc = spark.table(uc_full_table)
uc_count = df_uc.count()
uc_schema = df_uc.schema

# --- Step 4: Validation ---
row_count_match = hive_count == uc_count
schema_match = hive_schema == uc_schema
migration_status = "✅ Success" if row_count_match and schema_match else "❌ Failed"

# --- Step 5: Format Schema Strings ---
def format_schema(schema):
    return ", ".join([f"{f.name}:{f.dataType.simpleString()}" for f in schema])

hive_schema_str = format_schema(hive_schema)
uc_schema_str = format_schema(uc_schema)

# --- Step 6: Create Summary Table ---
summary = [
    Row(Metric="Hive Table Name", Value=hive_full_table),
    Row(Metric="UC Table Name", Value=uc_full_table),
    Row(Metric="Hive Row Count", Value=str(hive_count)),
    Row(Metric="UC Row Count", Value=str(uc_count)),
    Row(Metric="Row Count Match", Value="✅ Yes" if row_count_match else "❌ No"),
    Row(Metric="Schema Match", Value="✅ Yes" if schema_match else "❌ No"),
    Row(Metric="Migration Status", Value=migration_status),
    Row(Metric="Hive Schema", Value=hive_schema_str),
    Row(Metric="UC Schema", Value=uc_schema_str)
]

summary_df = spark.createDataFrame(summary)

# --- Step 7: Display Results ---
display(summary_df)

# COMMAND ----------

# ------------

# This section wii be  priortized after dev is completed- 9/11/2025

# NOTE: This is not from hive metastore (this is just an example direclty under unity catalog) not a priority as part of UAT validation, but need to get it done
# convert a managed table to external table
# convert a external table to managed table