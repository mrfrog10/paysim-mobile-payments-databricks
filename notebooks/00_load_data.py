# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Load PaySim into Unity Catalog
# MAGIC
# MAGIC **Source:** [PaySim synthetic mobile-money transactions](https://www.kaggle.com/datasets/ealaxi/paysim1) (Lopez-Rojas et al., 2016), 6,362,620 rows, one CSV (~494 MB).
# MAGIC
# MAGIC This notebook:
# MAGIC 1. creates the schema `workspace.paysim` and a volume `raw` for the file,
# MAGIC 2. downloads the Kaggle zip into the volume (or uses a file you uploaded there by hand),
# MAGIC 3. writes a typed bronze Delta table `paysim.bronze_transactions`.

# COMMAND ----------

CATALOG = "workspace"   # default catalog in Databricks Free Edition
SCHEMA = "paysim"
VOLUME = "raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
vol = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(vol)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the file
# MAGIC Kaggle's public download endpoint works without an API key for this dataset.
# MAGIC If outbound internet is blocked on your compute, upload `PS_20174392719_1491204439457_log.csv`
# MAGIC to the volume through **Catalog → workspace → paysim → raw → Upload to this volume** and re-run.

# COMMAND ----------

import os, zipfile, urllib.request, shutil

csv_name = "PS_20174392719_1491204439457_log.csv"
csv_path = f"{vol}/{csv_name}"

if not os.path.exists(csv_path):
    zip_tmp = "/tmp/paysim.zip"
    url = "https://www.kaggle.com/api/v1/datasets/download/ealaxi/paysim1"
    with urllib.request.urlopen(url, timeout=600) as r, open(zip_tmp, "wb") as f:
        shutil.copyfileobj(r, f)
    with zipfile.ZipFile(zip_tmp) as z:
        with z.open(csv_name) as src, open(csv_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
    os.remove(zip_tmp)

print(csv_path, round(os.path.getsize(csv_path) / 1e6, 1), "MB")

# COMMAND ----------

# Explicit schema: avoids a second pass over 6.3M rows for inference
# and stops the flags being read as strings.
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType()),
    StructField("isFlaggedFraud", IntegerType()),
])

raw = spark.read.csv(csv_path, header=True, schema=schema)
(raw.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_transactions"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_loaded FROM workspace.paysim.bronze_transactions
# MAGIC -- expected: 6,362,620 (same as the local DuckDB check in verification/crosscheck.py)
