# Databricks notebook source
# MAGIC %md
# MAGIC # 0. Setup
# MAGIC Creates the schema and the volume that holds the raw files. Safe to rerun.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))
from psx_lakehouse.config import Names

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "psx")
names = Names(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {names.catalog}.{names.schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {names.catalog}.{names.schema}.raw")
print("ready:", names.volume)
