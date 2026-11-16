# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Silver
# MAGIC Types and cleans the prices, keeps the newest version of each stock-day (Delta `MERGE`),
# MAGIC sends rows that break a data-quality rule to `silver_quarantine`, and back-adjusts prices for splits and bonus shares.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))
from psx_lakehouse.config import Names

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "psx")
names = Names(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

from psx_lakehouse import pipeline
store = pipeline.DeltaStore(spark, names)
print(pipeline.silver(store))
display(spark.sql(f"SELECT _dq_issue, count(*) AS rows FROM {names.table('silver_quarantine')} GROUP BY 1"))
