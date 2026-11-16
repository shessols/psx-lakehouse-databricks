# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Bronze
# MAGIC Auto Loader picks up only new or rewritten files and appends them, with the source file and load time, to `bronze_daily_prices`.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))
from psx_lakehouse.config import Names

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "psx")
names = Names(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

from psx_lakehouse import pipeline
pipeline.bronze_autoloader(spark, names)
display(spark.sql(f"SELECT _source_file, count(*) AS rows, max(_ingested_at) AS loaded_at FROM {names.table('bronze_daily_prices')} GROUP BY 1 ORDER BY 1 DESC LIMIT 5"))
