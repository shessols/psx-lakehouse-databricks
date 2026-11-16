# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Gold
# MAGIC Builds the fact table and the marts: daily movers, sector moves, market breadth and monthly momentum research.

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
print(pipeline.gold(store))
display(spark.table(names.table("gold_strategy_summary")))
