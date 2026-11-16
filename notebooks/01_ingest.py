# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Ingest
# MAGIC Lands the PSX dataset (one Parquet file per month + the stock list) in the `raw` volume.
# MAGIC Only new months and the last two months are downloaded again.
# MAGIC
# MAGIC If your workspace blocks outbound internet, download `dataset/prices/*.parquet` and
# MAGIC `dataset/stocks.csv` from the source repo and upload them to the volume (`prices/` and `stocks/`) in the UI, then skip this task.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))
from psx_lakehouse.config import Names

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "psx")
names = Names(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

from psx_lakehouse import ingest
print(ingest.land(names.volume))
