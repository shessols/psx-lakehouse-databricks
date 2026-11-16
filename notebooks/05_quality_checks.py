# Databricks notebook source
# MAGIC %md
# MAGIC # 5. Quality checks
# MAGIC Logs every check to `quality_log` and fails the job if any check breaks.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))
from psx_lakehouse.config import Names

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "psx")
names = Names(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

from datetime import datetime, timezone
from psx_lakehouse import pipeline
store = pipeline.DeltaStore(spark, names)
checks = pipeline.quality_checks(store)
run_at = datetime.now(timezone.utc)
log = spark.createDataFrame([(run_at, c["check"], str(c["value"]), c["passed"]) for c in checks],
                            "run_at timestamp, check string, value string, passed boolean")
log.write.mode("append").saveAsTable(names.table("quality_log"))
display(log)
failed = [c["check"] for c in checks if not c["passed"]]
assert not failed, f"Quality checks failed: {failed}"
