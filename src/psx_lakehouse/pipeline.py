"""Steps that read and write tables. Each Databricks notebook calls one of these.

On Databricks tables are Delta in Unity Catalog: silver is upserted with MERGE,
gold is rebuilt and then OPTIMIZEd. `LocalStore` runs the same steps on plain
Parquet so the whole pipeline can be tested without a workspace.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import transforms as T
from .config import Names


class DeltaStore:
    """Unity Catalog + Delta Lake (Databricks)."""

    def __init__(self, spark: SparkSession, names: Names):
        self.spark, self.n = spark, names

    def read(self, name: str) -> DataFrame:
        return self.spark.table(self.n.table(name))

    def overwrite(self, df: DataFrame, name: str, cluster_by: list[str] | None = None) -> None:
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if cluster_by:
            w = w.clusterBy(*cluster_by)
        w.saveAsTable(self.n.table(name))

    def upsert(self, df: DataFrame, name: str, keys: list[str]) -> None:
        from delta.tables import DeltaTable
        full = self.n.table(name)
        if not self.spark.catalog.tableExists(full):
            df.write.format("delta").clusterBy(*keys).saveAsTable(full)
            return
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forName(self.spark, full).alias("t")
         .merge(df.alias("s"), cond)
         .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
         .whenNotMatchedInsertAll()
         .execute())

    def optimize(self, name: str) -> None:
        self.spark.sql(f"OPTIMIZE {self.n.table(name)}")


class LocalStore:
    """Plain Parquet folders, for local runs and CI."""

    def __init__(self, spark: SparkSession, root: str):
        self.spark, self.root = spark, root

    def read(self, name: str) -> DataFrame:
        return self.spark.read.parquet(f"{self.root}/{name}")

    def overwrite(self, df: DataFrame, name: str, cluster_by: list[str] | None = None) -> None:
        df.write.mode("overwrite").parquet(f"{self.root}/{name}")

    def upsert(self, df: DataFrame, name: str, keys: list[str]) -> None:
        # Same result as the Delta MERGE: newest ingest per key wins.
        try:
            old = self.read(name).localCheckpoint()
            df = old.unionByName(df)
        except Exception:  # first run: table does not exist yet
            pass
        from pyspark.sql import Window
        w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        self.overwrite(df.localCheckpoint(), name)

    def optimize(self, name: str) -> None:
        pass


# ------------------------------------------------------------------ steps
def bronze_batch(spark: SparkSession, store, volume: str) -> int:
    """Batch load of every landed file into bronze (used locally)."""
    raw = spark.read.parquet(f"{volume}/prices")
    bronze = T.to_bronze(raw.select("*", "_metadata"))
    store.overwrite(bronze.drop("_metadata"), "bronze_daily_prices")
    stocks = spark.read.option("header", True).csv(f"{volume}/stocks/stocks.csv")
    store.overwrite(stocks.withColumn("_ingested_at", F.current_timestamp()), "bronze_stocks")
    return store.read("bronze_daily_prices").count()


def bronze_autoloader(spark: SparkSession, names: Names) -> None:
    """Incremental bronze with Auto Loader: only new or rewritten files are read."""
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "parquet")
              .option("cloudFiles.schemaLocation", f"{names.checkpoints}/bronze_schema")
              .option("cloudFiles.allowOverwrites", "true")   # the current month's file is rewritten daily
              .load(f"{names.volume}/prices"))
    (T.to_bronze(stream.select("*", "_metadata")).drop("_metadata")
     .writeStream
     .option("checkpointLocation", f"{names.checkpoints}/bronze_daily_prices")
     .trigger(availableNow=True)
     .toTable(names.table("bronze_daily_prices"))
     .awaitTermination())
    stocks = spark.read.option("header", True).csv(f"{names.volume}/stocks/stocks.csv")
    (stocks.withColumn("_ingested_at", F.current_timestamp())
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(names.table("bronze_stocks")))


def silver(store) -> dict:
    clean, quarantine = T.to_silver(store.read("bronze_daily_prices"))
    store.upsert(clean, "silver_daily_prices", ["symbol", "trade_date"])
    store.overwrite(quarantine, "silver_quarantine")
    stocks = (store.read("bronze_stocks")
              .select(F.upper(F.trim("symbol")).alias("symbol"), "name", "sector")
              .dropDuplicates(["symbol"]))
    store.overwrite(stocks, "silver_stocks")
    adjusted, events = T.adjust_corporate_actions(
        store.read("silver_daily_prices").drop("_ingested_at"))
    store.overwrite(adjusted, "silver_adjusted_prices", cluster_by=["symbol", "trade_date"])
    store.overwrite(events, "silver_corporate_actions")
    return {"quarantined": store.read("silver_quarantine").count(),
            "corporate_actions": store.read("silver_corporate_actions").count()}


def gold(store) -> dict:
    fct = T.fct_daily_prices(store.read("silver_adjusted_prices"))
    store.overwrite(fct, "gold_fct_daily_prices", cluster_by=["symbol", "trade_date"])
    fct = store.read("gold_fct_daily_prices")
    store.overwrite(T.mart_daily_movers(fct), "gold_daily_movers", cluster_by=["trade_date"])
    store.overwrite(T.mart_sector_daily(fct, store.read("silver_stocks")), "gold_sector_daily")
    store.overwrite(T.mart_market_breadth(fct), "gold_market_breadth")
    mom = T.mart_momentum_monthly(fct)
    store.overwrite(mom, "gold_momentum_monthly")
    store.overwrite(T.strategy_summary(store.read("gold_momentum_monthly")), "gold_strategy_summary")
    for t in ("gold_fct_daily_prices", "gold_daily_movers"):
        store.optimize(t)
    return {"fact_rows": store.read("gold_fct_daily_prices").count()}


def quality_checks(store, max_quarantine_pct: float = 1.0) -> list[dict]:
    """Checks that fail the job when broken. Returns one dict per check."""
    s = store.read("silver_daily_prices")
    total = s.count()
    dupes = s.groupBy("symbol", "trade_date").count().filter("count > 1").count()
    q = store.read("silver_quarantine").count()
    fct = store.read("gold_fct_daily_prices")
    bad_ohlc = fct.filter("high < low OR close > high OR close < low").count()
    latest = s.agg(F.max("trade_date")).first()[0]
    missing_sector = (store.read("gold_sector_daily").filter("sector = 'UNKNOWN'")
                      .agg(F.coalesce(F.sum("stocks"), F.lit(0))).first()[0])
    checks = [
        {"check": "no duplicate stock-days in silver", "value": dupes, "passed": dupes == 0},
        {"check": f"quarantined rows under {max_quarantine_pct}%", "value": q,
         "passed": q <= total * max_quarantine_pct / 100},
        {"check": "adjusted OHLC is consistent", "value": bad_ohlc, "passed": bad_ohlc == 0},
        {"check": "silver has rows", "value": total, "passed": total > 0},
        {"check": "latest trading day loaded", "value": str(latest), "passed": latest is not None},
        {"check": "stock-days without a sector (info)", "value": int(missing_sector), "passed": True},
    ]
    return checks
