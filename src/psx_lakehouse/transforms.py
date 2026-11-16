"""Pure PySpark transforms for the PSX lakehouse: bronze -> silver -> gold.

Every function takes and returns DataFrames, so the same code runs on
Databricks (Delta tables) and locally in tests (plain Spark).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

PRICE_COLS = ["open", "high", "low", "close"]

# PSX caps a day's move at 10% of the previous close (min Rs 1). A bigger jump
# is a corporate action (split, bonus shares), not trading.
LIMIT_PCT, LIMIT_RS, TOLERANCE = 0.10, 1.0, 0.005

MIN_PRICE, MIN_VOLUME = 5.0, 100_000  # "liquid" stock filter for movers


# ---------------------------------------------------------------- bronze
def to_bronze(raw: DataFrame, use_file_metadata: bool = True) -> DataFrame:
    """Raw file rows + lineage columns. Nothing is dropped in bronze.

    On Databricks the hidden `_metadata` column carries the source file;
    `use_file_metadata=False` is for in-memory test data.
    """
    src = F.col("_metadata.file_path") if use_file_metadata else F.lit("in-memory")
    return (raw.withColumn("_source_file", src)
               .withColumn("_ingested_at", F.current_timestamp()))


# ---------------------------------------------------------------- silver
def quality_flags(df: DataFrame) -> DataFrame:
    """Adds `_dq_issue`: null when the row is valid, else the first rule it breaks."""
    rules = [
        ("missing_key", F.col("symbol").isNull() | F.col("trade_date").isNull()),
        ("non_positive_price", F.least(*[F.col(c) for c in PRICE_COLS]) <= 0),
        ("negative_volume", F.col("volume") < 0),
        ("high_below_low", F.col("high") < F.col("low")),
        ("close_outside_range", (F.col("close") > F.col("high")) | (F.col("close") < F.col("low"))),
        ("open_outside_range", (F.col("open") > F.col("high")) | (F.col("open") < F.col("low"))),
    ]
    issue = F.lit(None).cast("string")
    for name, broken in reversed(rules):
        issue = F.when(F.coalesce(broken, F.lit(True)), F.lit(name)).otherwise(issue)
    return df.withColumn("_dq_issue", issue)


def to_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Typed, de-duplicated prices (latest ingest wins) + quarantined bad rows."""
    typed = bronze.select(
        F.upper(F.trim("symbol")).alias("symbol"),
        F.to_date("date").alias("trade_date"),
        *[F.col(c).cast("double").alias(c) for c in PRICE_COLS],
        F.col("volume").cast("long").alias("volume"),
        "_ingested_at",
    )
    w = Window.partitionBy("symbol", "trade_date").orderBy(F.col("_ingested_at").desc())
    latest = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    flagged = quality_flags(latest)
    clean = flagged.filter(F.col("_dq_issue").isNull()).drop("_dq_issue")
    quarantine = flagged.filter(F.col("_dq_issue").isNotNull())
    return clean, quarantine


def adjust_corporate_actions(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Back-adjust prices for splits and bonus shares.

    Each row is scaled by the product of the ratios of all LATER action days
    in that stock, computed as exp(sum(log(ratio))) over a forward window.
    """
    by_sym = Window.partitionBy("symbol").orderBy("trade_date")
    later = by_sym.rowsBetween(1, Window.unboundedFollowing)
    prev = F.lag("close").over(by_sym)
    limit = F.greatest(prev * (LIMIT_PCT + TOLERANCE), F.lit(LIMIT_RS + 0.01))
    df = (silver.withColumn("_prev", prev)
                .withColumn("_action", F.col("_prev").isNotNull() & (F.abs(F.col("close") - F.col("_prev")) > limit))
                .withColumn("_ratio", F.when(F.col("_action"), F.col("close") / F.col("_prev")).otherwise(F.lit(1.0)))
                .withColumn("adj_factor", F.coalesce(F.exp(F.sum(F.log("_ratio")).over(later)), F.lit(1.0))))
    events = (df.filter("_action")
                .select("symbol", "trade_date", F.col("_prev").alias("prev_close"), "close",
                        F.round("_ratio", 4).alias("ratio")))
    adjusted = df.select(
        "symbol", "trade_date",
        *[F.round(F.col(c) * F.col("adj_factor"), 4).alias(c) for c in PRICE_COLS],
        F.round(F.col("volume") / F.col("adj_factor")).cast("long").alias("volume"),
        F.col("close").alias("close_raw"),
        F.round("adj_factor", 6).alias("adj_factor"),
    )
    return adjusted, events


# ---------------------------------------------------------------- gold
def fct_daily_prices(adj: DataFrame) -> DataFrame:
    """One row per stock per day with returns, moving averages and 52-week range."""
    w = Window.partitionBy("symbol").orderBy("trade_date")
    def last(n):
        return w.rowsBetween(-(n - 1), 0)
    return (adj
            .withColumn("prev_close", F.lag("close").over(w))
            .withColumn("pct_change", F.round((F.col("close") / F.col("prev_close") - 1) * 100, 3))
            .withColumn("value_traded", F.round(F.col("close_raw") * F.col("volume"), 0))
            .withColumn("sma_20", F.round(F.avg("close").over(last(20)), 4))
            .withColumn("sma_50", F.round(F.avg("close").over(last(50)), 4))
            .withColumn("sma_200", F.round(F.avg("close").over(last(200)), 4))
            .withColumn("avg_volume_20", F.round(F.avg("volume").over(last(20)), 0))
            .withColumn("high_52w", F.max("high").over(last(248)))
            .withColumn("low_52w", F.min("low").over(last(248)))
            .withColumn("is_liquid", (F.col("close_raw") >= MIN_PRICE) & (F.col("volume") >= MIN_VOLUME)))


def mart_daily_movers(fct: DataFrame, top_n: int = 5) -> DataFrame:
    """Top gainers and losers each day among liquid stocks."""
    liquid = fct.filter("is_liquid AND pct_change IS NOT NULL")
    by_day = Window.partitionBy("trade_date")
    ranked = (liquid
              .withColumn("loser_rank", F.row_number().over(by_day.orderBy(F.col("pct_change").asc(), "symbol")))
              .withColumn("gainer_rank", F.row_number().over(by_day.orderBy(F.col("pct_change").desc(), "symbol"))))
    return (ranked.filter((F.col("loser_rank") <= top_n) | (F.col("gainer_rank") <= top_n))
                  .select("trade_date", "symbol", "close", "pct_change", "volume", "value_traded",
                          "loser_rank", "gainer_rank"))


def mart_sector_daily(fct: DataFrame, stocks: DataFrame) -> DataFrame:
    """Sector advancers, decliners, average move and value traded per day."""
    s = stocks.select(F.upper(F.trim("symbol")).alias("symbol"),
                      F.coalesce(F.col("sector"), F.lit("UNKNOWN")).alias("sector"))
    return (fct.filter("pct_change IS NOT NULL").join(s, "symbol", "left")
               .fillna({"sector": "UNKNOWN"})
               .groupBy("trade_date", "sector")
               .agg(F.count("*").alias("stocks"),
                    F.sum(F.when(F.col("pct_change") > 0, 1).otherwise(0)).alias("advancers"),
                    F.sum(F.when(F.col("pct_change") < 0, 1).otherwise(0)).alias("decliners"),
                    F.round(F.avg("pct_change"), 3).alias("avg_pct_change"),
                    F.sum("value_traded").alias("value_traded")))


def mart_market_breadth(fct: DataFrame) -> DataFrame:
    """Whole-market breadth per day."""
    return (fct.filter("pct_change IS NOT NULL").groupBy("trade_date")
               .agg(F.count("*").alias("stocks_traded"),
                    F.sum(F.when(F.col("pct_change") > 0, 1).otherwise(0)).alias("advancers"),
                    F.sum(F.when(F.col("pct_change") < 0, 1).otherwise(0)).alias("decliners"),
                    F.sum(F.when(F.col("close") >= F.col("high_52w"), 1).otherwise(0)).alias("new_52w_highs"),
                    F.sum(F.when(F.col("close") <= F.col("low_52w"), 1).otherwise(0)).alias("new_52w_lows"),
                    F.sum("value_traded").alias("value_traded"))
               .withColumn("breadth_ratio", F.round(F.col("advancers") / F.greatest(F.col("decliners"), F.lit(1)), 3)))


def mart_momentum_monthly(fct: DataFrame, top_n: int = 10, lookback: int = 12,
                          min_value_traded: float = 1_000_000.0) -> DataFrame:
    """Monthly momentum research: each month buy the top-N stocks by 12-month return.

    Returns one row per month with the momentum basket's next-month return and
    the equal-weighted market's next-month return (fees and tax not included).
    """
    month = F.date_trunc("month", "trade_date").cast("date")
    last_day = Window.partitionBy("symbol", "month").orderBy(F.col("trade_date").desc())
    monthly = (fct.withColumn("month", month)
                  .withColumn("_avg_val", F.avg("value_traded").over(Window.partitionBy("symbol", "month")))
                  .withColumn("_rn", F.row_number().over(last_day))
                  .filter("_rn = 1")
                  .select("symbol", "month", "close", F.col("_avg_val").alias("avg_value_traded")))
    w = Window.partitionBy("symbol").orderBy("month")
    m = (monthly
         .withColumn("ret_lookback", F.col("close") / F.lag("close", lookback).over(w) - 1)
         .withColumn("ret_next", F.lead("close").over(w) / F.col("close") - 1)
         .withColumn("_gap", F.months_between(F.lead("month").over(w), "month"))
         .filter((F.col("_gap") == 1) & F.col("ret_next").isNotNull()))
    eligible = m.filter(F.col("ret_lookback").isNotNull() & (F.col("avg_value_traded") >= min_value_traded))
    ranked = eligible.withColumn("mom_rank", F.row_number().over(
        Window.partitionBy("month").orderBy(F.col("ret_lookback").desc(), "symbol")))
    basket = (ranked.filter(F.col("mom_rank") <= top_n).groupBy("month")
                    .agg(F.avg("ret_next").alias("momentum_ret"),
                         F.concat_ws(",", F.sort_array(F.collect_list("symbol"))).alias("momentum_basket")))
    market = eligible.groupBy("month").agg(F.avg("ret_next").alias("market_ret"),
                                           F.count("*").alias("eligible_stocks"))
    out = basket.join(market, "month").orderBy("month")
    cum = Window.orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
    return (out.withColumn("momentum_growth", F.round(F.exp(F.sum(F.log1p("momentum_ret")).over(cum)), 4))
               .withColumn("market_growth", F.round(F.exp(F.sum(F.log1p("market_ret")).over(cum)), 4))
               .withColumn("momentum_ret", F.round("momentum_ret", 5))
               .withColumn("market_ret", F.round("market_ret", 5)))


def strategy_summary(momentum: DataFrame) -> DataFrame:
    """Annualised return and worst drawdown for momentum vs market."""
    rows = []
    for name, col in (("Momentum top 10 (12-month)", "momentum_growth"),
                      ("Market, equal weight", "market_growth")):
        peak = Window.orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
        d = momentum.withColumn("_dd", F.col(col) / F.max(col).over(peak) - 1)
        rows.append(d.agg(
            F.lit(name).alias("strategy"),
            F.count("*").alias("months"),
            F.round((F.pow(F.max_by(col, "month"), 12 / F.count("*")) - 1) * 100, 2).alias("annual_return_pct"),
            F.round(F.min("_dd") * 100, 2).alias("worst_drawdown_pct"),
        ))
    out = rows[0]
    for r in rows[1:]:
        out = out.unionByName(r)
    return out
