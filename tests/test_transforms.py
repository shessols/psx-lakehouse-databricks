from datetime import date, datetime, timedelta

from pyspark.sql import functions as F

from psx_lakehouse import transforms as T
from psx_lakehouse.ingest import months_between
from psx_lakehouse.pipeline import LocalStore, gold, quality_checks, silver


def raw_rows(spark, rows):
    cols = "symbol string, date timestamp, open double, high double, low double, close double, volume long"
    return spark.createDataFrame(rows, cols)


def d(i):
    return datetime(2026, 1, 1) + timedelta(days=i)


def test_months_between():
    assert months_between("2016-11-15", "2017-02-01") == ["2016-11", "2016-12", "2017-01", "2017-02"]


def test_silver_dedupes_latest_ingest_and_quarantines(spark):
    old = T.to_bronze(raw_rows(spark, [("abc", d(0), 10.0, 11.0, 9.0, 10.0, 100)]), use_file_metadata=False)
    old = old.withColumn("_ingested_at", F.lit(datetime(2026, 1, 1)))
    new = T.to_bronze(raw_rows(spark, [
        ("ABC", d(0), 10.0, 11.0, 9.0, 10.5, 200),   # restated row: must win
        ("ABC", d(1), 10.0, 9.0, 11.0, 10.0, 100),   # high < low
        ("XYZ", d(1), 0.0, 1.0, 0.0, 1.0, 100),      # zero price
    ]), use_file_metadata=False)
    clean, quarantine = T.to_silver(old.unionByName(new))
    c = clean.collect()
    assert len(c) == 1 and c[0]["symbol"] == "ABC" and c[0]["close"] == 10.5 and c[0]["volume"] == 200
    assert sorted(r["_dq_issue"] for r in quarantine.collect()) == ["high_below_low", "non_positive_price"]


def test_split_is_back_adjusted(spark):
    # 2-for-1 split on day 2: price 100 -> 50. Earlier prices should halve.
    rows = [("S", date(2026, 1, 1 + i), c, c, c, c, 1000) for i, c in enumerate([100.0, 102.0, 51.0, 52.0])]
    df = spark.createDataFrame(rows, "symbol string, trade_date date, open double, high double, low double, close double, volume long")
    adj, events = T.adjust_corporate_actions(df)
    closes = [r["close"] for r in adj.orderBy("trade_date").collect()]
    assert closes == [50.0, 51.0, 51.0, 52.0]
    assert adj.orderBy("trade_date").first()["volume"] == 2000
    ev = events.collect()
    assert len(ev) == 1 and ev[0]["ratio"] == 0.5


def test_normal_moves_are_not_adjusted(spark):
    rows = [("S", date(2026, 1, 1 + i), c, c, c, c, 1000) for i, c in enumerate([100.0, 109.0, 99.0])]
    df = spark.createDataFrame(rows, "symbol string, trade_date date, open double, high double, low double, close double, volume long")
    adj, events = T.adjust_corporate_actions(df)
    assert events.count() == 0
    assert [r["close"] for r in adj.orderBy("trade_date").collect()] == [100.0, 109.0, 99.0]


def test_movers_rank_only_liquid_stocks(spark):
    rows = []
    for sym, closes, vol in [("UP", [10, 11], 200_000), ("DOWN", [10, 9.2], 200_000),
                             ("THIN", [10, 5], 10), ("PENNY", [2, 1.5], 900_000)]:
        for i, c in enumerate(closes):
            rows.append((sym, date(2026, 1, 1 + i), float(c), float(c), float(c), float(c), vol, float(c), 1.0))
    adj = spark.createDataFrame(rows, "symbol string, trade_date date, open double, high double, low double, close double, volume long, close_raw double, adj_factor double")
    movers = T.mart_daily_movers(T.fct_daily_prices(adj)).collect()
    assert {r["symbol"] for r in movers} == {"UP", "DOWN"}
    loser = [r for r in movers if r["loser_rank"] == 1][0]
    assert loser["symbol"] == "DOWN" and loser["pct_change"] == -8.0


def test_full_pipeline_on_local_store(spark, tmp_path):
    vol = tmp_path / "volume"
    (vol / "stocks").mkdir(parents=True)
    rows = []
    for s_i, sym in enumerate(["AAA", "BBB", "CCC"]):
        price = 50.0 + s_i
        for i in range(400):
            price *= 1.0 + (0.004 if sym == "AAA" else -0.001 if sym == "BBB" else 0.001)
            rows.append((sym, d(i), price, price * 1.01, price * 0.99, price, 500_000))
    raw_rows(spark, rows).write.parquet(str(vol / "prices"))
    (vol / "stocks" / "stocks.csv").write_text("symbol,name,sector\nAAA,A Co,Cement\nBBB,B Co,Banks\nCCC,C Co,Banks\n")
    store = LocalStore(spark, str(tmp_path / "tables"))
    from psx_lakehouse.pipeline import bronze_batch
    assert bronze_batch(spark, store, str(vol)) == 1200
    assert silver(store)["quarantined"] == 0
    assert gold(store)["fact_rows"] == 1200
    assert all(c["passed"] for c in quality_checks(store))
    mom = store.read("gold_momentum_monthly")
    assert mom.count() > 0
    assert store.read("gold_strategy_summary").count() == 2
    # rerun: silver upsert must stay duplicate-free
    bronze_batch(spark, store, str(vol))
    silver(store)
    assert store.read("silver_daily_prices").count() == 1200
