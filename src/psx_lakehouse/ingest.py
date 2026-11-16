"""Land the PSX dataset (monthly Parquet files + stock list) in a Unity Catalog volume.

Incremental: on later runs only the last `refresh_months` monthly files are
downloaded again, because the current month's file grows every trading day.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import date

from .config import SOURCE_BASE


def _get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "psx-lakehouse"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def months_between(first: str, last: str) -> list[str]:
    """['2016-11', ..., '2026-11'] from ISO dates."""
    y, m = int(first[:4]), int(first[5:7])
    ly, lm = int(last[:4]), int(last[5:7])
    out = []
    while (y, m) <= (ly, lm):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def land(volume: str, base: str = SOURCE_BASE, refresh_months: int = 2) -> dict:
    """Download into <volume>/prices/YYYY-MM.parquet and <volume>/stocks/stocks.csv."""
    meta = json.loads(_get(f"{base}/metadata.json"))
    months = months_between(meta["first_date"], meta["last_date"])
    prices_dir, stocks_dir = f"{volume}/prices", f"{volume}/stocks"
    os.makedirs(prices_dir, exist_ok=True)
    os.makedirs(stocks_dir, exist_ok=True)
    have = {f[:7] for f in os.listdir(prices_dir) if f.endswith(".parquet")}
    todo = [m for m in months if m not in have] + [m for m in months[-refresh_months:] if m in have]
    for m in todo:
        data = _get(f"{base}/prices/{m}.parquet")
        with open(f"{prices_dir}/{m}.parquet", "wb") as f:
            f.write(data)
    with open(f"{stocks_dir}/stocks.csv", "wb") as f:
        f.write(_get(f"{base}/stocks.csv"))
    with open(f"{volume}/metadata.json", "w") as f:
        json.dump({**meta, "landed_on": date.today().isoformat(), "files_downloaded": len(todo)}, f)
    return {"months_downloaded": len(todo), "last_date": meta["last_date"], "rows_at_source": meta["rows"]}
