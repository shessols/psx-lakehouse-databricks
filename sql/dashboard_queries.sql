-- Queries for a Databricks SQL / AI/BI dashboard. Replace workspace.psx if you changed catalog or schema.

-- 1. Market breadth, last 90 trading days
SELECT trade_date, advancers, decliners, breadth_ratio, new_52w_highs, new_52w_lows
FROM workspace.psx.gold_market_breadth
WHERE trade_date >= date_sub(current_date(), 130)
ORDER BY trade_date;

-- 2. Today's top 5 losers and gainers (liquid stocks only)
SELECT CASE WHEN loser_rank <= 5 THEN 'Loser' ELSE 'Gainer' END AS side,
       least(loser_rank, gainer_rank) AS rank, symbol, close, pct_change, value_traded
FROM workspace.psx.gold_daily_movers
WHERE trade_date = (SELECT max(trade_date) FROM workspace.psx.gold_daily_movers)
ORDER BY side, rank;

-- 3. Sector heatmap: average daily move per sector per month
SELECT date_trunc('month', trade_date) AS month, sector, round(avg(avg_pct_change), 3) AS avg_daily_move
FROM workspace.psx.gold_sector_daily
WHERE trade_date >= add_months(current_date(), -12)
GROUP BY ALL
ORDER BY month, sector;

-- 4. Momentum vs market: growth of Rs 1
SELECT month, momentum_growth, market_growth
FROM workspace.psx.gold_momentum_monthly
ORDER BY month;

-- 5. Data quality history
SELECT run_at, check, value, passed
FROM workspace.psx.quality_log
ORDER BY run_at DESC, check;

-- 6. Delta time travel: how the fact table looked at an earlier version
DESCRIBE HISTORY workspace.psx.gold_fct_daily_prices;
-- SELECT count(*) FROM workspace.psx.gold_fct_daily_prices VERSION AS OF 0;
