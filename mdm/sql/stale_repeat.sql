-- Bars that are carried forward: identical close AND volume for N+ consecutive
-- sessions. Real prints essentially never repeat both.
WITH p AS (
  SELECT p.ticker, p.trade_date, c.session_idx, p.close, p.volume,
         LAG(p.close)  OVER (PARTITION BY p.ticker ORDER BY c.session_idx) AS prev_close,
         LAG(p.volume) OVER (PARTITION BY p.ticker ORDER BY c.session_idx) AS prev_volume
  FROM {ns}.prices_primary p
  JOIN {ns}.trading_calendar c ON c.trade_date = p.trade_date
  WHERE p.trade_date <= DATE '{as_of}'
),
s AS (
  SELECT *, CASE WHEN close = prev_close AND volume = prev_volume THEN 1 ELSE 0 END AS same
  FROM p
),
islands AS (
  SELECT *, SUM(1 - same) OVER (PARTITION BY ticker ORDER BY session_idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM s
),
runs AS (
  SELECT ticker, grp, SUM(same) AS run_len, max(trade_date) AS end_date, max(close) AS close
  FROM islands GROUP BY ticker, grp
)
SELECT
  'stale_repeat' AS check_name,
  CASE WHEN run_len >= {repeat_days} + 1 THEN 'high' ELSE 'medium' END AS severity,
  ticker,
  end_date AS trade_date,
  CAST(NULL AS STRING) AS contract,
  CAST(run_len AS DOUBLE) AS value,
  CAST({repeat_days} AS DOUBLE) AS threshold,
  concat('close ', CAST(ROUND(close, 2) AS STRING), ' and volume unchanged for ',
         CAST(run_len AS STRING), ' sessions') AS detail
FROM runs
WHERE run_len >= {repeat_days}
  AND end_date >= DATE '{window_start}'
