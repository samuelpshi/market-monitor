-- Sessions with no bar, between the ticker's first bar and as_of. The latest
-- session is excluded here because stale_latest already reports it.
WITH cal AS (
  SELECT trade_date, session_idx FROM {ns}.trading_calendar
  WHERE trade_date >= DATE '{window_start}' AND trade_date < DATE '{as_of}'
),
first_bar AS (
  SELECT ticker, min(trade_date) AS first_date FROM {ns}.prices_primary GROUP BY ticker
),
expected AS (
  SELECT w.ticker, c.trade_date
  FROM {ns}.watchlist w
  JOIN first_bar f ON f.ticker = w.ticker
  JOIN cal c ON c.trade_date >= f.first_date
)
SELECT
  'missing_day' AS check_name,
  'medium' AS severity,
  e.ticker,
  e.trade_date,
  CAST(NULL AS STRING) AS contract,
  CAST(NULL AS DOUBLE) AS value,
  CAST(NULL AS DOUBLE) AS threshold,
  'no bar for an NYSE session' AS detail
FROM expected e
LEFT JOIN {ns}.prices_primary p ON p.ticker = e.ticker AND p.trade_date = e.trade_date
WHERE p.ticker IS NULL
