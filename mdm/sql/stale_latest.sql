-- Watchlist tickers whose latest bar is older than the latest completed session.
WITH cal AS (
  SELECT trade_date, session_idx FROM {ns}.trading_calendar WHERE trade_date <= DATE '{as_of}'
),
cur AS (SELECT max(session_idx) AS idx, max(trade_date) AS d FROM cal),
latest AS (
  SELECT w.ticker, max(c.session_idx) AS last_idx, max(p.trade_date) AS last_date
  FROM {ns}.watchlist w
  LEFT JOIN {ns}.prices_primary p ON p.ticker = w.ticker AND p.trade_date <= DATE '{as_of}'
  LEFT JOIN cal c ON c.trade_date = p.trade_date
  GROUP BY w.ticker
)
SELECT
  'stale_latest' AS check_name,
  'high' AS severity,
  l.ticker,
  a.d AS trade_date,
  CAST(NULL AS STRING) AS contract,
  CAST(a.idx - coalesce(l.last_idx, -1) AS DOUBLE) AS value,
  0.0 AS threshold,
  concat('latest bar ', coalesce(CAST(l.last_date AS STRING), 'never'),
         ', expected ', CAST(a.d AS STRING)) AS detail
FROM latest l CROSS JOIN cur a
WHERE l.last_idx IS NULL OR l.last_idx < a.idx
