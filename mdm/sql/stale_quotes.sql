-- Option quotes unchanged since the previous snapshot even though the
-- underlying moved: the quote feed stopped updating. Unchanged quotes on an
-- unchanged spot are normal for illiquid strikes, so they are not flagged.
WITH prev_snap AS (
  SELECT max(snapshot_date) AS d FROM {ns}.options_quotes WHERE snapshot_date < DATE '{as_of}'
),
cur AS (
  SELECT * FROM {ns}.options_quotes
  WHERE snapshot_date = DATE '{as_of}' AND bid > 0 AND ask > 0
    AND strike / spot BETWEEN {m_lo} AND {m_hi}
),
prv AS (
  SELECT o.* FROM {ns}.options_quotes o JOIN prev_snap ps ON o.snapshot_date = ps.d
),
j AS (
  SELECT c.ticker, c.snapshot_date, c.spot, p.spot AS prev_spot,
         CASE WHEN c.bid = p.bid AND c.ask = p.ask THEN 1 ELSE 0 END AS same
  FROM cur c
  JOIN prv p ON p.ticker = c.ticker AND p.expiry = c.expiry
            AND p.strike = c.strike AND p.option_type = c.option_type
),
agg AS (
  SELECT ticker, max(snapshot_date) AS snapshot_date,
         SUM(same) AS n_same, COUNT(*) AS n_total,
         ABS(max(spot) / max(prev_spot) - 1) AS spot_move
  FROM j GROUP BY ticker
)
SELECT
  'stale_quote' AS check_name,
  CASE WHEN n_same * 1.0 / n_total >= 0.5 THEN 'high' ELSE 'medium' END AS severity,
  ticker,
  snapshot_date AS trade_date,
  CAST(NULL AS STRING) AS contract,
  CAST(n_same AS DOUBLE) AS value,
  CAST(n_total AS DOUBLE) AS threshold,
  concat(CAST(n_same AS STRING), ' of ', CAST(n_total AS STRING),
         ' near-money quotes unchanged while spot moved ',
         CAST(ROUND(spot_move * 100, 2) AS STRING), '%') AS detail
FROM agg
WHERE spot_move >= {min_spot_move} AND n_same * 1.0 / n_total >= {min_share}
