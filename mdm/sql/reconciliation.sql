-- Close-price breaks between primary and secondary vendor. Only compares dates
-- inside the secondary vendor's coverage window for that ticker, so a vendor
-- that simply wasn't queried today (rotation / quota) doesn't raise breaks.
WITH cov AS (
  SELECT ticker, min(trade_date) AS d0, max(trade_date) AS d1
  FROM {ns}.prices_secondary
  WHERE trade_date BETWEEN DATE '{window_start}' AND DATE '{as_of}'
  GROUP BY ticker
),
p AS (
  SELECT p.ticker, p.trade_date, p.close
  FROM {ns}.prices_primary p JOIN cov ON cov.ticker = p.ticker
  WHERE p.trade_date BETWEEN cov.d0 AND cov.d1
),
s AS (
  SELECT ticker, trade_date, close FROM {ns}.prices_secondary
  WHERE trade_date BETWEEN DATE '{window_start}' AND DATE '{as_of}'
),
j AS (
  SELECT coalesce(p.ticker, s.ticker) AS ticker,
         coalesce(p.trade_date, s.trade_date) AS trade_date,
         p.close AS p_close, s.close AS s_close
  FROM p FULL OUTER JOIN s ON p.ticker = s.ticker AND p.trade_date = s.trade_date
),
d AS (
  SELECT *,
         ABS(p_close - s_close) / s_close * 10000 AS diff_bps,
         p_close / s_close AS ratio
  FROM j
)
SELECT
  CASE WHEN p_close IS NULL THEN 'recon_missing_primary'
       WHEN s_close IS NULL THEN 'recon_missing_secondary'
       ELSE 'recon_break' END AS check_name,
  CASE WHEN p_close IS NULL OR s_close IS NULL THEN 'medium'
       WHEN (ABS(ratio - ROUND(ratio)) < 0.01 AND ROUND(ratio) >= 2)
         OR (ABS(1 / ratio - ROUND(1 / ratio)) < 0.01 AND ROUND(1 / ratio) >= 2) THEN 'low'
       WHEN diff_bps >= {break_bps} THEN 'high'
       ELSE 'medium' END AS severity,
  ticker,
  trade_date,
  CAST(NULL AS STRING) AS contract,
  ROUND(diff_bps, 1) AS value,
  CAST({warn_bps} AS DOUBLE) AS threshold,
  CASE WHEN p_close IS NULL THEN concat('secondary has ', CAST(ROUND(s_close, 2) AS STRING), ', primary has no bar')
       WHEN s_close IS NULL THEN concat('primary has ', CAST(ROUND(p_close, 2) AS STRING), ', secondary has no bar')
       WHEN (ABS(ratio - ROUND(ratio)) < 0.01 AND ROUND(ratio) >= 2)
         OR (ABS(1 / ratio - ROUND(1 / ratio)) < 0.01 AND ROUND(1 / ratio) >= 2)
         THEN concat('ratio ', CAST(ROUND(ratio, 3) AS STRING), ': likely split-adjusted vs as-traded')
       ELSE concat('primary ', CAST(ROUND(p_close, 2) AS STRING), ' vs secondary ', CAST(ROUND(s_close, 2) AS STRING))
  END AS detail
FROM d
WHERE p_close IS NULL OR s_close IS NULL OR diff_bps >= {warn_bps}
