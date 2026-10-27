-- Option quotes with bid above ask (crossed), bid equal to ask (locked), or a
-- bid with no ask (one-sided; common in after-hours Yahoo snapshots).
SELECT
  CASE WHEN bid > ask AND ask > 0 THEN 'crossed_quote'
       WHEN bid = ask AND bid > 0 THEN 'locked_quote'
       ELSE 'one_sided_quote' END AS check_name,
  CASE WHEN bid > ask AND ask > 0 THEN 'high'
       WHEN bid = ask AND bid > 0 THEN 'low'
       ELSE 'low' END AS severity,
  ticker,
  snapshot_date AS trade_date,
  concat(ticker, ' ', CAST(expiry AS STRING), ' ', option_type, ' K=', CAST(strike AS STRING)) AS contract,
  bid - ask AS value,
  0.0 AS threshold,
  concat('bid ', CAST(ROUND(bid, 2) AS STRING), ' / ask ', CAST(ROUND(ask, 2) AS STRING)) AS detail
FROM {ns}.options_quotes
WHERE snapshot_date = DATE '{as_of}'
  AND ((bid > ask AND ask > 0) OR (bid = ask AND bid > 0) OR (bid > 0 AND (ask IS NULL OR ask = 0)))
