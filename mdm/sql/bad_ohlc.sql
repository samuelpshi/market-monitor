-- Internally inconsistent bars: the price-data version of a miscoded charge.
SELECT
  'bad_ohlc' AS check_name,
  'high' AS severity,
  ticker,
  trade_date,
  CAST(NULL AS STRING) AS contract,
  close AS value,
  CAST(NULL AS DOUBLE) AS threshold,
  CASE
    WHEN close <= 0 OR open <= 0 THEN 'non-positive price'
    WHEN high < low THEN 'high below low'
    WHEN close > high OR close < low THEN 'close outside high/low range'
    WHEN open > high OR open < low THEN 'open outside high/low range'
    ELSE 'negative volume'
  END AS detail
FROM {ns}.prices_primary
WHERE trade_date BETWEEN DATE '{window_start}' AND DATE '{as_of}'
  AND (close <= 0 OR open <= 0 OR high < low
       OR close > high * 1.0001 OR close < low * 0.9999
       OR open > high * 1.0001 OR open < low * 0.9999
       OR volume < 0)
