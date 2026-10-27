-- American-option parity bounds (no-arbitrage), with dividends as a continuous
-- yield q:    S*e^(-qT) - K  <=  C - P  <=  S - K*e^(-rT)
-- The European equality does not hold for US single-stock options, so checking
-- it would flag every early-exercise premium as an "error".
-- Tolerance = both legs' half-spreads + abs buffer + spot-timing buffer.
WITH q AS (
  SELECT * FROM {ns}.options_quotes
  WHERE snapshot_date = DATE '{as_of}'
    AND bid > 0 AND ask > 0 AND ask >= bid
    AND t_years >= {min_t}
    AND strike / spot BETWEEN {m_lo} AND {m_hi}
    AND open_interest >= {min_oi}
),
pairs AS (
  SELECT c.ticker, c.snapshot_date, c.expiry, c.strike, c.spot, c.rate, c.div_yield, c.t_years,
         (c.bid + c.ask) / 2 - (p.bid + p.ask) / 2 AS cp,
         (c.ask - c.bid) / 2 + (p.ask - p.bid) / 2 AS half_spreads
  FROM q c
  JOIN q p ON p.ticker = c.ticker AND p.expiry = c.expiry AND p.strike = c.strike
  WHERE c.option_type = 'C' AND p.option_type = 'P'
),
b AS (
  SELECT *,
         spot * EXP(-div_yield * t_years) - strike AS lower_b,
         spot - strike * EXP(-rate * t_years) AS upper_b,
         half_spreads + {abs_tol} + {spot_tol} * spot AS tol
  FROM pairs
),
v AS (SELECT *, GREATEST(lower_b - cp, cp - upper_b) AS viol FROM b)
SELECT
  'put_call_parity' AS check_name,
  CASE WHEN viol > 3 * tol THEN 'high' ELSE 'medium' END AS severity,
  ticker,
  snapshot_date AS trade_date,
  concat(ticker, ' ', CAST(expiry AS STRING), ' K=', CAST(strike AS STRING)) AS contract,
  ROUND(viol, 4) AS value,
  ROUND(tol, 4) AS threshold,
  concat('C-P=', CAST(ROUND(cp, 2) AS STRING),
         ' outside [', CAST(ROUND(lower_b, 2) AS STRING), ', ', CAST(ROUND(upper_b, 2) AS STRING), ']') AS detail
FROM v
WHERE viol > tol
