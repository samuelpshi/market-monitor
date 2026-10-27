-- Research reads prices_clean, never prices_primary. A bar is suspect if a
-- quality check flagged it (including every session of a carried-forward run)
-- or the second vendor disagrees by more than break_bps.
CREATE OR REPLACE VIEW {ns}.price_suspects AS
WITH latest AS (
  SELECT f.* FROM {ns}.flags f
  JOIN (SELECT max(as_of) AS a FROM {ns}.flags) m ON f.as_of = m.a
),
direct AS (
  SELECT ticker, trade_date, check_name FROM latest
  WHERE check_name = 'bad_ohlc'
     OR (check_name = 'recon_break' AND severity = 'high')
),
runs AS (
  SELECT l.ticker, c2.trade_date, l.check_name
  FROM latest l
  JOIN {ns}.trading_calendar c1 ON c1.trade_date = l.trade_date
  JOIN {ns}.trading_calendar c2
    ON c2.session_idx BETWEEN c1.session_idx - CAST(l.value AS INT) + 1 AND c1.session_idx
  WHERE l.check_name = 'stale_repeat'
),
u AS (SELECT * FROM direct UNION ALL SELECT * FROM runs)
SELECT ticker, trade_date, min(check_name) AS reason, COUNT(DISTINCT check_name) AS n_reasons
FROM u GROUP BY ticker, trade_date;

CREATE OR REPLACE VIEW {ns}.prices_clean AS
SELECT p.*
FROM {ns}.prices_primary p
LEFT JOIN {ns}.price_suspects s ON s.ticker = p.ticker AND s.trade_date = p.trade_date
WHERE s.ticker IS NULL;
