# Market Data Monitor

Catches bad market data and unusual price moves before they reach research.
Daily prices and options chains for a 50-ticker watchlist land in Databricks
(or DuckDB locally) on a scheduled Job. SQL checks flag stale prices and stale
quotes, missing sessions, malformed bars, crossed quotes, put-call parity breaks
and breaks between two data vendors. Rolling-volatility / z-score signals flag
unusual moves. Claude drafts a daily digest for Slack, and code verifies every
figure in it against the flags table before it posts. A Streamlit dashboard
shows all of it.

```
Yahoo (prices, options) ─┐
Alpha Vantage (rotated) ─┼─> ingest ─> quality SQL ──┐
                         │            reconcile SQL ─┼─> flags ─> prices_clean ─> anomalies ─> digest ─> Slack
                         │                           │                                         │
                         └───────────────────────────┴──────────── Streamlit dashboard <───────┘
```

## Quick start (local, no API keys)

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m mdm.pipeline --demo --as-of 2026-10-23 --no-post   # synthetic market with planted faults
.venv/bin/python -m mdm.harness --batches 12                           # measure recall / false positives
.venv/bin/python -m pytest -q tests
.venv/bin/streamlit run app/streamlit_app.py
```

Live data: `python -m mdm.pipeline` (defaults to the latest completed NYSE session).
Set `ALPHA_VANTAGE_API_KEY`, `ANTHROPIC_API_KEY` and `SLACK_WEBHOOK_URL` as env vars,
or in Databricks secret scope `market-monitor`.

## Databricks

`databricks.yml` is an Asset Bundle: a five-task serverless Job (ingest → quality and
reconcile in parallel → anomalies → digest), running weekdays at 18:30 New York time.

```bash
databricks secrets create-scope market-monitor
databricks secrets put-secret market-monitor alpha_vantage_api_key   # also anthropic_api_key, slack_webhook_url
databricks bundle deploy -t dev && databricks bundle run market_data_monitor -t dev
```

Tables go to `main.market_monitor` (set `storage.spark_namespace`). The same SQL files run
on both engines. The tests parse them as Spark SQL, but nothing has been run on a cluster yet.

## Checks

| Check | Stage | What it flags |
|---|---|---|
| `stale_latest` | quality | latest bar older than the latest completed session |
| `stale_repeat` | quality | identical close **and** volume for ≥2 consecutive sessions (carried-forward bars) |
| `missing_day` | quality | NYSE session with no bar (exchange_calendars, so holidays aren't false hits) |
| `bad_ohlc` | quality | close/open outside high–low, high < low, non-positive price |
| `crossed_quote` / `locked_quote` / `one_sided_quote` | quality | option bid > ask / bid = ask / bid with no ask |
| `stale_quote` | quality | option chain unchanged since the prior snapshot while spot moved ≥1% |
| `put_call_parity` | quality | C − P outside the **American** bounds S·e^(−qT) − K ≤ C − P ≤ S − K·e^(−rT), beyond half-spreads + buffers |
| `recon_break` | reconciliation | primary vs secondary close ≥10 bps (≥50 bps = high); ratios of 2, 3, ½… tagged as split-adjustment mismatches |
| `return_zscore` / `vol_regime` | anomaly | \|z\| ≥ 3 vs the prior 20-day vol; 20d / 250d vol ≥ 2 |

Research reads `prices_clean`, which excludes bars flagged `bad_ohlc`, every session of a
`stale_repeat` run, and high-severity vendor breaks. Anomalies are computed on
`prices_clean`, so a bad bar can't register as a big move.

## LLM digest

`mdm/digest.py` builds a facts JSON from the flags, with every figure pre-formatted.
Claude (`claude-opus-5-5`, low effort, server-side refusal fallback enabled) writes the
Slack message. `verify()` then checks it line by line:
- every number matches a figure for a ticker named on that line
- dates are ISO and appear in the data
- "up"/"down" agrees with the sign of the return
- every high-severity item is mentioned
- no ticker without flags is mentioned

On failure the errors go back to Claude for one rewrite. If that also fails, a
deterministic template digest is posted instead, so an unverified number never reaches
Slack. Each run is stored in `digests` with its verification result. A digest already
posted for a session is not reposted.

## Measured detection (synthetic, `mdm.harness`)

510 faults planted across 12 batches, at most one per ticker per batch. Fault sizes were
drawn on both sides of each threshold, and there were 6 clean runs with no plants.

- **Recall on faults at or above each check's threshold: 100% (385/385).**
- Recall on all planted faults, including deliberately sub-threshold ones: 76%.
- False flags on clean data: 4.5 per run (50 tickers). 24 of the 27 were `recon_break`,
  caused by simulated vendor noise crossing the 10 bps threshold. The other 3 were
  genuine 3σ moves in the simulated returns.

These numbers come from a synthetic market. Real vendor disagreement and quote quality
will produce different false-positive rates. Measure on live data before quoting a
precision figure.

## Known limitations

- **yfinance** scrapes Yahoo's unofficial API. It breaks without notice, throttles, and
  Yahoo's terms restrict this use. Options quotes are delayed and not synchronized with the
  spot snapshot. That's why parity has a spot-timing buffer, and why deep in-the-money
  contracts still produce flags that are hard to adjudicate.
- **Alpha Vantage free tier** allows about 25 calls a day. With 50 tickers the second vendor
  is rotated 24 per day, so a vendor break can take up to 3 sessions to surface.
  `TIME_SERIES_DAILY` is as-traded while Yahoo's `Close` is split-adjusted, so pre-split
  history disagrees (tagged low severity).
- **Databricks serverless egress**: outbound calls to Yahoo and Alpha Vantage may be blocked
  by workspace network policy (Free Edition in particular). If so, run ingest outside
  Databricks and land files in a Volume.
- Dividend yield is trailing-12-month cash dividends / spot. The risk-free rate is ^IRX.
  Both are approximations that are good enough for bounds checks, not for pricing.
- The live Claude call and Slack posting are covered by tests with a fake client only.
