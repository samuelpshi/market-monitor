# Market Data Monitor

A daily pipeline that catches bad market data and unusual price moves before they reach research.

It pulls prices for a 50-ticker watchlist and options chains for 10 of them, then flags:
- stale prices and stale quotes
- missing sessions and malformed bars
- crossed quotes
- put-call parity breaks
- disagreements between two data vendors
- abnormal returns

Flagged bars are kept out of the dataset research reads. Claude writes a daily Slack digest, and code checks every figure in it against the data before it posts.

**Stack:** Python · SQL (Spark SQL / DuckDB) · Databricks Jobs · Claude API · Slack · Streamlit

```
 Yahoo Finance ──┐                 ┌─ quality checks (SQL) ─┐
 (prices, opts)  ├─> ingest ──────>┤                        ├─> flags ─> prices_clean ─> anomalies ─> digest ─> Slack
 Alpha Vantage ──┘                 └─ vendor reconcile (SQL)┘                                │
 (2nd vendor)                                                                                │
                                         Streamlit dashboard <───────────────────────────────┘
```

## Results

**Detection, measured with a fault-injection harness** (`mdm/harness.py`)

The harness plants random faults into a synthetic 50-ticker market and runs the real checks against them. Fault sizes are drawn on both sides of each check's threshold, so it measures both what the checks catch and what they miss.

| | |
|---|---|
| Faults planted | 510, across 8 fault types |
| Recall, faults at or above each check's threshold | **100% (385 / 385)** |
| Recall, all faults, including deliberately sub-threshold ones | 76% |
| False flags on clean data | 4.5 per 50-ticker run, mostly vendor noise crossing the 10 bps threshold |

**Live run on real data (2026-10-26)**
- Ingested 13,800 daily bars for all 50 tickers and 8,482 option quotes.
- Reconciled 24 tickers against the second vendor.
- Raised 6 flags. One was a SPY Dec $658 call quoted above the $650 call. A higher-strike call can never be worth more, so that quote is provably bad.
- Yahoo and Alpha Vantage closes never differed by more than 0.5 bps across 2,400 overlapping bars. Real vendor noise is far below the simulated noise, but the two vendors probably share an upstream feed (see [Limitations](#limitations)).

## Checks

All checks are SQL in [`mdm/sql/`](mdm/sql), written in the subset that runs on both Spark SQL (Databricks) and DuckDB (local).

| Check | Flags |
|---|---|
| `stale_latest` | Latest bar is older than the latest completed NYSE session |
| `stale_repeat` | Close **and** volume unchanged for 2+ sessions (a carried-forward bar) |
| `missing_day` | An NYSE session with no bar. Holidays come from `exchange_calendars`, so they aren't false hits |
| `bad_ohlc` | Close or open outside the high–low range, high below low, or a non-positive price |
| `crossed_quote` | Option bid above ask. Locked and one-sided quotes are flagged at low severity |
| `stale_quote` | An option chain unchanged since the last snapshot while spot moved at least 1% |
| `put_call_parity` | C − P outside the **American** no-arbitrage bounds `S·e^(−qT) − K ≤ C − P ≤ S − K·e^(−rT)`, beyond both legs' half-spreads plus a timing buffer |
| `recon_break` | Primary and secondary vendor close differ by ≥10 bps (≥50 bps is high severity). Ratios near 2, 3 or ½ are tagged as likely split-adjustment mismatches |
| `return_zscore` | Today's return is ≥3σ against the *prior* 20-day volatility |
| `vol_regime` | 20-day volatility is at least 2× the 250-day volatility |

## Design decisions

- **Flags gate the data, they don't just report on it.** Research reads `prices_clean`, a view that excludes bad bars, every session of a stale run, and high-severity vendor breaks. Anomaly signals are also computed from `prices_clean`, so a bad print can't show up as a big move.
- **American bounds, not the European equality.** US single-stock options are American, so checking `C − P = S − K·e^(−rT)` would flag every early-exercise premium as an error.
- **Root causes over symptoms.** A stale chain breaks parity on every strike. When a chain is flagged stale, its parity flags are demoted so the digest reports one problem, not 20.
- **Expiries are chosen by days to expiry.** SPY and QQQ list daily expiries, so "the first 4 expiries" are all under a week out. Parity skips those, and on the first live run that meant 0 contracts were tested. The pipeline now picks the listed expiry nearest to 14, 30, 60 and 90 days.
- **The LLM never posts an unchecked number.** `verify()` checks each draft line by line:
  - every figure matches the data for the one ticker named on that line
  - every contract reference matches a flagged contract exactly
  - dates are ISO and appear in the data
  - "up" or "down" agrees with the sign of the return
  - every high-severity item is covered
  - there are no self-corrections

  A draft that fails gets one rewrite with the errors fed back. If it fails again, a deterministic template digest is posted instead.
- **Reruns are safe.** Every write deletes its slice and then inserts. A digest already posted for a session is never reposted.
- **The second vendor is rotated.** Alpha Vantage's free tier allows about 25 calls a day, so 24 tickers are reconciled each session and every ticker is covered at least once every 3 sessions.

## Run it

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt

# Synthetic market with planted faults, no keys needed
.venv/bin/python -m mdm.pipeline --demo --as-of 2026-10-23 --no-post

# Measure detection recall and false positives
.venv/bin/python -m mdm.harness --batches 12

.venv/bin/python -m pytest -q tests
.venv/bin/streamlit run app/streamlit_app.py
```

For live data, copy `.env.example` to `.env` and fill in `ALPHA_VANTAGE_API_KEY`, `ANTHROPIC_API_KEY` and `SLACK_WEBHOOK_URL`. Then run:

```bash
.venv/bin/python -m mdm.pipeline              # latest completed NYSE session
.venv/bin/python -m mdm.pipeline --no-post    # draft the digest without posting
```

### Databricks

[`databricks.yml`](databricks.yml) is an Asset Bundle that defines a serverless Job with five tasks: ingest, then quality and reconcile in parallel, then anomalies, then digest. The `prod` target runs weekdays at 18:30 New York time. The `dev` target keeps the schedule paused and doesn't post to Slack. The Job writes Delta tables to `workspace.market_monitor` (Free Edition's catalog; override with `var.namespace`), and keys come from the secret scope `market-monitor`. It has run end-to-end on Databricks Free Edition serverless.

```bash
databricks secrets create-scope market-monitor
databricks secrets put-secret market-monitor alpha_vantage_api_key   # also anthropic_api_key, slack_webhook_url
databricks bundle deploy -t dev
databricks bundle run market_data_monitor -t dev   # test run
databricks bundle deploy -t prod                    # turn on the daily schedule
```

## Project layout

```
mdm/
  sources.py     Yahoo, Alpha Vantage, and a synthetic market with planted faults
  ingest.py      idempotent loads, vendor rotation
  sql/           quality checks, reconciliation, prices_clean view
  checks.py      runs the SQL checks and writes flags
  anomalies.py   rolling volatility and z-scores on prices_clean
  digest.py      facts → Claude draft → verify → Slack
  harness.py     fault injection: recall and false-positive rate
  engine.py      DuckDB / Spark storage behind one interface
  pipeline.py    CLI and job entry point
app/             Streamlit dashboard
jobs/            Databricks task entry point
tests/           planted-fault, idempotency, verifier, Spark-SQL-parse tests
```

## Limitations

- **yfinance** wraps Yahoo's unofficial API. It can break or throttle without notice, and Yahoo's terms restrict this use. Option quotes are delayed and not in sync with the spot snapshot, so deep in-the-money contracts with wide markets still raise parity flags that are hard to judge.
- **The two vendors may not be independent.** Closes agreeing to within 0.5 bps everywhere suggests a shared upstream feed. The reconciliation then catches errors in fetching or storing data, not errors in the source data itself. A truly independent second source would make it stronger.
- **Pre-split history disagrees between vendors.** Alpha Vantage's free daily series is as-traded, while Yahoo's close is split-adjusted. The mismatch is detected and tagged rather than corrected.
- **The harness figures are synthetic.** Recall and false-positive rates come from a simulated market. Real false-positive rates need to be tracked on live runs over time.
- **Serverless packaging.** Databricks serverless can't import modules from bundle workspace files, so the job installs `mdm` as a wheel built by the bundle. NumPy is pinned below 2 to match the ABI of the image's preinstalled pandas and pyarrow.
- **Dividend yield and the risk-free rate are approximations.** Dividend yield is trailing-12-month cash dividends over spot, and the rate is the 13-week T-bill yield (^IRX). That's adequate for bounds checks, not for pricing.
