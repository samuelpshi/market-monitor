import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from .calendar import calendar_frame, sessions
from .engine import Engine

log = logging.getLogger(__name__)
CALENDAR_START = date(2015, 1, 2)  # fixed so session_idx is stable across runs


def rotation(tickers: list[str], as_of: date, budget: int) -> list[str]:
    """Deterministic slice of the watchlist for today's secondary-vendor calls.
    With 50 tickers and a 24-call budget every ticker is reconciled at least
    every 3 sessions; the vendor returns ~100 days per call, so history fills in."""
    tickers = sorted(tickers)
    if budget >= len(tickers):
        return tickers
    idx = len(sessions(CALENDAR_START, as_of))
    start = (idx * budget) % len(tickers)
    return [tickers[(start + i) % len(tickers)] for i in range(budget)]


def _sql_list(xs) -> str:
    return ", ".join("'" + str(x).replace("'", "''") + "'" for x in xs)


def _stamp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    return df


def run_ingest(engine: Engine, cfg: dict, as_of: date, primary, secondary=None) -> list[dict]:
    ic = cfg["ingest"]
    tickers = cfg["watchlist"]
    log_rows = []

    engine.write(calendar_frame(CALENDAR_START, as_of + timedelta(days=60)), "trading_calendar")
    engine.write(pd.DataFrame({"ticker": tickers}), "watchlist")

    first_load = not engine.exists("prices_primary")
    start = as_of - timedelta(days=ic["history_days"] if first_load else ic["incremental_days"])

    # primary prices: only replace rows for tickers that actually came back, so a
    # failed download doesn't delete history and fake a run of missing days.
    p = primary.prices(tickers, start, as_of)
    got = sorted(p["ticker"].unique()) if not p.empty else []
    if got:
        engine.write(_stamp(p), "prices_primary",
                     f"ticker IN ({_sql_list(got)}) AND trade_date BETWEEN DATE '{start}' AND DATE '{as_of}'")
    log_rows.append({"source": primary.name, "dataset": "prices", "requested": len(tickers),
                     "returned": len(got), "rows": len(p)})

    if secondary is not None:
        todays = rotation(tickers, as_of, ic["secondary_daily_budget"])
        s = secondary.prices(todays, as_of - timedelta(days=150), as_of)
        got_s = sorted(s["ticker"].unique()) if not s.empty else []
        if got_s:
            engine.write(_stamp(s), "prices_secondary",
                         f"ticker IN ({_sql_list(got_s)}) AND trade_date BETWEEN "
                         f"DATE '{s.trade_date.min()}' AND DATE '{s.trade_date.max()}'")
        log_rows.append({"source": secondary.name, "dataset": "prices", "requested": len(todays),
                         "returned": len(got_s), "rows": len(s)})

    if ic.get("options_tickers"):
        rate = primary.risk_free_rate(ic.get("risk_free_symbol"))
        o = primary.options(ic["options_tickers"], as_of, ic["max_expiries"], rate,
                            ic.get("expiry_targets_days"))
        engine.write(_stamp(o), "options_quotes", f"snapshot_date = DATE '{as_of}'")
        log_rows.append({"source": primary.name, "dataset": "options",
                         "requested": len(ic["options_tickers"]),
                         "returned": o["ticker"].nunique() if not o.empty else 0, "rows": len(o)})

    lg = pd.DataFrame(log_rows)
    lg["as_of"] = as_of
    engine.write(_stamp(lg), "ingest_log", f"as_of = DATE '{as_of}'")
    for r in log_rows:
        log.info("ingest %s", r)
    return log_rows
