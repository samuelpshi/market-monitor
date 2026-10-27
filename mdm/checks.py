from datetime import date

import pandas as pd

from .engine import Engine

FLAG_COLS = ["check_name", "severity", "ticker", "trade_date", "contract", "value", "threshold", "detail"]


def window_start(engine: Engine, as_of: date, sessions_back: int) -> date:
    df = engine.sql(f"""
        SELECT min(trade_date) AS d FROM (
          SELECT trade_date FROM {engine.t('trading_calendar')}
          WHERE trade_date <= DATE '{as_of}' ORDER BY trade_date DESC LIMIT {sessions_back + 1}
        ) x""")
    return pd.Timestamp(df["d"].iloc[0]).date()


def _collect(engine: Engine, files: list[tuple[str, dict]], as_of: date, stage: str) -> pd.DataFrame:
    frames = []
    for f, params in files:
        df = engine.run_file(f, as_of=as_of, **params)
        if not df.empty:
            frames.append(df[FLAG_COLS])
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=FLAG_COLS)
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
    out["value"] = out["value"].astype("float64")
    out["threshold"] = out["threshold"].astype("float64")
    out["contract"] = out["contract"].astype("object")
    out["stage"] = stage
    out["as_of"] = as_of
    return out


def write_flags(engine: Engine, flags: pd.DataFrame, as_of: date, stage: str) -> None:
    engine.write(flags, "flags", f"as_of = DATE '{as_of}' AND stage = '{stage}'")


def run_quality(engine: Engine, cfg: dict, as_of: date) -> pd.DataFrame:
    c = cfg["checks"]
    ws = window_start(engine, as_of, c["missing_lookback_sessions"])
    pc = c["parity"]
    files = [
        ("stale_latest.sql", {}),
        ("stale_repeat.sql", {"repeat_days": c["stale_repeat_days"], "window_start": ws}),
        ("missing_days.sql", {"window_start": ws}),
        ("bad_ohlc.sql", {"window_start": ws}),
    ]
    if engine.exists("options_quotes"):
        files += [
            ("crossed_quotes.sql", {}),
            ("put_call_parity.sql", {
                "min_t": pc["min_days_to_expiry"] / 365.0, "m_lo": pc["moneyness_min"],
                "m_hi": pc["moneyness_max"], "min_oi": pc["min_open_interest"],
                "abs_tol": pc["abs_tolerance"], "spot_tol": pc["spot_tolerance_pct"],
            }),
            ("stale_quotes.sql", {
                "m_lo": pc["moneyness_min"], "m_hi": pc["moneyness_max"],
                "min_spot_move": c["stale_quote"]["min_spot_move"],
                "min_share": c["stale_quote"]["min_share"],
            }),
        ]
    flags = _collect(engine, files, as_of, "quality")
    # A stale chain breaks parity on every strike; report the root cause once
    # and demote the parity flags it explains.
    stale = set(flags.loc[flags.check_name == "stale_quote", "ticker"])
    m = (flags.check_name == "put_call_parity") & flags.ticker.isin(stale)
    flags.loc[m, "severity"] = "low"
    flags.loc[m, "detail"] = flags.loc[m, "detail"] + " (chain flagged stale)"
    write_flags(engine, flags, as_of, "quality")
    create_views(engine)
    return flags


def create_views(engine: Engine) -> None:
    """prices_clean = prices_primary minus bars flagged as suspect."""
    for stmt in engine.render("views.sql").split(";"):
        if stmt.strip():
            engine.sql(stmt)


def run_reconciliation(engine: Engine, cfg: dict, as_of: date) -> pd.DataFrame:
    rc = cfg["reconciliation"]
    if not engine.exists("prices_secondary"):
        return pd.DataFrame(columns=FLAG_COLS)
    ws = window_start(engine, as_of, rc["lookback_sessions"])
    flags = _collect(engine, [("reconciliation.sql", {
        "window_start": ws, "warn_bps": rc["warn_bps"], "break_bps": rc["break_bps"],
    })], as_of, "reconciliation")
    write_flags(engine, flags, as_of, "reconciliation")
    create_views(engine)
    return flags
