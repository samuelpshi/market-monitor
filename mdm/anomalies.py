"""Rolling-volatility and return z-score signals.

Uses adj_close so splits/dividends don't register as crashes, and voids returns
that span a missing session (a 2-day return scored against 1-day vol is a fake
anomaly). Reads prices_clean, so bars that failed quality checks are not
scored. The z-score scales today's return by the *prior* window's std, so the
move being scored doesn't dampen its own score."""

from datetime import date, timedelta

import numpy as np
import pandas as pd

from .checks import write_flags
from .engine import Engine

ANNUAL = np.sqrt(252)


def compute_signals(prices: pd.DataFrame, cal: pd.DataFrame, vol_window: int, long_window: int) -> pd.DataFrame:
    df = prices.merge(cal, on="trade_date", how="inner").sort_values(["ticker", "session_idx"])
    g = df.groupby("ticker", group_keys=False)
    gap = g["session_idx"].diff()
    df["ret"] = np.log(df["adj_close"] / g["adj_close"].shift(1))
    df.loc[gap != 1, "ret"] = np.nan
    rs = df.groupby("ticker")["ret"]
    prior_std = rs.transform(lambda r: r.shift(1).rolling(vol_window, min_periods=vol_window - 5).std())
    df["vol_short"] = rs.transform(lambda r: r.rolling(vol_window, min_periods=vol_window - 5).std()) * ANNUAL
    df["vol_long"] = rs.transform(lambda r: r.rolling(long_window, min_periods=long_window // 2).std()) * ANNUAL
    df["zscore"] = df["ret"] / prior_std
    df["vol_ratio"] = df["vol_short"] / df["vol_long"]
    return df[["ticker", "trade_date", "adj_close", "ret", "vol_short", "vol_long", "zscore", "vol_ratio"]]


def run_anomalies(engine: Engine, cfg: dict, as_of: date) -> pd.DataFrame:
    ac = cfg["anomalies"]
    lookback = as_of - timedelta(days=int(ac["long_window"] * 1.6) + 30)
    prices = engine.sql(f"""
        SELECT ticker, trade_date, adj_close FROM {engine.t('prices_clean')}
        WHERE trade_date BETWEEN DATE '{lookback}' AND DATE '{as_of}'""")
    cal = engine.sql(f"SELECT trade_date, session_idx FROM {engine.t('trading_calendar')}")
    for d in (prices, cal):
        d["trade_date"] = pd.to_datetime(d["trade_date"]).dt.date
    sig = compute_signals(prices, cal, ac["vol_window"], ac["long_window"])
    sig["as_of"] = as_of
    engine.write(sig, "signals", f"trade_date BETWEEN DATE '{lookback}' AND DATE '{as_of}'")

    today = sig[sig.trade_date == as_of]
    rows = []
    for r in today.itertuples():
        if pd.notna(r.zscore) and abs(r.zscore) >= ac["z_threshold"]:
            rows.append({
                "check_name": "return_zscore",
                "severity": "high" if abs(r.zscore) >= ac["z_threshold"] + 2 else "medium",
                "ticker": r.ticker, "trade_date": r.trade_date, "contract": None,
                "value": round(float(r.zscore), 2), "threshold": float(ac["z_threshold"]),
                "detail": f"return {r.ret * 100:+.2f}%, z={r.zscore:+.2f} vs {ac['vol_window']}d vol",
            })
        if pd.notna(r.vol_ratio) and r.vol_ratio >= ac["vol_ratio_threshold"]:
            rows.append({
                "check_name": "vol_regime", "severity": "medium",
                "ticker": r.ticker, "trade_date": r.trade_date, "contract": None,
                "value": round(float(r.vol_ratio), 2), "threshold": float(ac["vol_ratio_threshold"]),
                "detail": f"{ac['vol_window']}d vol {r.vol_short * 100:.1f}% vs "
                          f"{ac['long_window']}d {r.vol_long * 100:.1f}%",
            })
    flags = pd.DataFrame(rows, columns=["check_name", "severity", "ticker", "trade_date",
                                        "contract", "value", "threshold", "detail"])
    flags["contract"] = flags["contract"].astype("object")
    flags["stage"] = "anomaly"
    flags["as_of"] = as_of
    write_flags(engine, flags, as_of, "anomaly")
    return flags
