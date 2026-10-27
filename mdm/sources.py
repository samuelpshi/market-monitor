"""Data sources. Each returns pandas frames in the warehouse schema:

prices_primary   ticker, trade_date, open, high, low, close, adj_close, volume, source
prices_secondary ticker, trade_date, open, high, low, close, volume, source
options_quotes   snapshot_date, ticker, expiry, strike, option_type, bid, ask, last,
                 volume, open_interest, implied_vol, spot, div_yield, rate, t_years, source
"""

import logging
import math
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)

PRICE_COLS = ["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source"]
OPTION_COLS = [
    "snapshot_date", "ticker", "expiry", "strike", "option_type", "bid", "ask", "last",
    "volume", "open_interest", "implied_vol", "spot", "div_yield", "rate", "t_years", "source",
]


# --------------------------------------------------------------------------- yfinance
class YahooSource:
    """Primary source. yfinance scrapes Yahoo's unofficial endpoints: it breaks
    without notice, rate-limits aggressively, and Yahoo's terms restrict use."""

    name = "yahoo"

    def prices(self, tickers: list[str], start: date, end: date) -> pd.DataFrame:
        import yfinance as yf

        raw = yf.download(
            tickers, start=start, end=end + timedelta(days=1), auto_adjust=False,
            group_by="ticker", progress=False, threads=True,
        )
        frames = []
        for t in tickers:
            if t not in raw.columns.get_level_values(0):
                log.warning("yahoo returned nothing for %s", t)
                continue
            d = raw[t].dropna(how="all").reset_index()
            if d.empty:
                continue
            frames.append(pd.DataFrame({
                "ticker": t,
                "trade_date": pd.to_datetime(d["Date"]).dt.date,
                "open": d["Open"], "high": d["High"], "low": d["Low"],
                # "Close" with auto_adjust=False is split-adjusted but NOT dividend-adjusted.
                "close": d["Close"], "adj_close": d["Adj Close"],
                "volume": d["Volume"].astype("float64"),
                "source": self.name,
            }))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=PRICE_COLS)

    def risk_free_rate(self, symbol: str = "^IRX") -> float:
        import yfinance as yf

        h = yf.Ticker(symbol).history(period="10d")["Close"].dropna()
        if h.empty:
            log.warning("no risk-free rate from %s; using 0.04", symbol)
            return 0.04
        # ^IRX is a discount yield in percent; close enough to a cont. rate for bounds checks.
        return float(h.iloc[-1]) / 100.0

    @staticmethod
    def pick_expiries(expiries: list[str], snapshot_date: date, targets_days: list[int]) -> list[str]:
        """Nearest listed expiry to each target DTE. Taking the first N listed
        expiries fails for SPY/QQQ and mega-caps, whose first several are
        0-5 DTE dailies/weeklies that the parity check (rightly) skips."""
        dte = {e: (date.fromisoformat(e) - snapshot_date).days for e in expiries}
        picked = []
        for t in targets_days:
            cands = [e for e in expiries if dte[e] >= 7]
            if cands:
                e = min(cands, key=lambda e: abs(dte[e] - t))
                if e not in picked:
                    picked.append(e)
        return picked

    def options(self, tickers: list[str], snapshot_date: date, max_expiries: int, rate: float,
                targets_days: list[int] | None = None) -> pd.DataFrame:
        import yfinance as yf

        targets_days = targets_days or [14, 30, 60, 90][:max_expiries]
        frames = []
        for t in tickers:
            tk = yf.Ticker(t)
            try:
                spot = float(tk.fast_info["lastPrice"])
                expiries = self.pick_expiries(list(tk.options), snapshot_date, targets_days)
                divs = tk.dividends
            except Exception as e:  # yfinance raises a zoo of exception types
                log.warning("options metadata failed for %s: %s", t, e)
                continue
            # Trailing-12m cash dividends / spot. yfinance's info["dividendYield"]
            # changed units (fraction -> percent) in 2025, so don't trust it.
            if len(divs):
                cutoff = pd.Timestamp(snapshot_date, tz=divs.index.tz) - pd.Timedelta(days=365)
                q = float(divs[divs.index >= cutoff].sum()) / spot
            else:
                q = 0.0
            for exp in expiries:
                try:
                    chain = tk.option_chain(exp)
                except Exception as e:
                    log.warning("chain %s %s failed: %s", t, exp, e)
                    continue
                exp_d = date.fromisoformat(exp)
                for kind, df in (("C", chain.calls), ("P", chain.puts)):
                    if df.empty:
                        continue
                    frames.append(pd.DataFrame({
                        "snapshot_date": snapshot_date, "ticker": t, "expiry": exp_d,
                        "strike": df["strike"].astype(float), "option_type": kind,
                        "bid": df["bid"].astype(float), "ask": df["ask"].astype(float),
                        "last": df["lastPrice"].astype(float),
                        "volume": df["volume"].astype(float),
                        "open_interest": df["openInterest"].astype(float),
                        "implied_vol": df["impliedVolatility"].astype(float),
                        "spot": spot, "div_yield": q, "rate": rate,
                        "t_years": (exp_d - snapshot_date).days / 365.0,
                        "source": self.name,
                    }))
                time.sleep(0.5)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OPTION_COLS)


# ---------------------------------------------------------------------- Alpha Vantage
class AlphaVantageSource:
    """Secondary source for reconciliation. Free tier: ~25 requests/day and a
    per-minute cap; throttling comes back as HTTP 200 with a "Note"/"Information"
    body, not an error status, so it must be detected explicitly."""

    name = "alphavantage"
    URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str, pause_s: float = 13.0):
        self.api_key = api_key
        self.pause_s = pause_s

    def prices(self, tickers: list[str], start: date, end: date) -> pd.DataFrame:
        frames = []
        for i, t in enumerate(tickers):
            if i:
                time.sleep(self.pause_s)
            r = requests.get(self.URL, params={
                # TIME_SERIES_DAILY is the free endpoint and is as-traded (NOT split-adjusted).
                "function": "TIME_SERIES_DAILY", "symbol": t,
                "outputsize": "compact", "apikey": self.api_key,
            }, timeout=30)
            r.raise_for_status()
            body = r.json()
            series = body.get("Time Series (Daily)")
            if series is None:
                msg = body.get("Note") or body.get("Information") or body.get("Error Message") or str(body)[:200]
                log.error("alphavantage %s: %s", t, msg)
                if "rate limit" in msg.lower() or "requests per day" in msg.lower():
                    break  # daily quota gone; the rest will fail too
                continue
            d = pd.DataFrame.from_dict(series, orient="index").astype(float)
            d.index = pd.to_datetime(d.index).date
            d = d[(d.index >= start) & (d.index <= end)]
            frames.append(pd.DataFrame({
                "ticker": t, "trade_date": d.index,
                "open": d["1. open"].values, "high": d["2. high"].values,
                "low": d["3. low"].values, "close": d["4. close"].values,
                "volume": d["5. volume"].values, "source": self.name,
            }))
        cols = ["ticker", "trade_date", "open", "high", "low", "close", "volume", "source"]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)


# ------------------------------------------------------------------------- synthetic
def _bs(S, K, T, r, q, sigma, kind):
    from math import erf, exp, log, sqrt

    N = lambda x: 0.5 * (1 + erf(x / sqrt(2)))  # noqa: E731
    d1 = (log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    if kind == "C":
        return S * exp(-q * T) * N(d1) - K * exp(-r * T) * N(d2)
    return K * exp(-r * T) * N(-d2) - S * exp(-q * T) * N(-d1)


class SyntheticSource:
    """Deterministic fake market with known injected faults, for demos and tests.
    `faults` records exactly what was planted so tests can assert each is caught."""

    name = "synthetic"

    def __init__(self, tickers: list[str], sessions: list[date], seed: int = 7, plant: bool = True):
        self.tickers, self.sessions = tickers, sessions
        rng = np.random.default_rng(seed)
        self.faults: list[dict] = []
        rows = []
        for j, t in enumerate(tickers):
            s0 = 50 + 40 * j
            rets = rng.normal(0.0003, 0.015, len(sessions))
            px = s0 * np.exp(np.cumsum(rets))
            for d, c in zip(sessions, px):
                o = c * (1 + rng.normal(0, 0.003))
                rows.append({
                    "ticker": t, "trade_date": d, "open": o,
                    "high": max(o, c) * (1 + abs(rng.normal(0, 0.004))),
                    "low": min(o, c) * (1 - abs(rng.normal(0, 0.004))),
                    "close": c, "adj_close": c, "volume": float(rng.integers(1e6, 5e6)),
                })
        self.base = pd.DataFrame(rows)
        self.plant = plant
        self._recon_planted = False
        if plant:
            self._plant()

    def _plant(self):
        b, S = self.base, self.sessions
        t0, t1, t2 = self.tickers[0], self.tickers[1], self.tickers[2]
        last = S[-1]
        # 1) big move on the last day (anomaly)
        m = (b.ticker == t0) & (b.trade_date == last)
        prev = b.loc[(b.ticker == t0) & (b.trade_date == S[-2]), "close"].iloc[0]
        for col in ("open", "high", "low", "close", "adj_close"):
            b.loc[m, col] = prev * 0.91
        b.loc[m, "high"] = prev * 0.93
        b.loc[m, "low"] = prev * 0.89
        self.faults.append({"kind": "big_move", "ticker": t0, "date": last})
        # 2) stale: t1 repeats the same bar for the last 3 sessions
        ref = b[(b.ticker == t1) & (b.trade_date == S[-4])].iloc[0]
        for d in S[-3:]:
            m = (b.ticker == t1) & (b.trade_date == d)
            for col in ("open", "high", "low", "close", "adj_close", "volume"):
                b.loc[m, col] = ref[col]
        self.faults.append({"kind": "stale", "ticker": t1, "date": last})
        # 3) missing day for t2 mid-window
        miss = S[-15]
        self.base = b = b[~((b.ticker == t2) & (b.trade_date == miss))].reset_index(drop=True)
        self.faults.append({"kind": "missing_day", "ticker": t2, "date": miss})
        # 4) bad OHLC: close above high
        d = S[-6]
        m = (b.ticker == t2) & (b.trade_date == d)
        b.loc[m, "high"] = b.loc[m, "close"] * 0.99
        self.faults.append({"kind": "bad_ohlc", "ticker": t2, "date": d})

    def prices(self, tickers, start, end) -> pd.DataFrame:
        b = self.base
        out = b[b.ticker.isin(tickers) & (b.trade_date >= start) & (b.trade_date <= end)].copy()
        out["source"] = self.name
        return out[PRICE_COLS].reset_index(drop=True)

    def secondary_prices(self, tickers, start, end) -> pd.DataFrame:
        """Second vendor: rounding noise, plus planted breaks on tickers that
        were actually requested today (the rotation decides which)."""
        p = self.prices(tickers, start, end).drop(columns=["adj_close"])
        # vendor noise: fat-tailed, ~2 bps scale, then rounded to cents
        rng = np.random.default_rng(3)
        p["close"] = (p["close"] * (1 + 2e-4 * rng.standard_t(3, len(p)) / np.sqrt(3))).round(2)
        p["source"] = "synthetic_b"
        planted = {self.tickers[0], self.tickers[1], self.tickers[2]}
        cands = [t for t in tickers if t not in planted]
        last = self.sessions[-1]
        if self.plant and len(cands) >= 2 and not self._recon_planted and last in set(p.trade_date):
            self._recon_planted = True
            m = (p.ticker == cands[0]) & (p.trade_date == last)
            p.loc[m, "close"] *= 1.012  # 120 bps break
            self.faults.append({"kind": "recon_break", "ticker": cands[0], "date": last})
            # second source is as-traded across a split the primary adjusted for
            d = self.sessions[-10]
            m = (p.ticker == cands[1]) & (p.trade_date == d)
            p.loc[m, "close"] *= 2.0
            self.faults.append({"kind": "split_mismatch", "ticker": cands[1], "date": d})
        return p

    def risk_free_rate(self, symbol=None) -> float:
        return 0.04

    def _chain(self, tickers, snapshot_date, max_expiries, rate) -> pd.DataFrame:
        rows = []
        rng = np.random.default_rng(int(pd.Timestamp(snapshot_date).strftime("%Y%m%d")))
        spots = self.base[self.base.trade_date == snapshot_date].set_index("ticker")["close"]
        for t in tickers:
            if t not in spots:
                continue
            S = float(spots[t])
            for k in range(1, max_expiries + 1):
                expiry = self.sessions[-1] + timedelta(days=14 * k)
                T = (expiry - snapshot_date).days / 365.0
                for K in np.round(float(self.base[self.base.ticker == t].close.iloc[-30]) * np.linspace(0.8, 1.2, 9), 0):
                    for kind in ("C", "P"):
                        v = _bs(S, K, T, rate, 0.005, 0.25, kind)
                        half = max(0.02, 0.01 * v)
                        rows.append({
                            "snapshot_date": snapshot_date, "ticker": t, "expiry": expiry,
                            "strike": float(K), "option_type": kind,
                            "bid": round(max(v - half, 0.0), 2), "ask": round(v + half, 2),
                            "last": round(v, 2), "volume": float(rng.integers(0, 500)),
                            "open_interest": float(rng.integers(50, 5000)), "implied_vol": 0.25,
                            "spot": S, "div_yield": 0.005, "rate": rate, "t_years": T,
                            "source": self.name,
                        })
        return pd.DataFrame(rows, columns=OPTION_COLS)

    def options(self, tickers, snapshot_date, max_expiries, rate, targets_days=None) -> pd.DataFrame:
        df = self._chain(tickers, snapshot_date, max_expiries, rate)
        if snapshot_date != self.sessions[-1] or df.empty:
            return df  # faults only on the latest snapshot
        if not self.plant:
            return df
        t = tickers[2]
        # crossed quote
        i = df.index[(df.ticker == t) & (df.option_type == "C")][5]
        df.loc[i, "bid"] = df.loc[i, "ask"] + 0.30
        self.faults.append({"kind": "crossed", "ticker": t, "date": snapshot_date})
        # parity violation: a near-ATM call priced far too rich, with a tight spread
        sub = df[(df.ticker == tickers[1]) & (df.option_type == "C") & (df.t_years > 0.05)]
        j = (sub.strike - sub.spot).abs().idxmin()
        df.loc[j, "bid"] += 3.0
        df.loc[j, "ask"] += 3.0
        self.faults.append({"kind": "parity", "ticker": tickers[1], "date": snapshot_date})
        # stale quotes: a ticker whose whole chain is yesterday's, after a big spot move
        st = tickers[0]
        prev = self._chain([st], self.sessions[-2], max_expiries, rate).set_index(["expiry", "strike", "option_type"])
        m = df.ticker == st
        keyed = df[m].set_index(["expiry", "strike", "option_type"])
        keyed[["bid", "ask"]] = prev.loc[keyed.index, ["bid", "ask"]].values
        df.loc[m, ["bid", "ask"]] = keyed[["bid", "ask"]].values
        self.faults.append({"kind": "stale_quote", "ticker": st, "date": snapshot_date})
        return df
