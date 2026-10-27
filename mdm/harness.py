"""Fault-injection harness: measures recall and false-positive rate of the checks.

Each batch takes a clean synthetic market, plants at most one random fault per
ticker (so faults can't mask each other), runs the real quality /
reconciliation / anomaly code against an in-memory DuckDB, and scores which
plants were flagged. Fault sizes are drawn on both sides of each threshold on
purpose, so recall is reported separately for faults the checks are specified
to catch ("in spec") and for sub-threshold ones. A clean run with no plants
gives the false-positive rate.

    python -m mdm.harness --batches 8
"""

import argparse
import json
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from .anomalies import run_anomalies
from .calendar import sessions
from .checks import run_quality, run_reconciliation
from .config import load_config
from .engine import DuckDBEngine
from .ingest import CALENDAR_START, _stamp
from .calendar import calendar_frame
from .sources import SyntheticSource

KINDS = ["stale_repeat", "missing_day", "bad_ohlc", "recon_break", "crossed_quote",
         "put_call_parity", "stale_quote", "return_zscore"]
# the flag(s) that count as catching each fault kind
CATCHES = {"stale_repeat": {"stale_repeat"}, "missing_day": {"missing_day"}, "bad_ohlc": {"bad_ohlc"},
           "recon_break": {"recon_break"}, "crossed_quote": {"crossed_quote"},
           "put_call_parity": {"put_call_parity"}, "stale_quote": {"stale_quote"},
           "return_zscore": {"return_zscore"}}


def _market(cfg, as_of, seed):
    syn = SyntheticSource(cfg["watchlist"], sessions(as_of - timedelta(days=600), as_of), seed=seed, plant=False)
    S = syn.sessions
    prices = syn.prices(cfg["watchlist"], S[0], S[-1])
    secondary = syn.secondary_prices(cfg["watchlist"], S[-100], S[-1])
    opts_prev = syn._chain(cfg["watchlist"], S[-2], 2, 0.04)
    opts = syn._chain(cfg["watchlist"], S[-1], 2, 0.04)
    return syn, prices, secondary, opts_prev, opts


def _plant(rng, kind, t, S, prices, secondary, opts_prev, opts, cfg):
    """Mutates frames in place; returns the plant record."""
    c = cfg["checks"]
    rec = {"kind": kind, "ticker": t}
    tp = prices.ticker == t
    if kind == "stale_repeat":
        k = int(rng.integers(1, 5))  # 1 = a single repeat: below the 2-session spec
        end = len(S) - 1 - int(rng.integers(0, 10))
        src = prices.index[tp & (prices.trade_date == S[end - k])][0]
        for i in range(end - k + 1, end + 1):
            j = prices.index[tp & (prices.trade_date == S[i])]
            prices.loc[j, ["open", "high", "low", "close", "adj_close", "volume"]] = \
                prices.loc[src, ["open", "high", "low", "close", "adj_close", "volume"]].values
        rec.update(date=S[end], size=k, in_spec=k >= c["stale_repeat_days"])
    elif kind == "missing_day":
        d = S[len(S) - 2 - int(rng.integers(0, 40))]
        prices.loc[tp & (prices.trade_date == d), "_drop"] = True  # removed in _run
        rec.update(date=d, size=1, in_spec=True)
    elif kind == "bad_ohlc":
        d = S[len(S) - 1 - int(rng.integers(0, 40))]
        e = float(rng.choice([0.00002, 0.0005, 0.005, 0.02]))
        j = prices.index[tp & (prices.trade_date == d)]
        prices.loc[j, "close"] = prices.loc[j, "high"] * (1 + e)
        rec.update(date=d, size=e, in_spec=e > 1e-4)
    elif kind == "recon_break":
        d = S[len(S) - 1 - int(rng.integers(0, 15))]
        bps = float(rng.uniform(2, 200))
        j = secondary.index[(secondary.ticker == t) & (secondary.trade_date == d)]
        secondary.loc[j, "close"] = secondary.loc[j, "close"] * (1 + bps / 1e4 * rng.choice([-1, 1]))
        rec.update(date=d, size=bps, in_spec=bps >= cfg["reconciliation"]["warn_bps"] + 4)
    elif kind in ("crossed_quote", "put_call_parity"):
        pc = c["parity"]
        cand = opts[(opts.ticker == t) & (opts.option_type == "C") & (opts.t_years >= pc["min_days_to_expiry"] / 365)
                    & (opts.strike / opts.spot).between(pc["moneyness_min"], pc["moneyness_max"]) & (opts.bid > 0)]
        i = cand.index[int(rng.integers(0, len(cand)))]
        if kind == "crossed_quote":
            delta = float(rng.choice([0.01, 0.05, 0.25]))
            opts.loc[i, "bid"] = opts.loc[i, "ask"] + delta
            rec.update(date=S[-1], size=delta, in_spec=True)
        else:
            shift = float(rng.uniform(0.02, 3.0))
            opts.loc[i, ["bid", "ask"]] += shift
            r = opts.loc[i]
            put = opts[(opts.ticker == t) & (opts.expiry == r.expiry) & (opts.strike == r.strike) & (opts.option_type == "P")].iloc[0]
            tol = (r.ask - r.bid) / 2 + (put.ask - put.bid) / 2 + pc["abs_tolerance"] + pc["spot_tolerance_pct"] * r.spot
            # slack between the European value and the American upper bound
            slack = r.spot * (1 - np.exp(-r.div_yield * r.t_years))
            rec.update(date=S[-1], size=shift, in_spec=shift > tol + slack)
    elif kind == "stale_quote":
        m = opts.ticker == t
        prev = opts_prev[opts_prev.ticker == t].set_index(["expiry", "strike", "option_type"])
        keyed = opts[m].set_index(["expiry", "strike", "option_type"])
        opts.loc[m, ["bid", "ask"]] = prev.loc[keyed.index, ["bid", "ask"]].values
        move = abs(opts.loc[m, "spot"].iloc[0] / opts_prev.loc[opts_prev.ticker == t, "spot"].iloc[0] - 1)
        rec.update(date=S[-1], size=float(move), in_spec=move >= c["stale_quote"]["min_spot_move"])
    elif kind == "return_zscore":
        rets = np.log(prices.loc[tp, "adj_close"]).diff().iloc[-21:-1]
        z = float(rng.uniform(1.5, 8)) * rng.choice([-1, 1])
        # set the whole day's return to exactly z prior-window sigmas
        prev_close = float(prices.loc[tp & (prices.trade_date == S[-2]), "adj_close"].iloc[0])
        j = prices.index[tp & (prices.trade_date == S[-1])]
        factor = prev_close * float(np.exp(z * rets.std())) / float(prices.loc[j, "adj_close"].iloc[0])
        prices.loc[j, ["open", "high", "low", "close", "adj_close"]] *= factor
        secondary.loc[(secondary.ticker == t) & (secondary.trade_date == S[-1]), "close"] *= factor
        rec.update(date=S[-1], size=abs(z), in_spec=abs(z) >= cfg["anomalies"]["z_threshold"])
    return rec


def _run(cfg, as_of, prices, secondary, opts_prev, opts):
    eng = DuckDBEngine(":memory:", "mm")
    eng.write(calendar_frame(CALENDAR_START, as_of + timedelta(days=30)), "trading_calendar")
    eng.write(pd.DataFrame({"ticker": cfg["watchlist"]}), "watchlist")
    if "_drop" in prices:
        prices = prices[prices["_drop"].isna()].drop(columns="_drop")
    eng.write(_stamp(prices.reset_index(drop=True)), "prices_primary")
    eng.write(_stamp(secondary.reset_index(drop=True)), "prices_secondary")
    eng.write(_stamp(pd.concat([opts_prev, opts], ignore_index=True)), "options_quotes")
    return pd.concat([run_quality(eng, cfg, as_of), run_reconciliation(eng, cfg, as_of),
                      run_anomalies(eng, cfg, as_of)], ignore_index=True)


def _caught(rec, flags):
    f = flags[(flags.ticker == rec["ticker"]) & flags.check_name.isin(CATCHES[rec["kind"]])]
    if rec["kind"] in ("missing_day", "bad_ohlc", "recon_break", "stale_repeat"):
        f = f[f.trade_date == rec["date"]]
    return not f.empty


def run_harness(cfg, as_of: date, batches: int, seed: int = 0):
    cfg = json.loads(json.dumps(cfg))  # private copy
    tickers = cfg["watchlist"]
    rng = np.random.default_rng(seed)
    plants, fps, clean_stats = [], [], []

    # clean runs: no plants at all -> every non-low flag is a false positive
    for b in range(max(4, batches // 2)):
        _, prices, secondary, op, o = _market(cfg, as_of, seed=1000 + b)
        flags = _run(cfg, as_of, prices, secondary, op, o)
        clean_stats.append({"batch": b, "flags": len(flags), "non_low": int((flags.severity != "low").sum()),
                            "price_rows": len(prices), "recon_rows": len(secondary), "option_rows": len(o)})
        fps.append(flags.assign(batch=f"clean{b}"))

    for b in range(batches):
        syn, prices, secondary, op, o = _market(cfg, as_of, seed=b)
        S = syn.sessions
        batch_plants = []
        for t in tickers:
            if rng.random() < 0.15:
                continue
            kind = KINDS[int(rng.integers(0, len(KINDS)))]
            batch_plants.append(_plant(rng, kind, t, S, prices, secondary, op, o, cfg))
        flags = _run(cfg, as_of, prices, secondary, op, o)
        for rec in batch_plants:
            rec["caught"] = _caught(rec, flags)
            rec["batch"] = b
            plants.append(rec)
        planted_t = {r["ticker"] for r in batch_plants}
        stray = flags[~flags.ticker.isin(planted_t) & (flags.severity != "low")]
        fps.append(stray.assign(batch=f"plant{b}"))

    P = pd.DataFrame(plants)
    by_kind = P.groupby("kind").apply(lambda g: pd.Series({
        "planted": len(g), "in_spec": int(g.in_spec.sum()),
        "caught_in_spec": int((g.caught & g.in_spec).sum()),
        "recall_in_spec": (g.caught & g.in_spec).sum() / max(g.in_spec.sum(), 1),
        "below_spec": int((~g.in_spec).sum()),
        "caught_below_spec": int((g.caught & ~g.in_spec).sum()),
    }), include_groups=False)
    F = pd.concat(fps, ignore_index=True)
    clean = pd.DataFrame(clean_stats)
    clean_fp = F[F.batch.str.startswith("clean") & (F.severity != "low")]
    summary = {
        "faults_planted": int(len(P)),
        "in_spec": int(P.in_spec.sum()),
        "recall_in_spec": float((P.caught & P.in_spec).sum() / P.in_spec.sum()),
        "recall_all": float(P.caught.mean()),
        "clean_runs": int(len(clean)),
        "clean_false_positives": int(len(clean_fp)),
        "clean_fp_per_run": float(len(clean_fp) / len(clean)),
        "clean_fp_by_check": clean_fp.check_name.value_counts().to_dict(),
        "stray_flags_in_planted_runs": int(F.batch.str.startswith("plant").sum()),
    }
    return summary, by_kind, P, F


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--batches", type=int, default=8)
    ap.add_argument("--as-of", type=date.fromisoformat, default=date(2026, 10, 23))
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default="data/harness_results.json")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.WARNING)
    cfg = load_config()
    summary, by_kind, P, F = run_harness(cfg, a.as_of, a.batches, a.seed)
    pd.set_option("display.width", 160)
    shown = by_kind.copy()
    shown["recall_in_spec"] = shown["recall_in_spec"].map("{:.1%}".format)
    print(shown.to_string())
    print(json.dumps(summary, indent=2, default=str))
    missed = P[P.in_spec & ~P.caught]
    if len(missed):
        print("\nin-spec misses:\n" + missed.to_string(index=False))
    from pathlib import Path

    from .config import ROOT

    out = ROOT / a.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"summary": summary, "by_kind": by_kind.reset_index().to_dict("records"),
                               "plants": P.to_dict("records")}, indent=1, default=str))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
