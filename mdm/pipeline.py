"""CLI / job entry point.

    python -m mdm.pipeline --step all                 # live sources
    python -m mdm.pipeline --step all --synthetic     # fake market with planted faults
    python -m mdm.pipeline --step digest --no-post
"""

import argparse
import logging
from datetime import date, timedelta

from .anomalies import run_anomalies
from .calendar import last_completed_session, sessions
from .checks import run_quality, run_reconciliation
from .config import get_secret, load_config
from .digest import run_digest
from .engine import get_engine
from .ingest import CALENDAR_START, run_ingest

STEPS = ["ingest", "quality", "reconcile", "anomalies", "digest"]


class _Secondary:
    """Adapts SyntheticSource's second-vendor feed to the source interface."""

    name = "synthetic_b"

    def __init__(self, syn):
        self.syn = syn

    def prices(self, tickers, start, end):
        return self.syn.secondary_prices(tickers, start, end)


def build_sources(cfg, as_of, synthetic: bool, market_end: date | None = None):
    if synthetic:
        from .sources import SyntheticSource

        end = market_end or as_of
        syn = SyntheticSource(cfg["watchlist"], sessions(end - timedelta(days=600), end))
        return syn, _Secondary(syn)
    from .sources import AlphaVantageSource, YahooSource

    key = get_secret("ALPHA_VANTAGE_API_KEY")
    if not key:
        logging.warning("ALPHA_VANTAGE_API_KEY not set; skipping secondary vendor")
    return YahooSource(), (AlphaVantageSource(key) if key else None)


def run(steps, cfg, as_of: date, synthetic=False, use_llm=True, post=True, engine=None, sources=None):
    engine = engine or get_engine(cfg)
    out = {}
    if "ingest" in steps:
        primary, secondary = sources or build_sources(cfg, as_of, synthetic)
        out["ingest"] = run_ingest(engine, cfg, as_of, primary, secondary)
    if "quality" in steps:
        out["quality"] = run_quality(engine, cfg, as_of)
    if "reconcile" in steps:
        out["reconcile"] = run_reconciliation(engine, cfg, as_of)
    if "anomalies" in steps:
        out["anomalies"] = run_anomalies(engine, cfg, as_of)
    if "digest" in steps:
        out["digest"] = run_digest(engine, cfg, as_of, use_llm=use_llm, post=post)
    return engine, out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", default="all", choices=STEPS + ["all"])
    ap.add_argument("--as-of", type=date.fromisoformat, default=None)
    ap.add_argument("--config", default=None)
    ap.add_argument("--namespace", default=None, help="catalog.schema for Spark tables (overrides config)")
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--demo", action="store_true",
                    help="synthetic market; runs the prior session first so day-over-day checks have history")
    ap.add_argument("--no-llm", action="store_true")
    ap.add_argument("--no-post", action="store_true")
    ap.add_argument("--post", choices=["yes", "no"], default="yes", help="for job configs; --post no == --no-post")
    a = ap.parse_args(argv)
    a.no_post = a.no_post or a.post == "no"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = load_config(a.config)
    if a.namespace:
        cfg["storage"]["spark_namespace"] = a.namespace
    as_of = a.as_of or last_completed_session()
    if as_of not in sessions(CALENDAR_START, as_of):
        raise SystemExit(f"{as_of} is not an NYSE session; pass a session date")
    steps = STEPS if a.step == "all" else [a.step]
    if a.demo:
        srcs = build_sources(cfg, as_of, True, market_end=as_of)
        prev = sessions(as_of - timedelta(days=10), as_of)[-2]
        engine, _ = run(STEPS[:-1], cfg, prev, True, sources=srcs)
        _, out = run(steps, cfg, as_of, True, not a.no_llm, not a.no_post, engine=engine, sources=srcs)
    else:
        _, out = run(steps, cfg, as_of, a.synthetic, not a.no_llm, not a.no_post)
    for k, v in out.items():
        if hasattr(v, "shape"):
            print(f"{k}: {len(v)} flags")
            if len(v):
                print(v[["check_name", "severity", "ticker", "trade_date", "detail"]].to_string(index=False))
    if "digest" in out:
        d = out["digest"]
        print(f"\n--- digest ({d['source']}, verified={d['verified']}, posted={d['posted']}) ---\n{d['text']}")


if __name__ == "__main__":
    main()
