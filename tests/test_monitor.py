import json
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlglot

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mdm.calendar import sessions  # noqa: E402
from mdm.config import load_config  # noqa: E402
from mdm.digest import build_facts, run_digest, verify  # noqa: E402
from mdm.engine import SQL_DIR, DuckDBEngine, render_sql  # noqa: E402
from mdm.ingest import rotation  # noqa: E402
from mdm.pipeline import STEPS, build_sources, run  # noqa: E402
from mdm.sources import YahooSource  # noqa: E402

AS_OF = date(2026, 10, 23)
FAULT_TO_CHECK = {
    "big_move": "return_zscore", "stale": "stale_repeat", "missing_day": "missing_day",
    "bad_ohlc": "bad_ohlc", "recon_break": "recon_break", "split_mismatch": "recon_break",
    "crossed": "crossed_quote", "parity": "put_call_parity", "stale_quote": "stale_quote",
}


@pytest.fixture(scope="module")
def demo():
    cfg = load_config()
    eng = DuckDBEngine(":memory:", "mm")
    srcs = build_sources(cfg, AS_OF, True, market_end=AS_OF)
    prev = sessions(AS_OF - timedelta(days=10), AS_OF)[-2]
    run(STEPS[:-1], cfg, prev, True, engine=eng, sources=srcs)
    run(STEPS[:-1], cfg, AS_OF, True, engine=eng, sources=srcs)
    flags = eng.sql(f"SELECT * FROM mm.flags WHERE as_of = DATE '{AS_OF}'")
    return cfg, eng, srcs[0], flags


def test_every_planted_fault_is_flagged(demo):
    _, _, syn, flags = demo
    latest = {(f["kind"], f["ticker"]) for f in syn.faults}
    assert len(latest) == len(FAULT_TO_CHECK)
    for kind, ticker in latest:
        hit = flags[(flags.ticker == ticker) & (flags.check_name == FAULT_TO_CHECK[kind])]
        assert not hit.empty, f"{kind} on {ticker} not flagged"


def test_reruns_are_idempotent(demo):
    cfg, eng, _, flags = demo
    run(["quality", "reconcile", "anomalies"], cfg, AS_OF, True, engine=eng)
    again = eng.sql(f"SELECT * FROM mm.flags WHERE as_of = DATE '{AS_OF}'")
    assert len(again) == len(flags)


def test_prices_clean_excludes_suspect_bars(demo):
    _, eng, syn, _ = demo
    stale = next(f for f in syn.faults if f["kind"] == "stale")
    rows = eng.sql(f"SELECT * FROM mm.prices_clean WHERE ticker = '{stale['ticker']}' "
                   f"AND trade_date = DATE '{stale['date']}'")
    assert rows.empty


def _payload(demo):
    cfg, eng, _, _ = demo
    return cfg, build_facts(eng, cfg, AS_OF)


def test_verify_accepts_template_figures(demo):
    from mdm.digest import template_digest

    cfg, p = _payload(demo)
    assert verify(template_digest(p), p, cfg["watchlist"], cfg) == []


def test_verify_rejects_bad_drafts(demo):
    cfg, p = _payload(demo)
    move = next(f for f in p["facts"] if f["check"] == "return_zscore")
    t = move["ticker"]
    highs = " ".join(sorted({f["ticker"] for f in p["facts"] if f["severity"] == "high"}))
    base = f"*Digest {p['as_of']}* covering {highs}\n"
    assert any("not found" in e for e in verify(base + f"• {t} fell 9.81%, z=-6.52", p, cfg["watchlist"], cfg))
    assert any("as up" in e for e in verify(base + f"• {t} rallied {move['detail'].split()[1].rstrip(',')}", p, cfg["watchlist"], cfg))
    assert any("no flags" in e for e in verify(base + "• KO looks fine", p, cfg["watchlist"], cfg))
    assert any("not mentioned" in e for e in verify(f"*Digest {p['as_of']}*", p, cfg["watchlist"], cfg))
    assert any("non-ISO" in e for e in verify(base + "• as of Oct 23", p, cfg["watchlist"], cfg))
    assert any("self-correction" in e for e in verify(base + f"• {t} is… correction: fine", p, cfg["watchlist"], cfg))
    assert any("not in the data" in e for e in verify(base + f"• {t} 2026-12-31 K=999.0 broke parity", p, cfg["watchlist"], cfg))
    assert any("several tickers" in e for e in verify(base + f"• {t} and MSFT z=-6.52", p, cfg["watchlist"], cfg))


class FakeClient:
    """Stands in for anthropic.Anthropic; returns scripted drafts."""

    def __init__(self, drafts):
        self.drafts, self.calls = list(drafts), []
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self._create))

    def _create(self, **kw):
        self.calls.append(kw)
        text = self.drafts.pop(0)
        return SimpleNamespace(stop_reason="end_turn", content=[SimpleNamespace(type="text", text=text)])


def test_llm_draft_retried_then_posted_or_replaced(demo, monkeypatch):
    from mdm.digest import template_digest

    cfg, eng, _, _ = demo
    p = build_facts(eng, cfg, AS_OF)
    good = template_digest(p)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    c = FakeClient([good.replace(p["as_of"], "2026-10-22", 1), good])
    row = run_digest(eng, cfg, AS_OF, post=False, client=c)
    assert row["source"] == "llm" and row["verified"] and row["attempts"] == 2
    assert "failed the figure check" in json.dumps(c.calls[1]["messages"][-1]["content"])

    c = FakeClient(["• AAPL down 99.99%"] * 2)
    row = run_digest(eng, cfg, AS_OF, post=False, client=c)
    assert row["source"] == "template" and "rejected" in row["text"]


def test_sql_parses_as_spark():
    """Same SQL runs on Databricks; this catches DuckDB-only syntax (parse only)."""
    params = dict(ns="main.market_monitor", as_of="2026-10-23", window_start="2026-08-01",
                  repeat_days=2, min_t=0.02, m_lo=0.85, m_hi=1.15, min_oi=10, abs_tol=0.05,
                  spot_tol=0.002, warn_bps=10, break_bps=50, min_spot_move=0.01, min_share=0.25)
    for f in sorted(SQL_DIR.glob("*.sql")):
        for stmt in render_sql(f.read_text(), **params).split(";"):
            if stmt.strip():
                sqlglot.parse_one(stmt, read="spark")


def test_rotation_covers_watchlist_in_three_sessions():
    cfg = load_config()
    days = sessions(AS_OF - timedelta(days=10), AS_OF)[-3:]
    covered = set().union(*(rotation(cfg["watchlist"], d, 24) for d in days))
    assert covered == set(cfg["watchlist"])


def test_expiries_skip_dailies():
    exps = [(AS_OF + timedelta(days=d)).isoformat() for d in (1, 2, 3, 6, 13, 27, 34, 62, 90)]
    got = YahooSource.pick_expiries(exps, AS_OF, [14, 30, 60, 90])
    assert [(date.fromisoformat(e) - AS_OF).days for e in got] == [13, 27, 62, 90]
