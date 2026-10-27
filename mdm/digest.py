"""Daily digest: Claude drafts it, code verifies every figure against the flags
table, and only a verified draft is posted. A draft that still fails after a
retry is replaced by a deterministic template, so Slack never shows a number
the data doesn't contain."""

import json
import logging
import os
import re
from collections import Counter
from datetime import date, datetime, timezone

import pandas as pd
import requests

from .config import get_secret
from .engine import Engine

log = logging.getLogger(__name__)

SEV_RANK = {"high": 0, "medium": 1, "low": 2}
UP_WORDS = re.compile(r"\b(up|rose|rise|rising|gain(ed|s)?|jump(ed|s)?|rall(y|ied)|surg(e|ed)|climb(ed)?|soar(ed)?)\b", re.I)
DOWN_WORDS = re.compile(r"\b(down|fell|fall|falling|drop(ped|s)?|slid|plung(e|ed)|declin(e|ed)|sank|tumbl(e|ed)|lost)\b", re.I)
NUM_RE = re.compile(r"(?<![\w.])[-+−]?\$?\d[\d,]*(?:\.\d+)?%?")
ISO_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
CORRECTION = re.compile(r"\b(correction|I mean|rather,|sorry|scratch that)\b|…", re.I)
MONTH_DATE = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(,\s*\d{4})?\b")

SYSTEM = """You write the daily market-data quality digest for a Slack channel read by a data operations team.

Rules:
- Use only the figures that appear in the facts JSON, copied exactly as written (same rounding, same units). Do not compute, convert, round, or estimate any new number.
- One ticker per bullet, on the same line as any figure about it. Write contracts exactly as given in "contract".
- No self-corrections or ellipses: if unsure, leave it out.
- Write dates only as YYYY-MM-DD.
- Every fact with severity "high" must be mentioned.
- If a price move has data-quality or reconciliation context, say the move is unconfirmed and why; if it was confirmed by the second vendor, say so.
- Do not mention tickers that have no facts.
- Slack mrkdwn: a one-line *bold* summary, then short bullets grouped as *Price moves*, *Data quality*, *Vendor breaks*. Under 250 words. No preamble."""


# ----------------------------------------------------------------------------- facts
def _fmt(check: str, value, threshold) -> dict:
    if value is None or pd.isna(value):
        return {}
    if check == "return_zscore":
        return {"zscore": f"{value:+.2f}", "z_threshold": f"{threshold:.1f}"}
    if check.startswith("recon_break"):
        return {"diff_bps": f"{value:.1f} bps", "threshold_bps": f"{threshold:.0f} bps"}
    if check == "put_call_parity":
        return {"violation": f"${value:.2f}", "tolerance": f"${threshold:.2f}"}
    if check == "vol_regime":
        return {"vol_ratio": f"{value:.2f}x"}
    if check in ("stale_repeat", "stale_latest"):
        return {"sessions": f"{int(value)}"}
    return {}


def build_facts(engine: Engine, cfg: dict, as_of: date) -> dict:
    flags = engine.sql(f"SELECT * FROM {engine.t('flags')} WHERE as_of = DATE '{as_of}'")
    flags["trade_date"] = pd.to_datetime(flags["trade_date"]).dt.date
    counts = Counter(flags["severity"])
    by_check = flags.groupby("check_name").size().to_dict()

    reconciled_today = set()
    if engine.exists("prices_secondary"):
        r = engine.sql(f"SELECT DISTINCT ticker FROM {engine.t('prices_secondary')} WHERE trade_date = DATE '{as_of}'")
        reconciled_today = set(r["ticker"])

    shown = flags[flags.severity.isin(["high", "medium"])].copy()
    # one line per ticker for missing days, not one per day
    md = shown[shown.check_name == "missing_day"]
    shown = shown[shown.check_name != "missing_day"]
    if not md.empty:
        agg = md.groupby("ticker").agg(n=("trade_date", "size"), first=("trade_date", "min"), last=("trade_date", "max")).reset_index()
        shown = pd.concat([shown, pd.DataFrame({
            "check_name": "missing_day", "severity": "medium", "ticker": agg.ticker,
            "trade_date": agg["last"], "contract": None, "value": agg.n.astype(float), "threshold": None,
            "detail": [f"{n} missing sessions between {a} and {b}" for n, a, b in zip(agg.n, agg["first"], agg["last"])],
        })], ignore_index=True)
    # one line per ticker for parity breaks: count and worst violation
    pp = shown[shown.check_name == "put_call_parity"]
    shown = shown[shown.check_name != "put_call_parity"]
    if not pp.empty:
        rows = []
        for t, g in pp.groupby("ticker"):
            w = g.loc[g.value.idxmax()]
            rows.append({"check_name": "put_call_parity", "severity": min(g.severity, key=SEV_RANK.get),
                         "ticker": t, "trade_date": w.trade_date, "contract": w.contract, "value": w.value,
                         "threshold": w.threshold,
                         "detail": f"{len(g)} contracts outside parity bounds; worst {w.contract}: {w.detail}"})
        shown = pd.concat([shown, pd.DataFrame(rows)], ignore_index=True)
    shown["rank"] = shown.severity.map(SEV_RANK)
    shown = shown.sort_values(["rank", "check_name", "ticker"]).head(30)

    price_checks = {"stale_latest", "stale_repeat", "missing_day", "bad_ohlc",
                    "recon_break", "recon_missing_primary", "recon_missing_secondary"}
    quality_by_ticker = flags[flags.check_name.isin(price_checks)].groupby("ticker")["check_name"].apply(lambda s: sorted(set(s))).to_dict()
    facts = []
    for i, r in enumerate(shown.itertuples(), 1):
        f = {
            "id": f"F{i}", "ticker": r.ticker, "check": r.check_name, "severity": r.severity,
            "date": str(r.trade_date), "detail": r.detail, **_fmt(r.check_name, r.value, r.threshold),
        }
        if r.contract:
            f["contract"] = r.contract
        if r.check_name in ("return_zscore", "vol_regime"):
            ctx = quality_by_ticker.get(r.ticker, [])
            f["data_quality_context"] = ctx
            f["second_vendor"] = ("confirmed" if r.ticker in reconciled_today and not ctx else
                                  "not checked today" if r.ticker not in reconciled_today else "disagrees or quality issue")
        facts.append(f)

    return {
        "as_of": str(as_of),
        "totals": {"flags": int(len(flags)), "high": int(counts.get("high", 0)),
                   "medium": int(counts.get("medium", 0)), "low": int(counts.get("low", 0)),
                   "tickers_flagged": int(flags.ticker.nunique()),
                   "tickers_reconciled_today": len(reconciled_today)},
        "counts_by_check": {k: int(v) for k, v in by_check.items()},
        "facts": facts,
        "omitted_low_severity": int(counts.get("low", 0)),
    }


# --------------------------------------------------------------------------- verify
def _norm(tok: str) -> float | None:
    t = tok.replace("−", "-").replace("$", "").replace(",", "").rstrip("%").lstrip("+")
    try:
        return round(abs(float(t)), 6)
    except ValueError:
        return None


def _nums(text: str) -> list[tuple[str, float]]:
    out = []
    for m in NUM_RE.finditer(text):
        v = _norm(m.group())
        if v is not None:
            out.append((m.group(), v))
    return out


def verify(text: str, payload: dict, watchlist: list[str], cfg: dict) -> list[str]:
    errors = []
    facts = payload["facts"]
    tickers_with_facts = {f["ticker"] for f in facts}
    allowed_by_ticker: dict[str, set] = {}
    allowed_dates = {payload["as_of"]}
    for f in facts:
        s = allowed_by_ticker.setdefault(f["ticker"], set())
        for k, v in f.items():
            if k in ("id", "data_quality_context", "second_vendor"):
                continue
            vs = str(v)
            allowed_dates.update(ISO_DATE.findall(vs))
            for _, n in _nums(ISO_DATE.sub(" ", vs)):
                s.add(n)
    glob = {float(v) for v in payload["totals"].values()}
    glob |= {float(v) for v in payload["counts_by_check"].values()}
    glob.add(float(payload["omitted_low_severity"]))
    ac = cfg["anomalies"]
    glob |= {float(ac["vol_window"]), float(ac["long_window"]), float(cfg["reconciliation"]["warn_bps"])}
    all_fact_nums = set().union(*allowed_by_ticker.values()) if allowed_by_ticker else set()

    wl = sorted(watchlist, key=len, reverse=True)
    # strict boundaries: single-letter tickers (V, T) must not match inside words like "Vendor"
    tick_re = re.compile(r"(?<![A-Za-z0-9])(" + "|".join(map(re.escape, wl)) + r")(?![A-Za-z0-9])")

    contracts = {f["contract"] for f in facts if f.get("contract")}
    contract_re = re.compile(r"\b(" + "|".join(map(re.escape, wl)) + r") \d{4}-\d{2}-\d{2}(?: [CP])? K=[\d.]+")

    for ln, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        if CORRECTION.search(line):
            errors.append(f"line {ln}: contains a self-correction or ellipsis")
        for m in contract_re.finditer(line):
            if m.group() not in contracts:
                errors.append(f"line {ln}: contract '{m.group()}' not in the data")
        for d in ISO_DATE.findall(line):
            if d not in allowed_dates:
                errors.append(f"line {ln}: date {d} not in data")
        if MONTH_DATE.search(line):
            errors.append(f"line {ln}: non-ISO date '{MONTH_DATE.search(line).group()}'")
        body = MONTH_DATE.sub(" ", ISO_DATE.sub(" ", line))
        mentioned = set(tick_re.findall(body))
        for t in mentioned - tickers_with_facts:
            errors.append(f"line {ln}: mentions {t}, which has no flags")
        if len(mentioned) > 1 and _nums(tick_re.sub(" ", body)):
            errors.append(f"line {ln}: figures on a line naming several tickers ({', '.join(sorted(mentioned))})")
        allowed = glob | (set().union(*(allowed_by_ticker.get(t, set()) for t in mentioned)) if mentioned else all_fact_nums)
        for raw, v in _nums(tick_re.sub(" ", body)):
            if v not in allowed:
                errors.append(f"line {ln}: figure '{raw}' not found in the data" +
                              (f" for {', '.join(sorted(mentioned))}" if mentioned else ""))
        # direction: a move described as up must be positive, and vice versa
        for f in facts:
            if f["check"] != "return_zscore" or f["ticker"] not in mentioned:
                continue
            neg = f["zscore"].startswith("-")
            if neg and UP_WORDS.search(body) and not DOWN_WORDS.search(body):
                errors.append(f"line {ln}: describes {f['ticker']} as up, but its return is negative")
            if not neg and DOWN_WORDS.search(body) and not UP_WORDS.search(body):
                errors.append(f"line {ln}: describes {f['ticker']} as down, but its return is positive")

    all_mentioned = set(tick_re.findall(text))
    for f in facts:
        if f["severity"] == "high" and f["ticker"] not in all_mentioned:
            errors.append(f"high-severity {f['check']} on {f['ticker']} ({f['id']}) not mentioned")
    return sorted(set(errors), key=errors.index)


# ---------------------------------------------------------------------- generation
def template_digest(payload: dict) -> str:
    t = payload["totals"]
    lines = [f"*Market data digest {payload['as_of']}*: {t['flags']} flags "
             f"({t['high']} high, {t['medium']} medium, {t['low']} low) across {t['tickers_flagged']} tickers"]
    sections = {"Price moves": ("return_zscore", "vol_regime"),
                "Vendor breaks": ("recon_break", "recon_missing_primary", "recon_missing_secondary")}
    grouped: dict[str, list] = {"Price moves": [], "Data quality": [], "Vendor breaks": []}
    for f in payload["facts"]:
        sec = next((s for s, cs in sections.items() if f["check"] in cs), "Data quality")
        extra = ""
        if f["check"] == "return_zscore":
            extra = f" (second vendor: {f['second_vendor']})"
        grouped[sec].append(f"• {f['ticker']} [{f['severity']}] {f['check']}: {f['detail']}{extra}")
    for sec, items in grouped.items():
        if items:
            lines += ["", f"*{sec}*", *items]
    return "\n".join(lines)


def llm_digest(payload: dict, cfg: dict, client=None) -> tuple[str | None, list[str], int]:
    """Returns (text or None, last verification errors, attempts)."""
    import anthropic

    dc = cfg["digest"]
    client = client or anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))
    messages = [{"role": "user", "content": "Facts JSON:\n" + json.dumps(payload, indent=1)}]
    errors: list[str] = []
    for attempt in range(1, dc["max_retries"] + 2):
        resp = client.beta.messages.create(
            model=dc["model"], max_tokens=4000, system=SYSTEM, messages=messages,
            output_config={"effort": dc["effort"]},
            # Server-side refusal fallback: reroutes to another model if declined.
            betas=["server-side-fallback-2026-07-01"], fallbacks="default",
        )
        if resp.stop_reason == "refusal":
            return None, ["model refused"], attempt
        text = "".join(b.text for b in resp.content if b.type == "text").strip()
        errors = verify(text, payload, cfg["watchlist"], cfg)
        if not errors:
            return text, [], attempt
        log.warning("digest attempt %d failed verification: %s", attempt, errors)
        messages += [
            {"role": "assistant", "content": resp.content},
            {"role": "user", "content": "Your draft failed the figure check:\n- " + "\n- ".join(errors) +
             "\nRewrite it, using only figures exactly as they appear in the facts JSON."},
        ]
    return None, errors, attempt


def post_slack(text: str, webhook: str) -> None:
    r = requests.post(webhook, json={"text": text}, timeout=15)
    r.raise_for_status()


def run_digest(engine: Engine, cfg: dict, as_of: date, use_llm: bool = True, post: bool = True, client=None) -> dict:
    payload = build_facts(engine, cfg, as_of)
    text, errors, attempts, source = None, [], 0, "template"
    if use_llm and (client or get_secret("ANTHROPIC_API_KEY")):
        try:
            text, errors, attempts = llm_digest(payload, cfg, client)
            source = "llm" if text else "template"
        except Exception as e:  # never let the LLM step block the digest
            log.exception("LLM digest failed")
            errors = [f"LLM call failed: {type(e).__name__}: {e}"]
    if text is None:
        text = template_digest(payload)
        if errors:
            text += "\n\n_(LLM draft rejected by figure check; template digest shown.)_"

    webhook = get_secret("SLACK_WEBHOOK_URL")
    posted = False
    already = engine.exists("digests") and not engine.sql(
        f"SELECT 1 FROM {engine.t('digests')} WHERE as_of = DATE '{as_of}' AND posted").empty
    if post and already:
        log.info("digest for %s already posted; not reposting", as_of)
        post, posted = False, True
    if post and webhook:
        post_slack(text, webhook)
        posted = True
    elif post:
        log.info("SLACK_WEBHOOK_URL not set; digest not posted")

    row = {"as_of": as_of, "source": source, "model": cfg["digest"]["model"] if source == "llm" else None,
           "attempts": attempts, "verified": source == "template" or not errors,
           "rejected_errors": json.dumps(errors), "posted": posted, "text": text,
           "facts_json": json.dumps(payload), "created_at": datetime.now(timezone.utc).replace(tzinfo=None)}
    engine.write(pd.DataFrame([row]), "digests", f"as_of = DATE '{as_of}'")
    return row
