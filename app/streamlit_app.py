"""Market Data Monitor dashboard.

    streamlit run app/streamlit_app.py

Reads the local DuckDB file by default. Set DATABRICKS_HOST, DATABRICKS_HTTP_PATH
and DATABRICKS_TOKEN to read the same tables from a Databricks SQL warehouse.
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from mdm.config import load_config  # noqa: E402

CFG = load_config()
SEV_COLOR = {"high": "#d03b3b", "medium": "#ec835a", "low": "#898781"}  # status palette
SEV_SYMBOL = {"high": "x", "medium": "diamond", "low": "circle-open"}
SERIES = "#2a78d6"

st.set_page_config(page_title="Market Data Monitor", layout="wide")


@st.cache_resource
def _conn():
    if os.environ.get("DATABRICKS_HOST"):
        from databricks import sql  # type: ignore

        c = sql.connect(server_hostname=os.environ["DATABRICKS_HOST"],
                        http_path=os.environ["DATABRICKS_HTTP_PATH"],
                        access_token=os.environ["DATABRICKS_TOKEN"])
        return "databricks", c, CFG["storage"]["spark_namespace"]
    import duckdb

    path = ROOT / CFG["storage"]["duckdb_path"]
    if not path.exists():
        st.error(f"No database at {path}. Run `python -m mdm.pipeline --demo` first.")
        st.stop()
    return "duckdb", duckdb.connect(str(path), read_only=True), CFG["storage"]["duckdb_schema"]


def q(sql: str) -> pd.DataFrame:
    kind, c, ns = _conn()
    sql = sql.replace("{ns}", ns)
    if kind == "duckdb":
        return c.execute(sql).df()
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall_arrow().to_pandas()


@st.cache_data(ttl=300)
def load(sql: str) -> pd.DataFrame:
    return q(sql)


def style(fig: go.Figure, height=320) -> go.Figure:
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=30, b=10),
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      legend=dict(orientation="h", y=1.08, x=0), hovermode="x unified")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(137,135,129,0.2)", zeroline=False)
    return fig


# ------------------------------------------------------------------ header + filters
dates = load("SELECT DISTINCT as_of FROM {ns}.flags ORDER BY as_of DESC")
if dates.empty:
    st.info("No flags yet. Run the pipeline first.")
    st.stop()
c1, c2, c3 = st.columns([1, 1, 2])
as_of = c1.selectbox("Session", pd.to_datetime(dates.as_of).dt.date.tolist())
sev = c2.multiselect("Severity", ["high", "medium", "low"], default=["high", "medium"])
st.title(f"Market data monitor · {as_of}")

flags = load(f"SELECT * FROM {{ns}}.flags WHERE as_of = DATE '{as_of}'")
flags["trade_date"] = pd.to_datetime(flags["trade_date"]).dt.date
digest = load(f"SELECT * FROM {{ns}}.digests WHERE as_of = DATE '{as_of}'")
ingest = load(f"SELECT * FROM {{ns}}.ingest_log WHERE as_of = DATE '{as_of}'")

k = st.columns(5)
k[0].metric("High-severity flags", int((flags.severity == "high").sum()))
k[1].metric("All flags", len(flags))
k[2].metric("Tickers flagged", flags.ticker.nunique(), help=f"of {len(CFG['watchlist'])} on the watchlist")
recon_today = ingest.loc[ingest.dataset.eq("prices") & ~ingest.source.isin(["yahoo", "synthetic"]), "returned"]
k[3].metric("Reconciled today", int(recon_today.sum()) if len(recon_today) else 0,
            help="Second vendor is rotated across the watchlist (free-tier quota)")
if digest.empty:
    k[4].metric("Digest", "not run")
else:
    d = digest.iloc[0]
    k[4].metric("Digest posted", "yes" if d.posted else "no",
                help=f"source: {d.source}. LLM drafts are posted only if every figure matches the flags table")

tabs = st.tabs(["Flags", "Ticker", "Vendor reconciliation", "Options", "Digest", "Detector accuracy"])

# --------------------------------------------------------------------------- flags
with tabs[0]:
    f = flags[flags.severity.isin(sev)]
    by = (f.groupby(["stage", "check_name", "severity"]).size().rename("flags").reset_index()
          .sort_values("flags", ascending=False))
    left, right = st.columns([1, 2])
    left.dataframe(by, hide_index=True, use_container_width=True)
    stages = right.multiselect("Stage", sorted(f.stage.unique()), default=sorted(f.stage.unique()))
    right.dataframe(
        f[f.stage.isin(stages)][["severity", "check_name", "ticker", "trade_date", "contract", "value", "threshold", "detail"]]
        .sort_values(["severity", "check_name"], key=lambda s: s.map({"high": 0, "medium": 1, "low": 2}) if s.name == "severity" else s),
        hide_index=True, use_container_width=True, height=420)

# -------------------------------------------------------------------------- ticker
with tabs[1]:
    flagged = sorted(flags.ticker.unique())
    t = st.selectbox("Ticker", flagged + [x for x in CFG["watchlist"] if x not in flagged])
    sig = load(f"""SELECT trade_date, adj_close, ret, zscore, vol_short, vol_long FROM {{ns}}.signals
                   WHERE ticker = '{t}' AND trade_date <= DATE '{as_of}' ORDER BY trade_date""")
    raw = load(f"""SELECT trade_date, close FROM {{ns}}.prices_primary
                   WHERE ticker = '{t}' AND trade_date <= DATE '{as_of}' ORDER BY trade_date""")
    sig["trade_date"] = pd.to_datetime(sig.trade_date)
    raw["trade_date"] = pd.to_datetime(raw.trade_date)
    sig, raw = sig.tail(120), raw.tail(120)
    tf = flags[(flags.ticker == t) & flags.stage.isin(["quality", "reconciliation", "anomaly"])]
    tf = tf.assign(trade_date=pd.to_datetime(tf.trade_date)).merge(raw, on="trade_date", how="left")

    fig = go.Figure()
    fig.add_scatter(x=raw.trade_date, y=raw.close, name="close (primary)", line=dict(color=SERIES, width=2))
    for s_, g in tf.dropna(subset=["close"]).groupby("severity"):
        fig.add_scatter(x=g.trade_date, y=g.close, mode="markers", name=f"{s_} flag",
                        marker=dict(color=SEV_COLOR[s_], symbol=SEV_SYMBOL[s_], size=11,
                                    line=dict(width=2, color=SEV_COLOR[s_])),
                        text=g.check_name + ": " + g.detail, hovertemplate="%{text}<extra></extra>")
    st.plotly_chart(style(fig).update_layout(title=f"{t} close with flagged sessions"), use_container_width=True)

    a, b = st.columns(2)
    zf = go.Figure(go.Bar(x=sig.trade_date, y=sig.zscore, marker_color=SERIES, name="z-score",
                          hovertemplate="%{x|%Y-%m-%d}: z=%{y:.2f}<extra></extra>"))
    zt = CFG["anomalies"]["z_threshold"]
    for y in (zt, -zt):
        zf.add_hline(y=y, line=dict(color="#898781", dash="dot", width=1))
    a.plotly_chart(style(zf, 260).update_layout(title=f"Return z-score (±{zt} threshold)", hovermode="closest"),
                   use_container_width=True)
    vf = go.Figure()
    vf.add_scatter(x=sig.trade_date, y=sig.vol_short * 100, name=f"{CFG['anomalies']['vol_window']}d vol",
                   line=dict(color=SERIES, width=2))
    vf.add_scatter(x=sig.trade_date, y=sig.vol_long * 100, name=f"{CFG['anomalies']['long_window']}d vol",
                   line=dict(color="#eb6834", width=2))
    b.plotly_chart(style(vf, 260).update_layout(title="Annualized volatility (%)"), use_container_width=True)
    if sig.empty:
        st.caption("No signals: this ticker has no clean history yet.")

# ------------------------------------------------------------------ reconciliation
with tabs[2]:
    cov = load(f"""SELECT p.ticker, max(s.trade_date) AS last_reconciled
                   FROM {{ns}}.watchlist p LEFT JOIN {{ns}}.prices_secondary s
                     ON s.ticker = p.ticker AND s.trade_date <= DATE '{as_of}'
                   GROUP BY p.ticker ORDER BY last_reconciled NULLS FIRST""")
    cov["last_reconciled"] = pd.to_datetime(cov.last_reconciled).dt.date
    rb = flags[(flags.stage == "reconciliation") & flags.severity.isin(sev)]
    # split-adjustment mismatches are explained (low severity); keep them out of the histogram
    rb_hist = rb[(rb.check_name == "recon_break") & (rb.severity != "low")]
    a, b = st.columns([2, 1])
    if not rb.empty:
        h = go.Figure(go.Histogram(x=rb_hist.value.dropna(), nbinsx=30, marker_color=SERIES,
                                   marker_line=dict(color="white", width=2), name="breaks"))
        h.add_vline(x=CFG["reconciliation"]["warn_bps"], line=dict(color="#ec835a", dash="dot"))
        h.add_vline(x=CFG["reconciliation"]["break_bps"], line=dict(color="#d03b3b", dash="dot"))
        a.plotly_chart(style(h).update_layout(title="Primary vs secondary close difference (bps), flagged rows",
                                              hovermode="closest", bargap=0.05), use_container_width=True)
        a.dataframe(rb[["severity", "check_name", "ticker", "trade_date", "value", "detail"]]
                    .rename(columns={"value": "diff_bps"}), hide_index=True, use_container_width=True)
    else:
        a.success("No vendor breaks in the reconciliation window.")
    b.caption("Last date each ticker was checked against the second vendor")
    b.dataframe(cov[["ticker", "last_reconciled"]], hide_index=True, use_container_width=True, height=460)

# ------------------------------------------------------------------------- options
with tabs[3]:
    of = flags[flags.severity.isin(sev) & flags.check_name.isin(["put_call_parity", "crossed_quote", "locked_quote",
                                      "one_sided_quote", "stale_quote"])]
    snap = load(f"SELECT ticker, count(*) AS quotes FROM {{ns}}.options_quotes WHERE snapshot_date = DATE '{as_of}' GROUP BY ticker")
    s1, s2 = st.columns([1, 3])
    s1.caption("Quotes in today's snapshot")
    s1.dataframe(snap, hide_index=True, use_container_width=True)
    s2.dataframe(of[["severity", "check_name", "ticker", "contract", "value", "threshold", "detail"]]
                 .rename(columns={"value": "violation", "threshold": "tolerance"}),
                 hide_index=True, use_container_width=True, height=420)
    s2.caption("Parity uses American bounds S·e^(−qT) − K ≤ C − P ≤ S − K·e^(−rT) on mid prices; "
               "tolerance = both half-spreads + $%.2f + %.1f%% of spot. Parity breaks on a chain already "
               "flagged stale are demoted to low." % (CFG["checks"]["parity"]["abs_tolerance"],
                                                      CFG["checks"]["parity"]["spot_tolerance_pct"] * 100))

# -------------------------------------------------------------------------- digest
with tabs[4]:
    if digest.empty:
        st.info("No digest for this session.")
    else:
        d = digest.iloc[0]
        st.caption(f"source: {d.source} · attempts: {d.attempts} · verified: {d.verified} · posted: {d.posted}")
        st.markdown(d.text.replace("•", "-"))
        errs = json.loads(d.rejected_errors or "[]")
        if errs:
            st.warning("LLM draft rejected by the figure check:\n\n" + "\n".join(f"- {e}" for e in errs))
        with st.expander("Facts the digest was allowed to use"):
            st.json(json.loads(d.facts_json))

# ------------------------------------------------------------------------ accuracy
with tabs[5]:
    p = ROOT / "data" / "harness_results.json"
    if not p.exists():
        st.info("Run `python -m mdm.harness` to measure detection recall and false positives.")
    else:
        r = json.loads(p.read_text())
        s = r["summary"]
        m = st.columns(4)
        m[0].metric("Recall, faults at/above threshold", f"{s['recall_in_spec']:.1%}", help=f"{s['in_spec']} faults")
        m[1].metric("Recall, all planted faults", f"{s['recall_all']:.1%}", help=f"{s['faults_planted']} faults, incl. sub-threshold")
        m[2].metric("False flags per clean run", f"{s['clean_fp_per_run']:.1f}", help=f"{s['clean_runs']} clean runs, 50 tickers each")
        m[3].metric("Clean-run false flags", s["clean_false_positives"])
        bk = pd.DataFrame(r["by_kind"])
        bk["recall_in_spec"] = (bk.recall_in_spec * 100).round(1)
        st.dataframe(bk, hide_index=True, use_container_width=True)
        st.caption("Synthetic market. False-positive rates depend on the simulated vendor noise "
                   "(Student-t, ~2 bps scale) and will differ on real data.")
        st.write("False flags on clean runs by check:", s["clean_fp_by_check"])
