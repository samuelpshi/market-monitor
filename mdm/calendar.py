from datetime import date, datetime, timedelta

import pandas as pd


def _xnys():
    import exchange_calendars as xc

    return xc.get_calendar("XNYS")


def sessions(start: date, end: date) -> list[date]:
    try:
        idx = _xnys().sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
        return [d.date() for d in idx]
    except ImportError:
        # Fallback: weekdays only. Holidays will show up as false "missing days".
        return [d.date() for d in pd.bdate_range(start, end)]


def last_completed_session(now: datetime | None = None) -> date:
    """Latest NYSE session whose close has passed. Running before the close
    would otherwise ingest a partial intraday bar as if it were the daily close."""
    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    try:
        cal = _xnys()
        for d in reversed(sessions(now.date() - timedelta(days=10), now.date())):
            if cal.session_close(pd.Timestamp(d)) <= now:
                return d
    except ImportError:
        pass
    d = now.tz_convert("America/New_York")
    day = d.date() if d.hour >= 17 else d.date() - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def calendar_frame(start: date, end: date) -> pd.DataFrame:
    days = sessions(start, end)
    return pd.DataFrame({"trade_date": days, "session_idx": range(len(days))})
