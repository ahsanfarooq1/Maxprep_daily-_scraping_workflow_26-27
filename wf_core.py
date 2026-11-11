"""
wf_core.py — shared helpers for the date-driven Gap Finder workflow.

Everything in this folder is NEW code. The existing pipeline (app.py,
scrape_box_scores.py, APP/pipeline.py, ...) is never modified; it is only
IMPORTED, read-only, for its HTTP transport and box-score parser, so this
workflow parses games exactly the way the production pipeline does.

Where the legacy modules are looked up:
    $MAXPREPS_CORE_DIR  if set, otherwise the parent of this folder.
"""

import os
import re
import sys
import time
import datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
# legacy scraper modules: env override, else the bundled ./core copy, else the parent folder (old repo layout)
_BUNDLED = os.path.join(HERE, "core")
CORE_DIR = (os.environ.get("MAXPREPS_CORE_DIR")
            or (_BUNDLED if os.path.exists(os.path.join(_BUNDLED, "scrape_box_scores.py")) else os.path.dirname(HERE)))

# ─── Statuses ────────────────────────────────────────────────────────────────
PENDING = "PENDING"    # identified, not scraped yet
SUCCESS = "SUCCESS"    # required data retrieved
NO_DATA = "NO_DATA"    # page reachable, but no stats published (yet)
FAILED = "FAILED"      # technical failure (HTTP / network / parse error)
RETRY = "RETRY"        # explicitly queued for another look (see gap_store)
STATUSES = (PENDING, SUCCESS, NO_DATA, FAILED, RETRY)

# Which side(s) of a SUCCESS game actually have player rows.
CLS_FULL, CLS_TEAM, CLS_OPP, CLS_NONE = "full", "team_only", "opp_only", "no_data"


def log(*args):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True)


def now_str(now=None):
    return (now or dt.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")


def parse_ts(s):
    """'YYYY-MM-DD HH:MM:SS' -> datetime, or None."""
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S") if s else None
    except (ValueError, TypeError):
        return None


# ─── Dates ───────────────────────────────────────────────────────────────────
_MDY_RE = re.compile(r"/(\d{1,2})-(\d{1,2})-(\d{4})/")
_ISO_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def date_from_url(url):
    """Game date embedded in a MaxPreps game URL (.../MM-DD-YYYY/...), as an
    ISO string, or ''. Works for both the 2026 /game/{a}-vs-{b}/{date}/ shape
    and the legacy /games/{date}/... shape."""
    m = _MDY_RE.search(url or "")
    if not m:
        return ""
    try:
        return dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
    except ValueError:
        return ""


def date_from_value(v):
    """Best-effort ISO date from a schedule value ('2026-10-11T19:00:00',
    '10-11-2026', ...). '' when it isn't a date."""
    if not isinstance(v, str):
        return ""
    s = v.strip()
    m = _ISO_RE.match(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", s)
        if not m:
            return ""
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return dt.date(y, mo, d).isoformat()
    except ValueError:
        return ""


def iso_to_mdy(iso):
    """'2026-10-11' -> '10-11-2026' (the legacy gap/box-score date format)."""
    try:
        d = dt.date.fromisoformat(iso)
        return f"{d.month:02d}-{d.day:02d}-{d.year}"
    except (ValueError, TypeError):
        return ""


def today():
    """Today's date in US Central time (MaxPreps game dates are US-local), so a
    run from Pakistan at 03:00 PKT still treats 'yesterday' as the US game day.
    Falls back to the machine's local date if tz data isn't installed."""
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("America/Chicago")).date()
    except Exception:
        return dt.date.today()


def resolve_window(date=None, date_from=None, date_to=None, days_back=1, as_of=None):
    """-> (from_date, to_date) inclusive.

    Priority: explicit --date, then --date-from/--date-to, then the default of
    `days_back` days before today (default 1 = yesterday: on Oct 12 -> Oct 11).
    """
    as_of = as_of or today()
    if date:
        d = dt.date.fromisoformat(date)
        return d, d
    if date_from or date_to:
        f = dt.date.fromisoformat(date_from or date_to)
        t = dt.date.fromisoformat(date_to or date_from)
        if f > t:
            raise ValueError(f"--date-from {f} is after --date-to {t}")
        return f, t
    d = as_of - dt.timedelta(days=days_back)
    return d, d


# ─── Legacy modules (read-only) ──────────────────────────────────────────────
_legacy = {}


def legacy():
    """(scrape_box_scores, app) — imported lazily so the store/selection logic
    and its tests don't need network libraries or the parent folder."""
    if not _legacy:
        if CORE_DIR not in sys.path:
            sys.path.append(CORE_DIR)
        import scrape_box_scores as bx   # noqa: E402
        import app as gap_legacy         # noqa: E402
        _legacy["bx"], _legacy["app"] = bx, gap_legacy
    return _legacy["bx"], _legacy["app"]


def season_filename(season):
    """'2026-2027' -> '2026_2027' (matches the existing output names)."""
    return season.replace("-", "_")


def short_season(season):
    """'2026-2027' or '26-27' -> '26-27'."""
    m = re.match(r"^(?:20)?(\d{2})-(?:20)?(\d{2})$", (season or "").strip())
    return f"{m.group(1)}-{m.group(2)}" if m else season
