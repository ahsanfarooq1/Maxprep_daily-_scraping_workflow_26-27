"""
HS Basketball — Box Score Scraper
==================================
Input:  [state]_data_gaps.json    (produced by texas_data_gap_finder.py)
Output: [state]_box_scores.json

Scrapes every scheduled game box score for all teams classified as
'full' or 'partial' in the input file.

Uses the same boxscore.aspx?contestid={guid}&ssid={ssid} method as
texas_data_gap_finder.py — a plain HTTP request, no JS rendering needed.

Games are deduplicated by contest GUID so that if two Texas teams play
each other, the game is only fetched once.

Output is compatible with Accumulation_data.py.
"""

import os
import re
import sys
import json
import time
import base64
import struct
import argparse
import shutil
import threading
import subprocess
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# curl_cffi impersonates a real Chrome TLS handshake. Optional: everything
# degrades to system curl and then plain requests if it isn't installed.
try:
    from curl_cffi import requests as cffi_requests
    _CFFI_AVAILABLE = True
except ImportError:            # pragma: no cover - depends on environment
    cffi_requests = None
    _CFFI_AVAILABLE = False

# Windows consoles default to cp1252, which raises on the ✓/─ characters in
# our log lines and on non-ASCII player names.
if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

# ── Config ────────────────────────────────────────────────────────────────────

INPUT_FILE   = "texas_data_gaps.json"
OUTPUT_FILE  = "texas_box_scores.json"
DELAY        = 0.6          # seconds between HTTP requests (per worker thread)
TEAM_WORKERS = 15           # parallel teams (each thread scrapes its team's games sequentially)

# When refresh_build_id returns the SAME bid (i.e. MaxPreps hasn't rolled yet),
# wait this long before checking again, up to BID_STABLE_MAX_RETRIES times.
BID_STABLE_WAIT_SEC   = 15 * 60   # 15 minutes
BID_STABLE_MAX_RETRIES = 10        # ~2.5 hours total before giving up on a team

# Timestamped print: every log line gets a "[YYYY-MM-DD HH:MM:SS]" prefix so the
# Streamlit log viewer and stdout show live timing.
_original_print = print
def print(*args, **kwargs):
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)

# MaxPreps serves a 403 "Geo-block" page to requests from some countries.
# Diagnosed 2026-08-13: this is genuinely GEOGRAPHIC, not bot-detection —
# a VPN with an allowed exit IP fixes it for both browsers and scripts,
# while without one even a real headless Chrome is blocked. So run this
# scraper behind a VPN/proxy in an allowed region; no amount of header or
# TLS tuning substitutes for that.
#
# Keep this header set MINIMAL. Adding Chromium client-hints / Fetch Metadata
# headers (sec-ch-ua*, sec-fetch-*) made MaxPreps reject every request with
# 406 Not Acceptable — verified from a US-hosted CI runner, so it was not the
# geo-block. `sec-fetch-site: same-origin` is in fact wrong for a direct
# top-level navigation (a real browser sends `none`), and inconsistent
# Sec-Fetch metadata is exactly what a WAF flags. The four headers below are
# what has been observed working; don't "improve" them without re-running
# .github/workflows/verify-boxscore.yml.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.maxpreps.com/",
}

HTML_HEADERS = {**HEADERS, "Accept": "text/html,application/xhtml+xml,*/*"}

# ── Session with automatic retry on connection drops ─────────────────────────
# The transport-level Retry matters: scrape_game() returns None on any non-200,
# and its caller only retries on raised connection/timeout errors — so without
# this adapter a 429/5xx would silently drop that game for good.

def _make_session():
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=2.0,          # waits 2 s, 4 s, 8 s, 16 s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update(HEADERS)
    return s

# Thread-local sessions: a fresh Session per thread avoids any contention on
# the connection pool and matches the pattern used by app.py.
_tls = threading.local()

def _get_session():
    """Plain-requests session. Kept as the last-resort transport and because
    the diagnostic scripts import it directly."""
    if not hasattr(_tls, "session"):
        _tls.session = _make_session()
    return _tls.session


# ── HTTP transport: curl_cffi → system curl → plain requests ─────────────────
# MaxPreps has been observed rejecting plain-`requests` traffic with 406 Not
# Acceptable in some environments (its TLS fingerprint is recognisable), while
# accepting the same request from Chrome. Verified 2026-08-19 from a US VPN
# exit that ALL THREE backends work here — so this is defence in depth, not a
# workaround for a live failure: whichever backend the host can offer, one of
# them looks enough like a browser to be served.
#
# Order matters: curl_cffi impersonates Chrome's TLS in-process (fastest and
# most browser-like), system curl is a dependency-free backup, and plain
# requests is the final fallback so the scraper still runs on a host with
# neither.

CURL_IMPERSONATE = "chrome124"   # keep aligned with the User-Agent above


def _http_backend_label():
    parts = []
    if _CFFI_AVAILABLE:
        parts.append(f"curl_cffi({CURL_IMPERSONATE})")
    if shutil.which("curl"):
        parts.append("system-curl")
    parts.append("requests")
    return " → ".join(parts)


def _get_cffi_session(kind="html"):
    """Thread-local curl_cffi session; kind is 'html' or 'json'."""
    attr = f"cffi_{kind}"
    if not hasattr(_tls, attr):
        if not _CFFI_AVAILABLE:
            setattr(_tls, attr, None)
        else:
            s = cffi_requests.Session(impersonate=CURL_IMPERSONATE)
            s.headers.update(HTML_HEADERS if kind == "html" else HEADERS)
            setattr(_tls, attr, s)
    return getattr(_tls, attr)


def _fetch_via_cffi(url, timeout=25, allow_redirects=True, kind="html",
                    extra_headers=None):
    """Chrome-impersonating in-process GET → (status, text, final_url)."""
    session = _get_cffi_session(kind)
    if session is None:
        return None, None, None
    try:
        r = session.get(url, timeout=timeout, allow_redirects=allow_redirects,
                        headers=extra_headers)
        return r.status_code, (r.text if r.status_code == 200 else None), r.url
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        raise
    except Exception as e:
        print(f"    [WARN] curl_cffi fetch failed: {e}")
        return None, None, None


def _fetch_via_curl(url, timeout=30, kind="html", extra_headers=None):
    """System-curl GET → (status, body, final_url). (None,None,None) if absent."""
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return None, None, None
    hdrs = dict(HTML_HEADERS if kind == "html" else HEADERS)
    if extra_headers:
        hdrs.update(extra_headers)
    marker = "\n__CURL_META__"
    cmd = [curl_bin, "-sL"]
    for key, val in hdrs.items():
        cmd.extend(["-H", f"{key}: {val}"])
    cmd.extend(["-w", marker + "%{http_code}__URL__%{url_effective}",
                "--max-time", str(timeout), url])
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              encoding="utf-8", errors="replace",
                              timeout=timeout + 10)
        out = proc.stdout or ""
        if marker not in out:
            return None, None, None
        body, meta = out.rsplit(marker, 1)
        if "__URL__" not in meta:
            return None, None, None
        code_raw, final_url = meta.split("__URL__", 1)
        return int(code_raw.strip()), body, final_url.strip()
    except Exception as e:
        print(f"    [WARN] curl fetch failed: {e}")
        return None, None, None


def _fetch_via_requests(url, timeout=25, allow_redirects=True, kind="html",
                        extra_headers=None):
    """Plain-requests GET → (status, text, final_url)."""
    hdrs = dict(HTML_HEADERS if kind == "html" else HEADERS)
    if extra_headers:
        hdrs.update(extra_headers)
    try:
        r = _get_session().get(url, headers=hdrs, timeout=timeout,
                               allow_redirects=allow_redirects)
        return r.status_code, (r.text if r.status_code == 200 else None), r.url
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        raise
    except Exception as e:
        print(f"    [WARN] requests fetch failed: {e}")
        return None, None, None


def _http_get(url, timeout=25, allow_redirects=True, kind="html",
              extra_headers=None):
    """GET a MaxPreps URL, trying each transport in turn.

    Returns (status, text, final_url); text is None unless status == 200.
    A 403/404 from the first backend is returned immediately rather than
    retried — those are real answers (geo-block / missing page), not
    fingerprint rejections, so retrying just multiplies the request count.
    """
    last = (None, None, url)
    for fetch in (_fetch_via_cffi, _fetch_via_curl, _fetch_via_requests):
        if fetch is _fetch_via_cffi and not _CFFI_AVAILABLE:
            continue
        try:
            if fetch is _fetch_via_curl:
                status, text, final = fetch(url, timeout=timeout, kind=kind,
                                            extra_headers=extra_headers)
            else:
                status, text, final = fetch(url, timeout=timeout,
                                            allow_redirects=allow_redirects,
                                            kind=kind,
                                            extra_headers=extra_headers)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            raise
        if status == 200 and text:
            return 200, text, final or url
        if status in (403, 404):
            return status, None, final or url
        last = (status, None, final or url)
    return last


def _http_get_page(url, timeout=25, allow_redirects=True):
    """GET an HTML page → (status, text, final_url)."""
    return _http_get(url, timeout=timeout, allow_redirects=allow_redirects,
                     kind="html")


def _is_geo_block(status=None, body=None):
    """True when MaxPreps served its geographic-restriction page."""
    if status == 403:
        return True
    return "geo-block" in (body or "").lower()


def _geo_block_error_message():
    return (
        "MaxPreps returned 403 Geo-block — this IP is outside an allowed "
        "region (common outside the US).\n"
        "  Fix: connect a SYSTEM-WIDE VPN (a browser VPN extension is not "
        "enough — it doesn't route Python), confirm https://www.maxpreps.com "
        "loads in your browser, then re-run.\n"
        "  No header or TLS change bypasses a geo-block."
    )


# ── Thread-safe build ID management ──────────────────────────────────────────
# Many threads can hit a stale build ID at the same time. Without locking they
# would all independently refetch, blasting MaxPreps with concurrent root-page
# requests. The lock + version pattern (mirrored from app.py) collapses those
# concurrent refresh attempts into a single fetch.

_bid_lock = threading.Lock()
_bid_value = None
_bid_version = 0

def _fetch_build_id_raw():
    """Fetch the build ID from a team SCHEDULE page (not the homepage).

    MaxPreps sometimes runs two builds simultaneously — one serves the
    homepage, another serves team pages. Reading the buildId from the
    homepage and then using it on /_next/data/{bid}/{team}/schedule.json
    yields 404/406 across every team. We hit a known-stable team page
    first because that page's build is the one that's authoritative for
    the schedule.json endpoint we call. Falls back to the homepage if
    no team page returns a usable buildId.

    Caller must hold _bid_lock.
    """
    delays = [5, 10, 20, 40, 60]
    # Season-suffixed pages first: they're the same URL shape we actually
    # scrape, so their build is the one serving our schedule.json calls.
    seed_pages = [
        "https://www.maxpreps.com/tx/austin/austin-maroons/basketball/25-26/schedule/",
        "https://www.maxpreps.com/nm/albuquerque/la-cueva-bears/basketball/25-26/schedule/",
        "https://www.maxpreps.com/tx/austin/austin-maroons/basketball/schedule/",
        "https://www.maxpreps.com/ca/concord/de-la-salle-spartans/basketball/schedule/",
        "https://www.maxpreps.com",   # last-resort fallback
    ]
    last_err = None
    saw_geo_block = False
    for attempt, wait in enumerate(delays, 1):
        for url in seed_pages:
            try:
                status, text, _final = _http_get_page(url, timeout=30)
                if status == 403:
                    saw_geo_block = True
                    continue
                if status != 200 or not text:
                    continue
                bid = _extract_build_id(text)
                if bid:
                    return bid
            except Exception as e:
                last_err = e
                continue
        # A geo-block will never resolve by waiting — fail fast with the fix.
        if saw_geo_block:
            raise RuntimeError(_geo_block_error_message())
        print(f"  [WARN] Build ID not found in any seed page (attempt {attempt}/{len(delays)}). Waiting {wait}s…")
        time.sleep(wait)
    raise RuntimeError(f"MaxPreps build ID not found after all retries: {last_err}")


def _extract_build_id(html):
    """Pull the Next.js buildId out of a rendered MaxPreps page."""
    m = re.search(r"/_next/static/([a-zA-Z0-9_-]+)/_buildManifest\.js", html or "")
    return m.group(1) if m else None

def get_build_id():
    """Returns (build_id, version) atomically. Lazy-fetches on first call."""
    global _bid_value, _bid_version
    with _bid_lock:
        if _bid_value is None:
            _bid_value = _fetch_build_id_raw()
        return _bid_value, _bid_version

def refresh_build_id(old_version):
    """Refresh only if the cached version matches old_version. Collapses
    concurrent 404-driven refreshes from many threads into a single fetch."""
    global _bid_value, _bid_version
    with _bid_lock:
        if _bid_version == old_version:
            _bid_value = _fetch_build_id_raw()
            _bid_version += 1
        return _bid_value, _bid_version


def team_url_to_path(team_url):
    return re.sub(r"https://www\.maxpreps\.com/", "", team_url).rstrip("/")


_GUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def decode_contest_guid(c_param):
    """The `c=` query parameter of a game URL → contest GUID string.

    Two formats exist, and BOTH must be handled:

      new (2026)   c=dc2f2ce6-a427-4c18-93ca-839e288f67a0   — already a GUID
      legacy       c=m2wDd2EH10m3PvRUgsthEA                 — base64url of the
                                                              16 raw bytes

    The base64 branch alone silently returned None for every current URL
    (b64-decoding a hyphenated GUID yields the wrong length), and callers
    that skip entries without a guid then dropped EVERY game. Check for an
    already-formed GUID first.
    """
    if not c_param:
        return None
    s = c_param.strip()
    if _GUID_RE.match(s):
        return s.lower()
    try:
        b64 = s.replace("-", "+").replace("_", "/")
        pad = (4 - len(b64) % 4) % 4
        b = base64.b64decode(b64 + "=" * pad)
        if len(b) != 16:
            return None
        p1 = struct.unpack_from("<I", b, 0)[0]
        p2 = struct.unpack_from("<H", b, 4)[0]
        p3 = struct.unpack_from("<H", b, 6)[0]
        p4 = b[8:16].hex()
        return f"{p1:08x}-{p2:04x}-{p3:04x}-{p4[:4]}-{p4[4:]}"
    except Exception:
        return None


def _short_season(season):
    """Normalise a season string for use as a URL path segment.

    '2024-2025' → '24-25'; '24-25' → '24-25'; None/empty → None.
    Used to fetch past-season schedule data — MaxPreps serves it at
    {team_path}/{YY-YY}/schedule.json, NOT {team_path}/schedule.json
    (which always returns the current season regardless of any flag we pass)."""
    if not season:
        return None
    m = re.match(r'^(?:20)?(\d{2})-(?:20)?(\d{2})$', season.strip())
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return season


# ── Team level: varsity / jv / freshman ──────────────────────────────────────
# MaxPreps nests the level AFTER the gender and BEFORE the season, and varsity
# carries no segment at all. Verified live 2026-08-19:
#   boys varsity    /tx/allen/allen-eagles/basketball/25-26/schedule/
#   boys jv         /tx/allen/allen-eagles/basketball/jv/25-26/schedule/
#   boys freshman   /tx/allen/allen-eagles/basketball/freshman/25-26/schedule/
#   girls jv        /tx/allen/allen-eagles/basketball/girls/jv/25-26/schedule/
# The reverse order (/jv/girls/) is a 404, so gender always precedes level.

LEVELS = ("varsity", "jv", "freshman")
_LEVEL_SEGMENTS = {"jv", "freshman"}


def normalise_level(level):
    """Anything falsy or unrecognised -> 'varsity'."""
    lv = (level or "varsity").strip().lower()
    return lv if lv in LEVELS else "varsity"


def level_segment(level):
    """'jv' -> '/jv'.  varsity -> '' (MaxPreps has no varsity segment)."""
    lv = normalise_level(level)
    return "" if lv == "varsity" else f"/{lv}"


def level_file_suffix(level):
    """Filename infix: '' for varsity so existing varsity outputs keep their
    names, '_jv' / '_freshman' otherwise."""
    lv = normalise_level(level)
    return "" if lv == "varsity" else f"_{lv}"


def strip_level(team_path):
    """Drop a trailing /jv or /freshman from a team path or URL."""
    if not team_path:
        return team_path
    trailing_slash = team_path.endswith("/")
    parts = [x for x in team_path.rstrip("/").split("/") if x != ""]
    if parts and parts[-1].lower() in _LEVEL_SEGMENTS:
        parts.pop()
    out = "/".join(parts)
    # Rebuild the scheme separator that the split above collapsed.
    out = out.replace("https:/", "https://").replace("http:/", "http://")
    return out + "/" if trailing_slash else out


def apply_level(team_path, level):
    """Return `team_path` with the level segment applied (idempotent).

    Works for both bare paths ('tx/allen/allen-eagles/basketball/girls') and
    full URLs ('https://www.maxpreps.com/tx/.../basketball/girls/').
    """
    if not team_path:
        return team_path
    trailing_slash = team_path.endswith("/")
    base = strip_level(team_path).rstrip("/")
    seg = level_segment(level)
    out = f"{base}{seg}"
    return out + "/" if trailing_slash else out


def level_of(team_path):
    """'…/basketball/girls/jv' -> 'jv';  no segment -> 'varsity'."""
    parts = [x for x in (team_path or "").rstrip("/").split("/") if x]
    if parts and parts[-1].lower() in _LEVEL_SEGMENTS:
        return parts[-1].lower()
    return "varsity"


def _with_stats_tab(url):
    """Force MaxPreps' 2026-redesigned game page to render its Stats tab.

    The redesign split the game page into Recap / Stats / Roster / Matchup
    tabs; the per-player shooting/totals tables that used to sit directly on
    the page (inside div.stat-category) now only render under Stats, and the
    page defaults to Recap. Appending ?tab=stats reproduces what happens when
    a visitor clicks the Stats tab.
    """
    if not url:
        return url
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query))
    q["tab"] = "stats"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def _schedule_page_url(team_path, season_suffix=None):
    """The schedule URL a browser loads."""
    if season_suffix:
        return f"https://www.maxpreps.com/{team_path}/{season_suffix}/schedule/"
    return f"https://www.maxpreps.com/{team_path}/schedule/"


def _schedule_json_url(build_id, team_path, season_suffix=None):
    if season_suffix:
        return (f"https://www.maxpreps.com/_next/data/{build_id}/"
                f"{team_path}/{season_suffix}/schedule.json")
    return (f"https://www.maxpreps.com/_next/data/{build_id}/"
            f"{team_path}/schedule.json")


def fetch_schedule(build_id, team_path, season_suffix=None):
    """Returns contest list, {"_expired": True}, or None on error.

    season_suffix (e.g. '24-25') is inserted between the team path and
    'schedule.json' so the past-season schedule is fetched instead of the
    current one. None means current season (existing behavior).

    This is the CLEANEST source of a schedule: the contests array carries the
    per-game ssid that boxscore.aspx wants. Verified working 2026-08-19.
    fetch_game_entries() wraps this with HTML fallbacks for hosts where the
    endpoint is refused.
    """
    url = _schedule_json_url(build_id, team_path, season_suffix)
    time.sleep(DELAY)
    try:
        # Referer + x-nextjs-data mirror what the Next.js client sends; some
        # edges refuse the data endpoint without them.
        status, text, _final = _http_get(
            url, timeout=25, kind="json",
            extra_headers={"Referer": _schedule_page_url(team_path, season_suffix),
                           "x-nextjs-data": "1"})
        if status == 404:
            return {"_expired": True}
        if status != 200 or not text:
            return None
        data = json.loads(text)
        return (
            data.get("pageProps", {}).get("initialPageProps", {}).get("contests")
            or data.get("pageProps", {}).get("contests")
            or []
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        # Re-raise so the worker's outer retry can decide whether to back off.
        raise
    except Exception as e:
        print(f"    [WARN] schedule fetch failed for {team_path}: {e}")
        return None


def _contests_from_next_data(html):
    """Contests array embedded in a schedule page's __NEXT_DATA__ script.

    Note: on the 2026 pages this is frequently present but EMPTY (the
    schedule is hydrated client-side), so treat a 0-length result as "no
    answer" and move to the anchor scan rather than as "no games".
    """
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html or "", re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except Exception:
        return None
    props = data.get("props", data)          # __NEXT_DATA__ nests under props
    pp = props.get("pageProps", {}) or data.get("pageProps", {})
    return (pp.get("initialPageProps", {}).get("contests") or pp.get("contests"))


def _normalize_game_href(href):
    """Absolute https game URL from a schedule-page anchor."""
    if not href:
        return None
    href = href.split("#")[0].strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.maxpreps.com" + href
    if href.startswith("http"):
        return href
    return None


def _game_entries_from_schedule_html(html):
    """(game_url, guid, ssid=None) for every game link on a schedule page.

    Last-resort source: a schedule page also links to games that are NOT this
    team's (opponent widgets, "other games today"), so this over-collects.
    scrape_game() drops anything whose stat tables don't belong to the team
    we're scraping, so the extra links cost requests but can't corrupt data.
    """
    entries = []
    seen = set()
    for href in re.findall(r"""href=["']([^"']+)["']""", html or ""):
        full = _normalize_game_href(href)
        if not full or "maxpreps.com" not in full:
            continue
        if "/game/" not in full and "/games/" not in full:
            continue
        m = re.search(r"[?&]c=([A-Za-z0-9_-]+)", full)
        if not m:
            continue
        guid = decode_contest_guid(m.group(1))
        key = guid or full
        if key in seen:
            continue
        seen.add(key)
        entries.append((full, guid, None))
    return entries


def fetch_game_entries(build_id, team_path, season_suffix=None):
    """[(game_url, guid, ssid), …] for a team's schedule, or {"_expired": True}
    on a 404 season page, or None when every source failed.

    Tries, in order:
      1. /_next/data/…/schedule.json  — clean, carries ssid  (preferred)
      2. schedule page __NEXT_DATA__  — same contests shape, no extra request cost
      3. schedule page game anchors   — over-collects, no ssid  (last resort)

    Order is deliberate and the reverse of what an HTML-first implementation
    would do: verified 2026-08-19 that (1) returns exactly the team's games
    with ssids (27/34/29 for three test teams) while (2) returned 0 contests
    and (3) returned 69/93/72 links — i.e. HTML-first would triple the
    request count and lose the ssid. The HTML paths exist for hosts where
    the data endpoint is refused (406).
    """
    contests = fetch_schedule(build_id, team_path, season_suffix=season_suffix)
    if isinstance(contests, dict):          # {"_expired": True}
        return contests
    if contests:
        entries = get_game_entries(contests)
        if entries:
            return entries
        return []                            # season page exists, no games listed

    # schedule.json unavailable (406/blocked) — fall back to the HTML page.
    page_url = _schedule_page_url(team_path, season_suffix)
    time.sleep(DELAY)
    try:
        status, html, _final = _http_get_page(page_url, timeout=25)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        raise
    if status == 404:
        return {"_expired": True}
    if status != 200 or not html:
        print(f"    [WARN] schedule page {status} for {team_path}")
        return None

    embedded = _contests_from_next_data(html)
    if embedded:
        entries = get_game_entries(embedded)
        if entries:
            return entries

    link_entries = _game_entries_from_schedule_html(html)
    if link_entries:
        print(f"    [INFO] {team_path}: using {len(link_entries)} anchor links "
              f"(schedule.json unavailable; some may not be this team's games)")
        return link_entries
    return []


def get_game_entries(contests):
    """
    Extract (game_url, contest_guid, ssid) for every scheduled game.
    The ssid at index 14 is the team's own season ID — consistent across
    all games in the team's schedule.
    """
    NULL = "00000000-0000-0000-0000-000000000000"
    team_ssid = next(
        (c[14] for c in contests
         if isinstance(c, list) and len(c) > 14 and c[14] and c[14] != NULL),
        None,
    )
    entries = []
    for c in contests:
        if not (isinstance(c, list) and len(c) > 18):
            continue
        url = c[18]
        if not (isinstance(url, str) and url.startswith("https://")):
            continue
        m = re.search(r"[?&]c=([A-Za-z0-9_-]+)", url)
        guid = decode_contest_guid(m.group(1)) if m else None
        ssid = (c[14] if len(c) > 14 and c[14] and c[14] != NULL else team_ssid)
        entries.append((url, guid, ssid))
    return entries


# ── HTML parsing ──────────────────────────────────────────────────────────────

# Column-header → field-name mappings per stat category
# (percentage columns are deliberately omitted — they're recalculated downstream)

_SHOOTING_MAP = {
    "min":  "minutes_played",
    "pts":  "points",
    "fgm":  "fg_made",
    "fga":  "fg_attempts",
}
_DETAILED_MAP = {
    "3pm":  "3pt_made",
    "3pa":  "3pt_attempts",
    "ftm":  "ft_made",
    "fta":  "ft_attempts",
    "2fgm": "2pt_made",
    "2fga": "2pt_attempts",
}
_TOTALS_MAP = {
    "oreb": "offensive_rebounds",
    "dreb": "defensive_rebounds",
    "reb":  "rebounds",
    "ast":  "assists",
    "stl":  "steals",
    "blk":  "blocks",
    "to":   "turnovers",
    "pf":   "personal_fouls",
}
_MISC_MAP = {
    "chr":  "charges_taken",
    "defl": "deflections",
    "tf":   "technical_fouls",
}
_CAT_MAPS = {
    "shooting":          _SHOOTING_MAP,
    "detailed_shooting": _DETAILED_MAP,
    "totals":            _TOTALS_MAP,
    "misc":              _MISC_MAP,
}


def _safe_num(text):
    """Cell text → int/float, or None if blank/dash."""
    if not text or text in ("-", "—", "–"):
        return None
    try:
        f = float(text)
        return int(f) if f == int(f) else f
    except (ValueError, TypeError):
        return None


def _parse_athlete_cell(text):
    """
    'C. Urune-Williams(Jr)' or 'C. Urune-Williams (Jr)'
    → ('C. Urune-Williams', 'Jr')
    """
    m = re.match(r"^(.+?)\s*\((\w+)\)\s*$", text.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return text.strip(), ""


def _identify_category(headers):
    """Determine stat category type from table column headers."""
    hs = {h.lower().replace(" ", "").replace("%", "") for h in headers}
    if "chr" in hs or "defl" in hs:
        return "misc"
    if "oreb" in hs or "dreb" in hs:
        return "totals"
    if "3pm" in hs or "ftm" in hs or "2fgm" in hs:
        return "detailed_shooting"
    if "fgm" in hs or "fga" in hs:
        return "shooting"
    return None


def _table_header_cells(table):
    """Header cells for a stat table.

    The 2026-redesigned tables do still use <thead> (verified against live
    pages), so the first branch is the normal path; falling back to the
    table's first row just keeps this working if that ever changes.
    """
    cells = table.select("thead th, thead td")
    if cells:
        return cells
    first_tr = table.find("tr")
    return first_tr.find_all(["th", "td"]) if first_tr else []


def _table_body_rows(table):
    """Player rows for a stat table, excluding the header row.

    Team Totals sit in <tfoot> in the current markup, so they're naturally
    excluded here. The fallback ('every row after the first') covers a table
    rendered without a <tbody> wrapper.
    """
    rows = table.select("tbody tr")
    if rows:
        return rows
    all_rows = table.find_all("tr")
    return all_rows[1:] if len(all_rows) > 1 else []


def _parse_players(table, category):
    """
    Parse player rows from a stat table, excluding the Team Totals row
    (filtered by name text, regardless of whether it lives in <tbody> or
    <tfoot> — see _table_body_rows()).

    Returns a list of player dicts with the fields expected by
    Accumulation_data.py for this category.
    """
    field_map = _CAT_MAPS.get(category, {})
    headers = [th.get_text(strip=True) for th in _table_header_cells(table)]

    players = []
    for tr in _table_body_rows(table):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(cells) < 2:
            continue

        # Column 1 is always the athlete name cell
        name_text = cells[1]
        if not name_text or "team totals" in name_text.lower():
            continue

        player_name, player_class = _parse_athlete_cell(name_text)
        if not player_name:
            continue

        player = {"player_name": player_name, "class": player_class}

        for col_idx, header in enumerate(headers):
            if col_idx <= 1:
                continue             # skip # and Athlete Name columns
            if col_idx >= len(cells):
                break
            key = header.lower().replace(" ", "").replace("%", "")
            field = field_map.get(key)
            if field:
                player[field] = _safe_num(cells[col_idx])

        players.append(player)

    return players


def _slugify(text):
    """Normalised slug, matching the form MaxPreps uses in game URLs."""
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


# ── Next.js RSC payload parsing (primary stat source) ───────────────────────
# MaxPreps' 2026 redesign renders the Stats tab with React Server Components.
# The rendered HTML only ever contains ONE team's tables (whichever the
# client-side team switcher has selected — by default the team listed first
# in the game URL), so HTML scraping structurally cannot see the other team.
#
# The streamed RSC payload, however, carries BOTH teams' complete stat tables
# in a single response, as JSON, with each player row carrying an athlete
# href that names their school. That makes it strictly better than the HTML:
#   * both teams from one request (no second fetch, no missing opponent)
#   * deterministic team attribution (no inferring from URL slug order)
#   * machine-readable column names
# Verified 2026-08-13 against a live CO game page.
#
# The payload arrives as a series of  self.__next_f.push([1,"<js string>"])
# calls; concatenating those string literals reconstructs it.

_RSC_PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,("(?:[^"\\]|\\.)*")\]\)', re.S)

# Athlete profile links look like
#   https://www.maxpreps.com/co/commerce-city/adams-city-eagles/athletes/omar-deluna/?careerid=…
# The first three path segments are the school key we match teams on.
#
# Anchor on '/athletes/' rather than the host: the RSC payload currently uses
# absolute URLs, but the same links are RELATIVE ('/tx/allen/allen-eagles/
# athletes/…') in the rendered HTML. Requiring the host would make
# _school_of_rows() return None for relative hrefs, which silently discards
# the whole table — so accept either form.
_ATHLETE_SCHOOL_RE = re.compile(
    r"/([a-z]{2}/[^/]+/[^/]+)/athletes/", re.I)

# Stat group objects in the payload: {"name":"Shooting","subgroups":[…]}
_RSC_GROUP_RE = re.compile(r'\{"name":"([^"]*)","subgroups":')


def _school_key(team_path):
    """First three path segments of a team id/path — the identity shared by a
    team page and its athletes' profile links.

    'co/commerce-city/adams-city-eagles/basketball/girls'
        -> 'co/commerce-city/adams-city-eagles'
    """
    parts = [p for p in (team_path or "").split("/") if p]
    return "/".join(parts[:3]).lower() if len(parts) >= 3 else ""


def _rsc_payload_from_soup(soup):
    """Reconstruct the streamed RSC payload from the page's <script> tags."""
    chunks = []
    for script in soup.find_all("script"):
        text = script.string or script.get_text() or ""
        if "self.__next_f.push" not in text:
            continue
        for m in _RSC_PUSH_RE.finditer(text):
            try:
                chunks.append(json.loads(m.group(1)))
            except Exception:
                continue
    return "".join(chunks)


def _balanced_json_at(s, start):
    """Extract the balanced JSON object beginning at s[start] == '{'."""
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return None


def _players_from_rsc_table(columns, rows, category):
    """Convert one RSC stat table into our player-dict list.

    Column 0 is the jersey number and column 1 the athlete (whose cell carries
    `value` = display name, `caption` = '(Sr)' class, `href` = profile link).
    Remaining columns map to our fields by their `header` text, using the same
    _CAT_MAPS the HTML parser uses. Team totals live in each column's
    `overallValue`, not as a row, so no totals row filtering is needed —
    the name check below is just belt-and-braces.
    """
    field_map = _CAT_MAPS.get(category, {})
    headers = [(c.get("header") or "") for c in columns]

    players = []
    for row in rows:
        cells = row.get("columns") or []
        if len(cells) < 2:
            continue
        name_cell = cells[1] or {}
        player_name = (name_cell.get("value") or "").strip()
        if not player_name or "team totals" in player_name.lower():
            continue
        player_class = (name_cell.get("caption") or "").strip().strip("()").strip()

        player = {"player_name": player_name, "class": player_class}
        for idx, header in enumerate(headers):
            if idx <= 1:
                continue              # skip # and Name columns
            if idx >= len(cells):
                break
            key = header.lower().replace(" ", "").replace("%", "")
            field = field_map.get(key)
            if field:
                player[field] = _safe_num((cells[idx] or {}).get("value"))
        players.append(player)
    return players


def _school_of_rows(rows):
    """Majority school key across a table's athlete links.

    Using the majority (rather than the first hit) means a row whose athlete
    has no profile link — or a stray cross-linked athlete — can't misattribute
    the whole table.
    """
    counts = {}
    for row in rows:
        for cell in row.get("columns") or []:
            m = _ATHLETE_SCHOOL_RE.search((cell or {}).get("href") or "")
            if m:
                k = m.group(1).lower()
                counts[k] = counts.get(k, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _parse_rsc_stats(soup):
    """Extract per-school, per-category player stats from the RSC payload.

    Returns {school_key: {category: [player, …]}} — empty dict when the
    payload is absent or carries no recognisable stat tables (e.g. the Recap
    tab, whose payload has no stat groups at all).
    """
    payload = _rsc_payload_from_soup(soup)
    if not payload:
        return {}

    out = {}
    for m in _RSC_GROUP_RE.finditer(payload):
        raw = _balanced_json_at(payload, m.start())
        if not raw:
            continue
        try:
            group = json.loads(raw)
        except Exception:
            continue

        for sub in group.get("subgroups") or []:
            stats = sub.get("stats") or {}
            columns = stats.get("columns") or []
            rows = stats.get("rows") or []
            if not rows:
                continue          # 'Game Stats' / 'Per 32' season tables are empty here
            category = _identify_category([c.get("header") or "" for c in columns])
            if not category:
                continue          # not one of our four tracked categories
            school = _school_of_rows(rows)
            if not school:
                continue          # can't attribute — safer to drop than to guess
            players = _players_from_rsc_table(columns, rows, category)
            if players:
                out.setdefault(school, {}).setdefault(category, []).extend(players)
    return out


# ── Page-hyperlink team identification (deterministic) ──────────────────────
# Every box-score page starts with a scoreline widget that links to BOTH
# teams' canonical pages. We take the first two unique canonical team_ids
# we encounter in document order as the matchup pair. This avoids URL-slug
# guesswork that breaks when a city has multiple teams (e.g. Austin Bowie
# vs Austin Maroons → URL 'austin-vs-bowie' is ambiguous via slugs but
# unambiguous via hyperlinks).
#
# The trailing (?:/.*)? must accept MULTIPLE extra segments: as of the 2026
# redesign these scoreline links carry a season too, e.g.
#   /co/westminster/westminster-wolves/basketball/25-26/schedule/
# The previous single-segment pattern didn't match those, so no team ids were
# found and opponent resolution silently fell back to raw URL slugs.
_TEAM_LINK_RE = re.compile(
    r"^/([a-z]{2})/([^/]+)/([^/]+)/basketball(?:/(boys|girls))?(?:/.*)?$",
    re.I,
)


def _canonical_team_ids_on_page(soup, limit=2):
    """Returns the first `limit` unique canonical team_ids found in document
    order in any <a href> on the page."""
    seen = []
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]
        m = _TEAM_LINK_RE.match(href)
        if m:
            gp = f"/{m.group(4).lower()}" if m.group(4) else ""
            tid = (f"{m.group(1).lower()}/{m.group(2).lower()}/"
                   f"{m.group(3).lower()}/basketball{gp}")
            if tid not in seen:
                seen.append(tid)
                if len(seen) >= limit:
                    break
    return seen


def _team_name_from_id(team_id):
    """Fallback canonical team_name from a team_id slug.
    'tx/austin/austin-maroons/basketball/girls' → 'Austin Maroons'."""
    parts = (team_id or "").split("/")
    if len(parts) >= 3:
        name = parts[2].replace("-", " ").title()
        return name.replace("Aandm", "A&M").replace("aandm", "a&m")
    return team_id or ""


def _id_to_name_from_opp_index(opp_index):
    """Build {team_id: team_name} from the slug-keyed opp_index. Used to
    canonicalise the opponent's team_name when we got its team_id from page
    hyperlinks (not from slug matching)."""
    out = {}
    if not opp_index:
        return out
    for matches in opp_index.values():
        for tid, tname in matches:
            out.setdefault(tid, tname)
    return out


# ── Opponent canonical-name index ──────────────────────────────────────────
# Game URLs give us a SHORT opponent slug (e.g. 'san-augustine'). To put the
# full canonical team_id ('tx/san-augustine/san-augustine-wolves/basketball/
# girls') and full team_name ('San Augustine Wolves') into the opponent
# section — same shape as our own team — we look the slug up in an index
# built from the master team-list JSON.

_opp_index_cache: dict = {}        # cache_key → {slug: [(team_id, team_name), …]}
_opp_index_lock = threading.Lock()


def _candidate_slugs_for_team(team_id, team_name):
    """All slugs MaxPreps might use in a game URL to represent this team."""
    out = set()
    parts = (team_id or "").split("/")
    if len(parts) >= 2 and parts[1]:
        out.add(parts[1])                          # city slug
    if len(parts) >= 3 and parts[2]:
        out.add(parts[2])                          # full team slug
        if "-" in parts[2]:
            out.add(parts[2].rsplit("-", 1)[0])    # team slug minus mascot
    name_slug = _slugify(team_name)
    if name_slug:
        out.add(name_slug)
        if "-" in name_slug:
            out.add(name_slug.rsplit("-", 1)[0])   # name minus last word
    return {s for s in out if s}


def _build_opp_index(sport, season):
    """Load the master team list and build slug → [(team_id, team_name)].

    Covers EVERY state in the master file, so out-of-state opponents (LA,
    OK, NM, etc. teams visiting TX) also get canonical names — no short
    slugs leaking into the output if we have the team's row anywhere.

    Prefers the seasoned master file ('{sport}_basketball_all_states_{YY-YY}.json')
    over the un-seasoned one ('{sport}_basketball_all_states.json').
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    short = _short_season(season)
    candidates = []
    if short:
        candidates.append(f"{sport}_basketball_all_states_{short}.json")
    candidates.append(f"{sport}_basketball_all_states.json")

    index: dict[str, list[tuple[str, str]]] = {}
    used_file = None
    for fname in candidates:
        path = os.path.join(script_dir, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        used_file = path
        for sd in data.get("byState", {}).values():
            for d in sd.get("regions", {}).values():
                for t in d.get("teams", []):
                    url = t.get("teamUrl", "")
                    if not url:
                        continue
                    tid = re.sub(r"https?://(?:www\.)?maxpreps\.com/", "", url).rstrip("/")
                    tname = t.get("teamName", "") or ""
                    # Clean stored corruptions like 'Aandm' → 'A&M'.
                    tname = tname.replace("Aandm", "A&M").replace("aandm", "a&m")
                    for slug in _candidate_slugs_for_team(tid, tname):
                        index.setdefault(slug, []).append((tid, tname))
        break  # first match wins
    return index, used_file


def _get_opp_index(sport, season):
    """Cached accessor for the opponent index. Safe across threads."""
    key = (sport or "boys", _short_season(season))
    with _opp_index_lock:
        if key not in _opp_index_cache:
            idx, used = _build_opp_index(sport, season)
            _opp_index_cache[key] = idx
            if used:
                print(f"Opponent index loaded from {os.path.basename(used)} "
                      f"({len(idx):,} slugs).")
            else:
                print(f"[WARN] No master file found for {sport}/{season} — "
                      f"opponents will use short slugs only.")
        return _opp_index_cache[key]


def _resolve_opponent(opp_slug, our_team_id, opp_index):
    """Look up opp_slug in the master index.

    Returns (canonical_team_id, canonical_team_name). If the slug isn't in
    the master file (out-of-state team we haven't catalogued, etc.), returns
    the short slug + Title-cased name as a graceful degradation — never
    leaves the opponent section blank.
    """
    fallback_name = opp_slug.replace("-", " ").title() if opp_slug else ""
    if not opp_slug or not opp_index:
        return opp_slug, fallback_name
    matches = opp_index.get(opp_slug, [])
    if not matches:
        return opp_slug, fallback_name
    if len(matches) == 1:
        return matches[0]
    # Multiple master teams share this slug (e.g. duplicate city names). Prefer
    # a match in the same state as our own team if possible.
    our_state = our_team_id.split("/", 1)[0] if our_team_id else ""
    same_state = [m for m in matches if m[0].split("/", 1)[0] == our_state]
    if len(same_state) == 1:
        return same_state[0]
    # Still ambiguous — take the first deterministic match.
    return matches[0]


def _matchup_segment(game_url):
    """Extract the '{slugA}-vs-{slugB}' path segment from a game URL.

    MaxPreps has used two URL shapes for game pages:
      old (pre-2026):  /games/{date}/{sport}/{slugA}-vs-{slugB}.htm
      new (2026 redesign): /{state}/{sport}[/{gender}]/game/{slugA}-vs-{slugB}/{date}/
    Both encode the matchup as a single path segment containing '-vs-'
    (the old one with a trailing .htm). Scan segments directly instead of
    anchoring to the surrounding path structure, so this survives further
    URL-shape changes as long as the '{a}-vs-{b}' segment itself persists.
    """
    if not game_url:
        return None
    path = re.sub(r"^https?://[^/]+", "", game_url).split("?")[0]
    for seg in path.split("/"):
        if not seg:
            continue
        seg_clean = seg[:-4] if seg.lower().endswith(".htm") else seg
        if re.match(r"^[a-z0-9]+(?:-[a-z0-9]+)*-vs-[a-z0-9]+(?:-[a-z0-9]+)*$", seg_clean, re.I):
            return seg_clean.lower()
    return None


def _first_slug_from_url(game_url):
    """Return the FIRST-listed team's slug from the matchup segment.

    MaxPreps' redesigned Stats tab shows only one team's tables at a time,
    selected via a client-side team switcher we can't drive without running
    JS. Empirically the tab defaults to whichever team is listed first in
    the URL's '{a}-vs-{b}' segment — used to infer which side's tables we're
    looking at on a plain (no-JS) fetch.
    """
    matchup = _matchup_segment(game_url)
    if not matchup:
        return None
    parts = matchup.split("-vs-")
    return parts[0].strip("-") if len(parts) == 2 else None


def _opp_slug_from_url(game_url, team_id):
    """Derive the opponent's canonical slug from the game URL's matchup
    segment ('{slugA}-vs-{slugB}'), where one of slugA/slugB matches our
    team. This is the SINGLE source of truth for who the opponent is — it's
    MaxPreps' canonical naming, never an abbreviation or fallback.

    Returns the opponent's slug, or None if the URL doesn't contain a
    recognisable '{a}-vs-{b}' segment (rare — tournament games etc.).
    """
    matchup = _matchup_segment(game_url)
    if not matchup:
        return None
    parts = matchup.split("-vs-")
    if len(parts) != 2:
        return None
    a, b = parts[0].strip("-"), parts[1].strip("-")
    if not a or not b:
        return None
    # Decide which side is us using slugs derived from our team_id.
    our_slugs = _our_team_slugs(team_id)
    a_us = _slug_matches_us(a, our_slugs)
    b_us = _slug_matches_us(b, our_slugs)
    if a_us and not b_us:
        return b
    if b_us and not a_us:
        return a
    if a_us and b_us:
        # Both sides match our team_id (extremely rare: e.g. our slug is a
        # substring of the opponent's). Pick the one with the shorter overlap
        # since the longer one is more specifically us.
        return b if len(a) >= len(b) else a
    # Neither side matched our team_id — URL probably uses a name we can't
    # recognise. No fallback by design.
    return None


def _our_team_slugs(team_id):
    """Return the set of candidate slugs that may represent us in a game URL.

    Our canonical team_id has the shape:  tx/CITY/TEAM-SLUG/basketball[/girls]
    MaxPreps' game-URL slug for us is often one of:
      - CITY              (most common, e.g. 'milford')
      - TEAM-SLUG         ('milford-bulldogs')
      - TEAM-SLUG without trailing mascot word ('milford-bulldogs' → 'milford')
      - CITY+TEAM-SLUG    rare
    """
    parts = (team_id or "").split("/")
    slugs = set()
    if len(parts) >= 2 and parts[1]:
        slugs.add(parts[1])                                # city
    if len(parts) >= 3 and parts[2]:
        slugs.add(parts[2])                                # full team slug
        no_mascot = parts[2].rsplit("-", 1)[0]             # drop trailing word
        if no_mascot:
            slugs.add(no_mascot)
    return slugs


def _slug_matches_us(candidate, our_slugs):
    """True if `candidate` (from the URL) represents our team.

    Match is exact first, then a directional substring match (so 'huntington'
    matches our 'huntington-red-devils' team_slug, and vice versa). We avoid
    any reverse fuzzy match against arbitrary text — only candidates already
    in `our_slugs` participate."""
    if not candidate:
        return False
    if candidate in our_slugs:
        return True
    for s in our_slugs:
        if s and (s == candidate or candidate.startswith(s + "-")
                  or s.startswith(candidate + "-")):
            return True
    return False


def _is_our_team(page_name, our_slugs):
    """True if `page_name` (the text in a div's span.school) refers to us.

    page_name is title-case (e.g. 'Huntington', 'Anderson Shiro'). We compare
    its slug form against the canonical slugs derived from our team_id.
    """
    if not page_name:
        return False
    return _slug_matches_us(_slugify(page_name), our_slugs)


def parse_game_page(soup, game_url, our_team_name, team_id, opp_index=None):
    """
    Parse the per-player stat tables on a rendered game page.

    MaxPreps' 2026 redesign replaced the old div.stat-category layout (both
    teams' tables side by side, identified via span.school) with tabbed
    Recap/Stats/Roster/Matchup navigation, rendered via React Server
    Components.

    PRIMARY source is the streamed RSC payload (see _parse_rsc_stats), which
    carries BOTH teams' stat tables as JSON with per-athlete school links.
    The rendered HTML only ever contains the ONE team the client-side
    switcher has selected, so the RSC path is the only way to capture the
    opponent's players at all.

    FALLBACK, if the payload is missing or unparseable, is the previous
    behaviour: scan every <table>, categorise by column headers, and infer
    which team is shown from the game URL's '{a}-vs-{b}' slug order.

    The opponent's identity is determined from page hyperlinks / the game
    URL slug — NOT from any text scraped from the page body — same as
    before the redesign.

    Returns:
    {
      "team_name":   <our canonical team name, with mascot>,
      "opp_name":    <opponent's canonical team name, with mascot>,
      "opp_id":      <opponent's canonical team_id, e.g. 'tx/san-augustine/san-augustine-wolves/basketball/girls'>,
      "shooting":          {"team": {"players":[...]}, "opponent": {"players":[...]}},
      "detailed_shooting": {...},
      "totals":            {...},
      "misc":              {...},
    }
    or None if no recognisable stat data is present anywhere on the page.
    """
    tables = soup.find_all("table")
    rsc_stats = _parse_rsc_stats(soup)
    if not tables and not rsc_stats:
        return None

    # ── Opponent identity from PAGE HYPERLINKS (deterministic) ───────────
    # Box-score pages start with a scoreline widget that links to both teams'
    # canonical pages. Using those hyperlinks is unambiguous — it's immune to
    # the URL-slug ambiguity that breaks for same-city opponents like
    # 'austin-vs-bowie' (Austin Bowie vs Austin Maroons, both in Austin).
    # team_id may carry a level segment (…/basketball/jv). Page hyperlinks and
    # the master opponent index only ever use the VARSITY-shaped id, so match
    # on the level-stripped form and re-apply the level to the result. A JV
    # game is JV for both sides, so the opponent inherits our level.
    our_level    = level_of(team_id)
    base_team_id = strip_level(team_id)

    page_tids = _canonical_team_ids_on_page(soup, limit=2)
    opp_id = ""
    if base_team_id in page_tids:
        opp_id = next((t for t in page_tids if t != base_team_id), "")

    # If hyperlinks didn't put us in the matchup pair (rare — page structure
    # change or missing links), fall back to the URL-slug + master-index path.
    if not opp_id:
        opp_slug_raw = _opp_slug_from_url(game_url, base_team_id) or ""
        opp_id_fallback, opp_name_fallback = _resolve_opponent(opp_slug_raw, base_team_id, opp_index)
        opp_id = opp_id_fallback
        opp_name = opp_name_fallback
    else:
        # We have a canonical team_id from the page. Look up its full team_name
        # in the master index; if missing, derive from the slug.
        id_to_name = _id_to_name_from_opp_index(opp_index)
        opp_name = id_to_name.get(opp_id) or _team_name_from_id(opp_id)

    # Carry our level onto the opponent so JV/freshman records never collide
    # with varsity ones downstream. Only for ids that look like a team path.
    if opp_id and "/" in opp_id:
        opp_id = apply_level(opp_id, our_level)

    # ── Per-category storage ─────────────────────────────────────────────
    team_cats = {c: [] for c in ("shooting", "detailed_shooting", "totals", "misc")}
    opp_cats  = {c: [] for c in ("shooting", "detailed_shooting", "totals", "misc")}

    def _finish():
        result = {
            "team_name": our_team_name,
            "opp_name":  opp_name or "",
            "opp_id":    opp_id or "",
        }
        for cat in ("shooting", "detailed_shooting", "totals", "misc"):
            result[cat] = {
                "team":     {"players": team_cats[cat]},
                "opponent": {"players": opp_cats[cat]},
            }
        return result

    # ── PRIMARY: RSC payload (both teams, deterministically attributed) ──
    if rsc_stats:
        our_key = _school_key(team_id)
        opp_key = _school_key(opp_id)
        # If hyperlinks/URL didn't pin the opponent down, take whichever other
        # school the payload itself contains — it only ever holds the two
        # teams in this matchup.
        if not opp_key:
            others = [k for k in rsc_stats if k != our_key]
            if len(others) == 1:
                opp_key = others[0]
                if not opp_id:
                    # Rebuild a full team_id by reusing our own sport/gender
                    # suffix ('basketball' or 'basketball/girls') — both teams
                    # in a matchup necessarily share it.
                    suffix = "/".join(team_id.split("/")[3:]) if team_id else ""
                    opp_id = f"{opp_key}/{suffix}".rstrip("/") if suffix else opp_key
                    id_to_name = _id_to_name_from_opp_index(opp_index)
                    opp_name = (id_to_name.get(opp_id)
                                or _team_name_from_id(opp_id) or opp_name)

        for cat in ("shooting", "detailed_shooting", "totals", "misc"):
            if our_key and our_key in rsc_stats:
                team_cats[cat].extend(rsc_stats[our_key].get(cat, []))
            if opp_key and opp_key in rsc_stats:
                opp_cats[cat].extend(rsc_stats[opp_key].get(cat, []))

        if any(team_cats[c] or opp_cats[c] for c in team_cats):
            return _finish()
        # Payload had stat groups but none matched either side — fall through
        # to the HTML path rather than reporting the game as empty.

    if not tables:
        return None

    # ── FALLBACK: which team's tables is the Stats tab currently showing? ──
    # Without the RSC payload we can't click the team switcher, so infer it
    # from the URL slug order. Compare against slugs derived from BOTH
    # team_ids (not opp_name) — slugs like 'adams-city-commerce-city' (team
    # name + literal city, MaxPreps' disambiguation suffix for duplicate
    # school names) don't match a display name via prefix-matching but do
    # match the team_id's own slug set.
    shown_slug = _first_slug_from_url(game_url)
    our_slugs = _our_team_slugs(team_id)
    opp_slugs = _our_team_slugs(opp_id) if opp_id else set()
    shown_is_us = None
    if shown_slug:
        is_us = _slug_matches_us(shown_slug, our_slugs)
        is_opp = _slug_matches_us(shown_slug, opp_slugs) if opp_slugs else False
        if is_us and not is_opp:
            shown_is_us = True
        elif is_opp and not is_us:
            shown_is_us = False
        # else ambiguous/unknown — leave shown_is_us as None

    found_any = False
    for table in tables:
        headers = [th.get_text(strip=True) for th in _table_header_cells(table)]
        category = _identify_category(headers)
        if not category:
            continue  # not one of our four known stat tables — skip (Roster, quarter-by-quarter score, etc.)
        players = _parse_players(table, category)
        if not players:
            continue
        found_any = True
        # Default to "us" when the shown side can't be determined — matches
        # the old parser's intent of never leaving our own section empty
        # without cause. shown_is_us is only ever False on a POSITIVE
        # opponent-slug match, so this can't silently misattribute confirmed
        # opponent data as ours.
        if shown_is_us is False:
            opp_cats[category].extend(players)
        else:
            team_cats[category].extend(players)

    if not found_any:
        return None

    return _finish()


# ── Game scraping ─────────────────────────────────────────────────────────────

def scrape_game(game_url, guid, ssid, our_team_name, team_id, opp_index=None):
    """
    Fetch and parse one game's box score.

    opp_index is the slug → canonical-team-id map produced by _get_opp_index.
    Passed through to parse_game_page so opponent records carry the FULL
    canonical team_id and team_name (e.g. 'tx/san-augustine/san-augustine-
    wolves/basketball/girls' + 'San Augustine Wolves'), matching the shape
    of our own team's record.

    Returns a record dict compatible with Accumulation_data.py, or None
    if the page could not be fetched or has no recognisable stat table.
    """
    time.sleep(DELAY)

    url = (
        f"https://www.maxpreps.com/local/stats/boxscore.aspx"
        f"?contestid={guid}&ssid={ssid}"
        if guid and ssid else game_url
    )

    try:
        status, html, final_url = _http_get_page(url, timeout=25, allow_redirects=True)
        if status == 404:
            return {"_404": True}
        if status != 200 or not html:
            return None

        # MaxPreps' 2026 redesign defaults the game page to its Recap tab;
        # the per-player stat tables only render under Stats. boxscore.aspx
        # redirects to the plain (tab-less) canonical URL, so re-fetch that
        # URL with ?tab=stats explicitly selected — same as clicking Stats.
        stats_url = _with_stats_tab(final_url)
        if stats_url != final_url:
            time.sleep(DELAY)
            st2, html2, final2 = _http_get_page(stats_url, timeout=25,
                                                allow_redirects=True)
            if st2 == 200 and html2:
                html, final_url = html2, (final2 or stats_url)

        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        # Re-raise connection/timeout errors so the caller can handle retries
        if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            raise e
        print(f"    [WARN] fetch failed for {game_url[-55:]}: {e}")
        return None

    # Opponent identity comes from the canonical game URL — never guessed.
    # Pass the final redirected URL since boxscore.aspx redirects to the
    # public /{state}/{sport}/game/{a}-vs-{b}/{date}/ form that contains the
    # slugs (final_url here is that canonical URL + our ?tab=stats addition,
    # which doesn't affect the path slugs parse_game_page reads).
    page = parse_game_page(soup, final_url, our_team_name, team_id, opp_index)
    if not page:
        return None      # game has no recognisable stat table content

    # Game date from the final (redirected) URL
    date_m = re.search(r"/(\d{1,2}-\d{1,2}-\d{4})/", final_url)
    game_date = date_m.group(1) if date_m else ""

    # Anchor-derived entries carry no guid; recover it from the resolved URL so
    # every record still has a contest_id for downstream dedup.
    if not guid:
        cm = re.search(r"[?&]c=([A-Za-z0-9_-]+)", final_url or "")
        if cm:
            guid = decode_contest_guid(cm.group(1))

    return {
        "contest_id":        guid,
        "game_url":          final_url,
        "game_date":         game_date,
        "is_deleted":        False,
        "team":     {"team_id": team_id,        "team_name": our_team_name},
        "opponent": {"team_id": page["opp_id"], "team_name": page["opp_name"]},
        "shooting":          page["shooting"],
        "detailed_shooting": page["detailed_shooting"],
        "totals":            page["totals"],
        "misc":              page["misc"],
    }


# ── Output helpers ────────────────────────────────────────────────────────────

def _save(games, errors, total_teams, output_file, processed_teams=None):
    out = {
        "meta": {
            "totalGames":  len(games),
            "totalErrors": len(errors),
            "totalTeams":  total_teams,
            "processedTeamsCount": len(processed_teams) if processed_teams else 0,
            "processedTeams": list(processed_teams) if processed_teams else [],
            "errors":      errors,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "games": games,
    }
    # Atomic write: avoid leaving a half-written file if interrupted mid-save.
    tmp = output_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    os.replace(tmp, output_file)


def _name_from_url(team_url, fallback=""):
    """Derive full team name (e.g. 'Avinger Indians') from the URL slug — see
    matching helper in app.py. Page parser needs the full name to match the
    team header on the box score."""
    m = re.match(r"https?://(?:www\.)?maxpreps\.com/([^/]+)/([^/]+)/([^/]+)/", team_url)
    if m:
        slug = m.group(3).replace("-", " ").title()
        if slug:
            return slug.replace("Aandm", "A&M").replace("aandm", "a&m")
    return fallback


# ── Core scraping logic (callable from gap finder or standalone) ──────────────

def _scrape_team(team, season_suffix=None, opp_index=None, game_cache=None):
    """Worker: scrape one team's full set of games. Returns
    (team_id, team_name, team_url, games_list, error_dict_or_None).

    season_suffix (e.g. '24-25') makes the schedule fetch target a past
    season. None = current season.

    opp_index is the slug→canonical map from the master team list, used so
    every game record carries the opponent's FULL canonical team_id and name.

    All HTTP work happens here — no shared state is touched. Caller commits
    the returned games/error under a lock.
    """
    team_url = team["teamUrl"]
    team_id = team_url_to_path(team_url)
    # Derive team_name from URL slug rather than trusting the gaps file —
    # stale stored names (e.g. 'Avinger' instead of 'Avinger Indians') break
    # the box-score page parser's team-header match.
    team_name = _name_from_url(team_url, team.get("teamName", ""))
    path = team_url_to_path(team_url)

    # ── Schedule with bounded retries ────────────────────────────────────────
    # fetch_game_entries() returns entry tuples directly: schedule.json first
    # (clean, carries ssid), then the schedule page's __NEXT_DATA__, then its
    # game anchors — so a host where the data endpoint is refused still works.
    game_entries = None
    bid_change_retries = 0     # 404s where refresh produced a NEW bid
    stable_bid_retries = 0     # 404s where refresh returned the SAME bid (wait 15 min, then retry)
    none_retries = 0
    net_retries = 0
    bid, bid_version = get_build_id()
    while game_entries is None:
        try:
            game_entries = fetch_game_entries(bid, path, season_suffix=season_suffix)

            if isinstance(game_entries, dict) and game_entries.get("_expired"):
                # 404 on schedule.json. Try a fresh build_id.
                new_bid, new_bid_version = refresh_build_id(bid_version)
                if new_bid != bid:
                    # MaxPreps rolled the build id — retry with the new one immediately.
                    bid_change_retries += 1
                    if bid_change_retries > 3:
                        return team_id, team_name, team_url, [], {
                            "teamName": team_name, "teamUrl": team_url,
                            "stage": "schedule", "reason": "build_id_kept_rolling",
                        }
                    bid, bid_version = new_bid, new_bid_version
                    game_entries = None
                    continue
                # Same bid back. Per user-requested strategy: MaxPreps may not have
                # rolled the build id yet — wait 15 minutes then check again. Repeat
                # up to BID_STABLE_MAX_RETRIES times before skipping the team.
                stable_bid_retries += 1
                if stable_bid_retries > BID_STABLE_MAX_RETRIES:
                    return team_id, team_name, team_url, [], {
                        "teamName": team_name, "teamUrl": team_url,
                        "stage": "schedule",
                        "reason": f"build_id_stable_after_{BID_STABLE_MAX_RETRIES}x{BID_STABLE_WAIT_SEC//60}min_waits",
                    }
                print(f"  [{team_name}] schedule 404, bid={bid} unchanged. "
                      f"Waiting {BID_STABLE_WAIT_SEC // 60} min for build id to update "
                      f"({stable_bid_retries}/{BID_STABLE_MAX_RETRIES}).")
                time.sleep(BID_STABLE_WAIT_SEC)
                game_entries = None
                continue

            if game_entries is None:
                none_retries += 1
                if none_retries > 5:
                    return team_id, team_name, team_url, [], {
                        "teamName": team_name, "teamUrl": team_url,
                        "stage": "schedule", "reason": "fetch_returned_none",
                    }
                time.sleep(5)
                continue

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            net_retries += 1
            if net_retries > 5:
                return team_id, team_name, team_url, [], {
                    "teamName": team_name, "teamUrl": team_url,
                    "stage": "schedule", "error": str(e),
                }
            time.sleep(min(5 * net_retries, 30))
            continue
        except Exception as e:
            return team_id, team_name, team_url, [], {
                "teamName": team_name, "teamUrl": team_url,
                "stage": "schedule", "error": str(e),
            }

    # ── Games (sequential within a single team to cap per-team request rate) ──
    entries = game_entries
    game_cache = game_cache or {}
    team_games = []
    for game_url, guid, ssid in entries:
        # Anchor-sourced entries have guid=None; scrape_game() falls back to
        # the plain game URL and recovers the guid from the resolved page,
        # so don't skip them the way the schedule.json-only path used to.
        if not guid and not game_url:
            continue
        # Cache hit: an earlier run already scraped this exact contest_id for
        # this team, and scrape_game() only ever stores records with real
        # stat content (an empty/no-data page returns None and is never
        # saved) — so any cached record here is trustworthy to reuse as-is,
        # no HTTP request needed. This is what keeps a daily re-run cheap.
        cached = game_cache.get((team_id, guid)) if guid else None
        if cached is not None:
            team_games.append(cached)
            continue
        for _attempt in range(3):
            try:
                record = scrape_game(game_url, guid, ssid, team_name, team_id, opp_index)
                if isinstance(record, dict) and record.get("_404"):
                    break
                if record:
                    team_games.append(record)
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                time.sleep(min(10 * (_attempt + 1), 30))
            except Exception:
                break

    return team_id, team_name, team_url, team_games, None


def run(input_file=None, output_file=None, sport="boys", season="2025-2026",
        workers=None, run_accumulator=True, limit=None, level="varsity"):
    """
    Scrape all full/partial teams from a gaps JSON file.
    Can be called programmatically or invoked via main().

    workers: parallel team count (default: TEAM_WORKERS = 15).
    limit:   process only the first N unprocessed teams (smoke-testing).
    level:   'varsity' (default), 'jv' or 'freshman'. Applied to every team URL
             so this stage is correct even when handed a varsity gaps file.
    """
    if input_file is None:
        input_file = INPUT_FILE
    if output_file is None:
        output_file = OUTPUT_FILE
    if workers is None or workers <= 0:
        workers = TEAM_WORKERS

    # ── Load input ───────────────────────────────────────────────────────────
    with open(input_file, encoding="utf-8") as f:
        gaps = json.load(f)

    # By default, scrape EVERY team — full, partial, AND teams the gap finder
    # classified as "no box scores". The gap-finder classification is just a
    # heuristic based on the presence of any stat-category divs on the page;
    # it can misclassify teams whose schedules added stats after the gap run,
    # so we re-check them too. Dedup by teamUrl in case the same team appears
    # in more than one bucket of the gaps file.
    full_b    = gaps.get("teamsFullBoxScores", [])
    partial_b = gaps.get("teamsPartialBoxScores", [])
    none_b    = gaps.get("teamsNoBoxScores", [])
    teams_by_url = {}
    for t in full_b + partial_b + none_b:
        url = t.get("teamUrl")
        if url and url not in teams_by_url:
            teams_by_url[url] = t
    teams = list(teams_by_url.values())

    # Force every team URL to the requested level. apply_level() is idempotent,
    # so a gaps file that already carries the level is left alone; a varsity one
    # is upgraded. Without this, --level jv against a varsity gaps file would
    # silently re-scrape varsity.
    level = normalise_level(level)
    if level != "varsity":
        for t in teams:
            if t.get("teamUrl"):
                t["teamUrl"] = apply_level(t["teamUrl"], level)

    total_teams = len(teams)
    # Normalise the season into the YY-YY URL segment ('2024-2025' → '24-25').
    # Without this the schedule fetch URL omits the season and MaxPreps falls
    # back to the current season — silently scraping wrong-season games.
    season_suffix = _short_season(season)
    # Build the slug→canonical-team-id index so opponent records carry the
    # full canonical team_id / team_name (e.g. 'tx/san-augustine/san-augustine-
    # wolves/basketball/girls' instead of just 'san-augustine'). Cached.
    opp_index = _get_opp_index(sport, season)
    print(f"Teams to process : {total_teams}  (full={len(full_b)} + partial={len(partial_b)} + no-data={len(none_b)})")
    print(f"Season           : {season} (URL suffix: {season_suffix or '(current)'})")
    print(f"Opp index size   : {len(opp_index):,} slugs")
    print(f"Workers          : {workers}")
    print(f"Level            : {level}{' (no URL segment)' if level=='varsity' else ' (URL segment /'+level+')'}")
    print(f"HTTP transport   : {_http_backend_label()}")
    if limit:
        print(f"Limit            : first {limit} unprocessed team(s) only")
    print()

    # Warm the build ID cache before fanning out so all threads share one fetch.
    bid, _ = get_build_id()
    print(f"Build ID : {bid}\n")
    print(f"Output   : {output_file}\n")
    print("─" * 60)

    all_games  = []
    errors     = []
    processed_teams = set()
    game_cache = {}

    # Every team gets a full pass every run (schedule re-fetch is cheap and
    # is the only way to catch newly-added games on an in-progress season).
    # What keeps a re-run cheap instead of a full re-scrape is the per-game
    # cache below: any (team_id, contest_id) an earlier run already has a
    # real record for is reused with no HTTP request — scrape_game() only
    # ever stores records with real stat content, so any existing record is
    # trustworthy. Games missing from the cache (new, or previously
    # unstatted) are (re-)fetched live.
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            existing_games = existing_data.get("games", [])
            game_cache = {
                (g["team"]["team_id"], g["contest_id"]): g
                for g in existing_games
                if g.get("team", {}).get("team_id") and g.get("contest_id")
            }
            print(f"Loaded {len(game_cache)} already-scraped games from the existing "
                  f"file — only new or previously-unstatted games will be (re-)fetched.")
        except Exception as e:
            print(f"Could not load existing output file for game cache: {e}")

    teams_to_do = teams
    if limit and limit > 0:
        teams_to_do = teams_to_do[:limit]
    if not teams_to_do:
        print(f"Nothing to do — all {total_teams} teams already processed.")
    else:
        print(f"Submitting {len(teams_to_do)} teams to {workers} workers...\n")
        agg_lock = threading.Lock()

        def _commit_result(team_id, team_name, team_url, games, error):
            with agg_lock:
                if error is not None:
                    errors.append(error)
                    # Don't mark errored teams as processed — future run will retry.
                else:
                    all_games.extend(games)
                    processed_teams.add(team_id)
                tdone = len(processed_teams)
                errs = len(errors)
                pct = tdone / total_teams * 100 if total_teams else 0.0
                status = "ERR " if error is not None else f"+{len(games):>3}"
                print(f"  [{tdone:>4}/{total_teams}] {pct:5.1f}% | err={errs:<3} | games={len(all_games):>5} | {status} | {team_name}")
                # Periodic save (every 10 successful team completions).
                if (tdone % 10 == 0 or tdone == total_teams) and error is None:
                    try:
                        _save(all_games, errors, total_teams, output_file, processed_teams)
                    except Exception as save_e:
                        print(f"  [WARN] Periodic save failed: {save_e}")

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_scrape_team, t, season_suffix, opp_index, game_cache): t for t in teams_to_do}
            for fut in as_completed(futures):
                try:
                    team_id, team_name, team_url, games, error = fut.result()
                except Exception as e:
                    # A worker crash shouldn't kill the loop. Record it and move on.
                    t = futures[fut]
                    print(f"  [ERROR] Worker crashed for {t.get('teamName', '?')}: {e}")
                    with agg_lock:
                        errors.append({
                            "teamName": t.get("teamName", ""),
                            "teamUrl":  t.get("teamUrl", ""),
                            "stage":    "worker_crash",
                            "error":    str(e),
                        })
                    continue
                _commit_result(team_id, team_name, team_url, games, error)

    # ── Final save ────────────────────────────────────────────────────────────
    # MUST pass processed_teams — otherwise the meta is overwritten with an empty
    # list and the next run would re-scrape every team from scratch.
    _save(all_games, errors, total_teams, output_file, processed_teams)

    # Surface any team in the input that we never reached. Skipped when --limit
    # is set, where leaving most teams unprocessed is the whole point.
    all_input_paths = {team_url_to_path(t["teamUrl"]) for t in teams}
    missing = all_input_paths - processed_teams
    if limit:
        print(f"\n[--limit {limit}] {len(missing)} team(s) intentionally left "
              f"for a later run.")
        missing = set()
    if missing:
        print(f"\n[WARNING] {len(missing)} input teams were not processed by the scraper:")
        for p in list(missing)[:20]:
            print(f"    {p}")
        if len(missing) > 20:
            print(f"    ... and {len(missing) - 20} more")
        print("Re-run the scraper to retry these teams.")

    unique_guids = len({g["contest_id"] for g in all_games})
    print("\n" + "=" * 60)
    print(f"  Total game records  : {len(all_games)}")
    print(f"  Unique contest IDs  : {unique_guids}")
    print(f"  Teams with errors   : {len(errors)}")
    print(f"  Saved → {output_file}")
    print("=" * 60)

    # Quick sample
    if all_games:
        print("\nSample games (first 3):")
        for g in all_games[:3]:
            t_players  = len(g["shooting"]["team"]["players"])
            op_players = len(g["shooting"]["opponent"]["players"])
            print(f"  {g['team']['team_name']} vs {g['opponent']['team_name']}"
                  f"  ({g['game_date']}) — "
                  f"team {t_players} players, opp {op_players} players")

    # ── Accumulation ─────────────────────────────────────────────────────────
    # Skip when an external orchestrator (APP/pipeline.py) drives accumulation
    # itself — that pipeline runs stats-tab scraping in between, so the
    # accumulator must run AFTER both files exist.
    if not run_accumulator:
        print("\n[run_accumulator=False] Skipping auto-chained accumulator.")
        return

    accumulated_file = output_file.replace("box_scores", "accumulated_stats")
    print(f"\n{'─' * 60}")
    print(f"Running data accumulation → {accumulated_file}")
    print("─" * 60)
    try:
        from Accumulation_data import process_stats
        process_stats(input_file=output_file, output_file=accumulated_file)
    except Exception as e:
        print(f"  [ERROR] Accumulation failed: {e}")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="HS Basketball Box Score Scraper"
    )
    parser.add_argument(
        "--state", default="TX", help="State code (default: TX)"
    )
    parser.add_argument(
        "--sport", default="boys", choices=["boys", "girls"],
        help="boys (default) or girls",
    )
    parser.add_argument(
        "--season", default="2025-2026",
        help="Season (e.g., 2025-2026)",
    )
    parser.add_argument(
        "--output", default=None, help="Explicit output file (optional)"
    )
    parser.add_argument(
        "--workers", type=int, default=TEAM_WORKERS,
        help=f"Parallel team workers (default: {TEAM_WORKERS}). "
             f"Each worker scrapes one team's games sequentially. "
             f"Raise for speed, lower if you hit rate limits.",
    )
    parser.add_argument(
        "--level", default="varsity", choices=list(LEVELS),
        help="Team level to scrape (default: varsity). MaxPreps nests this "
             "after the gender: /basketball/girls/jv/…",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N unprocessed teams. Use for a quick "
             "smoke test before committing to a full state run.",
    )
    parser.add_argument(
        "--no-accumulate", action="store_true",
        help="Skip the auto-chained Accumulation_data run. Useful when an "
             "external orchestrator handles the accumulator stage itself.",
    )
    parser.add_argument(
        "--input", default=None,
        help="Explicit path to the gaps JSON file. Overrides the CWD lookup. "
             "Required when running outside the default per-state layout.",
    )
    args = parser.parse_args()

    state_lower = args.state.lower()
    season_fn = args.season.replace("-", "_")

    if args.input:
        input_file = args.input
        if not os.path.exists(input_file):
            print(f"Error: --input file {input_file} not found.")
            sys.exit(1)
    else:
        # Default lookup: CWD-relative filename, then DATA_DIR fallback for
        # state-folder layouts when the orchestrator hasn't passed --input.
        input_file = f"{state_lower}_data_gaps_{args.sport}_{season_fn}.json"
        if not os.path.exists(input_file):
            # DATA_DIR-relative
            data_dir = os.environ.get("DATA_DIR")
            if data_dir and os.path.exists(os.path.join(data_dir, input_file)):
                input_file = os.path.join(data_dir, input_file)
            else:
                # Fallback to the dash version or old name
                alt_name = f"{state_lower}_data_gaps_{args.sport}_{args.season}.json"
                if os.path.exists(alt_name):
                    input_file = alt_name
                else:
                    fallback = f"{state_lower}_data_gaps.json"
                    if os.path.exists(fallback):
                        input_file = fallback
                    else:
                        print(f"Error: Input file {input_file} not found.")
                        sys.exit(1)

    out = args.output
    if out is None:
        # Output: tx_box_scores_boys_2025_2026.json  (+ _jv / _freshman)
        out = input_file.replace("data_gaps", "box_scores")
        sfx = level_file_suffix(args.level)
        if sfx and sfx not in out:
            out = out.replace(".json", f"{sfx}.json")

    run(input_file=input_file, output_file=out, sport=args.sport,
        season=args.season, workers=args.workers,
        run_accumulator=not args.no_accumulate, limit=args.limit,
        level=args.level)


if __name__ == "__main__":
    main()
