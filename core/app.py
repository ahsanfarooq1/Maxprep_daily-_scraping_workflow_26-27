
"""
HS Basketball — Box Score Gap Finder (High Speed Parallel Version)
==================================================================
Merges the multi-threaded performance of v3 with robust runtime saving 
and resume logic.

Expected runtime for 1800+ teams: ~30–45 minutes (vs 7 hours).


HOW TO USE:::::===========================================================
python app.py --state TX --sport girls --season 2025-2026   
python app.py --state AL --sport boys --season 2024-2025
==========================================================================
"""

import os
import re
import sys
import json
import time
import base64
import struct
import hashlib
import threading
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR = os.environ.get("DATA_DIR", ".")

# Timestamped print: every log line gets a "[YYYY-MM-DD HH:MM:SS]" prefix.
_original_print = print
def print(*args, **kwargs):
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)

# ─── State lookup ─────────────────────────────────────────────────────────────

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

# ─── Config ───────────────────────────────────────────────────────────────────

INPUT_FILE    = "boys_basketball_all_states.json"
DELAY         = 0.3    # base delay (per thread)
SCHED_WORKERS = 20     # Parallel schedule fetches
GAME_WORKERS  = 50     # Parallel game checks

# MaxPreps serves a 403 "Geo-block" page to requests from some countries —
# genuinely geographic (a VPN in an allowed region fixes it; header/TLS
# tuning does not). See the matching comment in scrape_box_scores.py;
# this header set is kept in sync with it.
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

# ─── Thread-local HTTP sessions ───────────────────────────────────────────────

_tls = threading.local()

def _session(json_mode=True):
    key = "jsess" if json_mode else "hsess"
    if not hasattr(_tls, key):
        s = requests.Session()
        s.headers.update(HEADERS if json_mode else HTML_HEADERS)
        setattr(_tls, key, s)
    return getattr(_tls, key)

# ─── Build-ID management (Thread Safe) ────────────────────────────────────────

_bid_lock    = threading.Lock()
_bid_value   = None
_bid_version = 0

def _fetch_raw_bid():
    """Extract the Next.js buildId that serves team-schedule API calls.

    MaxPreps sometimes runs two builds simultaneously — one serves the
    homepage, another serves team pages. Reading the buildId from the
    homepage and then using it on /_next/data/{bid}/{team}/schedule.json
    yields 404/406 across every team. We hit a known-stable team SCHEDULE
    page first because that page's build is the one that's authoritative
    for the schedule.json endpoint we actually call.
    """
    seed_pages = [
        "https://www.maxpreps.com/tx/austin/austin-maroons/basketball/schedule/",
        "https://www.maxpreps.com/ca/concord/de-la-salle-spartans/basketball/schedule/",
        "https://www.maxpreps.com",   # last-resort fallback
    ]
    last_err = None
    for url in seed_pages:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            m = re.search(r"/_next/static/([a-zA-Z0-9_-]+)/_buildManifest\.js", r.text)
            if m:
                return m.group(1)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Build ID not found in any seed page. Last error: {last_err}")

def get_build_id():
    global _bid_value, _bid_version
    with _bid_lock:
        if _bid_value is None:
            _bid_value = _fetch_raw_bid()
        return _bid_value, _bid_version

def refresh_build_id(old_version):
    global _bid_value, _bid_version
    with _bid_lock:
        if _bid_version == old_version:
            _bid_value    = _fetch_raw_bid()
            _bid_version += 1
        return _bid_value, _bid_version

# ─── Helpers ──────────────────────────────────────────────────────────────────

def team_url_to_path(team_url):
    return re.sub(r"https://www\.maxpreps\.com/", "", team_url).rstrip("/")

def clean_team_name(name):
    """Fix known URL-encoding corruptions in team names from the master list."""
    return name.replace("Aandm", "A&M").replace("aandm", "a&m")

def name_from_url(team_url, fallback=""):
    """Derive full team name (e.g. 'Avinger Indians') from the URL slug.
    Falls back to the stored name only if the URL doesn't parse — this prevents
    old/stale stored names like 'Avinger' from breaking the page parser, which
    needs the full name to match the box score's team header."""
    m = re.match(r"https?://(?:www\.)?maxpreps\.com/([^/]+)/([^/]+)/([^/]+)/", team_url)
    if m:
        slug = m.group(3).replace("-", " ").title()
        if slug:
            return clean_team_name(slug)
    return clean_team_name(fallback)

def decode_contest_guid(c_param):
    """Delegates to scrape_box_scores so both stages agree on the format.

    MaxPreps' `c=` parameter is now already a GUID; only legacy URLs use the
    base64url form. Keeping one implementation avoids the gap finder and the
    box-score scraper disagreeing about which games have a usable contest id.
    """
    from scrape_box_scores import decode_contest_guid as _decode
    return _decode(c_param)

def _short_season(season):
    """Normalise '2024-2025' or '24-25' → '24-25'. Used to inject the season
    segment into the schedule fetch URL so past seasons are actually fetched."""
    if not season:
        return None
    m = re.match(r'^(?:20)?(\d{2})-(?:20)?(\d{2})$', season.strip())
    return f"{m.group(1)}-{m.group(2)}" if m else season


def _raw_fetch_schedule(bid, team_path, season_suffix=None):
    """Fetch one team's schedule contests via the shared transport.

    Uses scrape_box_scores._http_get so the gap finder benefits from the same
    curl_cffi → system-curl → requests chain as the box-score scraper. Some
    hosts get 406 on plain-requests traffic, and this stage makes by far the
    most requests, so it must not be the weak link.
    """
    from scrape_box_scores import _http_get, _schedule_page_url

    if season_suffix:
        url = f"https://www.maxpreps.com/_next/data/{bid}/{team_path}/{season_suffix}/schedule.json"
    else:
        url = f"https://www.maxpreps.com/_next/data/{bid}/{team_path}/schedule.json"
    time.sleep(DELAY)
    try:
        status, text, _final = _http_get(
            url, timeout=20, kind="json",
            extra_headers={"Referer": _schedule_page_url(team_path, season_suffix),
                           "x-nextjs-data": "1"})
        if status == 404: return {"_expired": True}
        if status == 429:
            time.sleep(5)
            return "_retry"
        if status in (500, 502, 503, 504): return "_retry"
        if status != 200 or not text: return None
        data = json.loads(text)
        ipp = data.get("pageProps", {}).get("initialPageProps", {}) or data.get("pageProps", {})
        contests = ipp.get("contests") or []
        return (contests, _extract_overall_record(ipp))
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return "_retry"
    except Exception: return None


def _extract_overall_record(ipp):
    """Total games MaxPreps has recorded as COMPLETED for this team this
    season (wins + losses [+ ties]), read straight from the same
    schedule.json payload already fetched for game discovery — no extra
    request needed.

    This is server-computed from official results, so it's immune to the
    duplicate-contest-id artifacts that can inflate a raw schedule-row count
    (see _dedupe_games_by_date_opponent) — including a reschedule that shifts
    the calendar date, which a (date, opponent) dedup can't catch at all.
    Returns None if the field isn't present in this payload shape.
    """
    try:
        rec = ipp["teamContext"]["standingsData"]["overallStanding"]["overallWinLossTies"]
        return sum(int(p) for p in rec.split("-"))
    except Exception:
        return None


def get_game_entries(contests):
    NULL_GUID = "00000000-0000-0000-0000-000000000000"
    team_ssid = None
    for c in contests:
        if isinstance(c, list) and len(c) > 14:
            if c[14] and c[14] != NULL_GUID:
                team_ssid = c[14]
                break
    entries = []
    for c in contests:
        if not (isinstance(c, list) and len(c) > 18): continue
        game_url = c[18]
        if not (isinstance(game_url, str) and game_url.startswith("https://")): continue
        m = re.search(r"[?&]c=([A-Za-z0-9_-]+)", game_url)
        guid = decode_contest_guid(m.group(1)) if m else None
        ssid = c[14] if len(c) > 14 and c[14] and c[14] != NULL_GUID else team_ssid
        entries.append((game_url, guid, ssid))
    return entries

def _check_soup(soup, team_name):
    """Legacy quick check — true if the page has ANY stat section and our
    team isn't explicitly flagged as 'not entered'. Kept for compatibility
    only; the gap finder now uses _classify_game (below) which gives a
    proper 4-bucket per-game classification."""
    stat_sections = soup.select("div.stat-category")
    no_data_msgs  = [el.get_text(strip=True).lower() for el in soup.select("div.no-data")]
    norm = team_name.lower().strip()
    team_not_entered = any(norm in msg and "not entered" in msg for msg in no_data_msgs)
    return bool(stat_sections) and not team_not_entered


def _players_key(players):
    # -1 sentinel for a missing stat (never a real value - minutes/points/FG
    # are always >= 0): when two players tie on name, sorted() compares the
    # next tuple field, and comparing None against an int (e.g. one player
    # missing minutes_played) raises TypeError. Found via a reproducible
    # crash in a downstream engine that also builds box-score records from
    # this same fingerprint call - dormant here since this function is only
    # used for classification, but still a real bug worth fixing at the
    # source rather than leaving for the next caller that hits it.
    def _n(v):
        return v if v is not None else -1
    return sorted(
        (p.get("player_name", ""), _n(p.get("minutes_played")), _n(p.get("points")),
         _n(p.get("fg_made")), _n(p.get("fg_attempts")))
        for p in (players or [])
    )


def _stats_fingerprint(page):
    """Deterministic fingerprint of a parsed box-score page's ACTUAL player
    stats, used to tell two schedule entries sharing the same date+opponent
    apart:
      - identical fingerprint  => the same game reported under two contest_ids
        (a stale entry MaxPreps left behind after the game time was edited)
      - different fingerprint  => a genuine doubleheader — two real games

    Keyed off whichever SIDE actually has data. MaxPreps sometimes propagates
    only OUR team's own stats onto the stale duplicate contest_id and leaves
    the opponent's side empty there — so comparing the whole page (both
    sides) would call that a "different game" when it's really the same one.
    Our own team's stat line, when present, is the reliable signal (a coach
    doesn't record an identical box score for two different real games); the
    opponent's line is the fallback for the rare opp_only case where we have
    no stats of our own to compare.
    """
    categories = ("shooting", "detailed_shooting", "totals", "misc")
    team_parts = [(cat, tuple(_players_key((page.get(cat) or {}).get("team", {}).get("players"))))
                  for cat in categories]
    if any(page.get(cat, {}).get("team", {}).get("players") for cat in categories):
        parts = team_parts
    else:
        parts = [(cat, tuple(_players_key((page.get(cat) or {}).get("opponent", {}).get("players"))))
                 for cat in categories]
    return hashlib.sha256(repr(parts).encode("utf-8")).hexdigest()


def _classify_game(soup, final_url, our_team_name, our_team_id, opp_index):
    """Classify a single game's box-score page into one of:
        'full'      → both teams uploaded stats
        'team_only' → only OUR team uploaded
        'opp_only'  → only the OPPONENT uploaded
        'no_data'   → neither team uploaded any player rows

    Reuses the same parser the box-score scraper uses, so the gap finder's
    per-game classification matches what the downstream scraper will
    actually capture.

    Returns a dict {classification, date, url, opponent_team_id, opponent_team_name}
    or None if the page couldn't be parsed at all.
    """
    from scrape_box_scores import (
        parse_game_page,
        _canonical_team_ids_on_page,
        _id_to_name_from_opp_index,
        _team_name_from_id,
    )

    date_m = re.search(r"/(\d{1,2}-\d{1,2}-\d{4})/", final_url)
    date = date_m.group(1) if date_m else ""

    page = parse_game_page(soup, final_url, our_team_name, our_team_id, opp_index)
    if page is not None:
        has_team = any(len(page[c]["team"]["players"]) > 0
                       for c in ("shooting", "detailed_shooting", "totals", "misc"))
        has_opp = any(len(page[c]["opponent"]["players"]) > 0
                      for c in ("shooting", "detailed_shooting", "totals", "misc"))
        if has_team and has_opp:
            cls = "full"
        elif has_team:
            cls = "team_only"
        elif has_opp:
            cls = "opp_only"
        else:
            cls = "no_data"
        return {
            "classification":     cls,
            "date":               date,
            "url":                final_url,
            "opponent_team_id":   page["opp_id"],
            "opponent_team_name": page["opp_name"],
            "stats_fingerprint":  _stats_fingerprint(page) if cls != "no_data" else None,
        }

    # parse_game_page returned None → no stat-category divs at all.
    # Still try to identify the opponent so the no-data entry is informative.
    page_tids = _canonical_team_ids_on_page(soup, limit=2)
    opp_id = next((t for t in page_tids if t != our_team_id), "")
    if opp_id:
        id_to_name = _id_to_name_from_opp_index(opp_index)
        opp_name = id_to_name.get(opp_id) or _team_name_from_id(opp_id)
    else:
        opp_id = opp_name = ""
    return {
        "classification":     "no_data",
        "date":               date,
        "url":                final_url,
        "opponent_team_id":   opp_id,
        "opponent_team_name": opp_name,
        "stats_fingerprint":  None,
    }

def _contest_id_from_url(url):
    m = re.search(r"[?&]c=([A-Za-z0-9_-]+)", url or "")
    return m.group(1) if m else None


def _build_game_cache(existing_gaps):
    """(team_id, contest_id) -> (classification, game_rec) for every game an
    earlier run already found real stats for. Deliberately EXCLUDES no_data
    games — an unplayed/unstatted game today can still get a result
    tomorrow, so those must always be re-checked.

    This is what makes a daily re-run cheap: every team's schedule is
    re-fetched fresh every time (to catch newly-added games), but only a
    game missing from this cache actually hits the box-score page."""
    cache = {}
    for bucket in ("teamsFullBoxScores", "teamsPartialBoxScores", "teamsNoBoxScores"):
        for team in existing_gaps.get(bucket, []):
            t_id = team_url_to_path(team.get("teamUrl", ""))
            for cls, key in (("full", "fullDataGames"), ("team_only", "teamOnlyDataGames"),
                             ("opp_only", "opponentOnlyDataGames")):
                for g in team.get(key, {}).get("games", []):
                    cid = _contest_id_from_url(g.get("url"))
                    if cid:
                        cache[(t_id, cid)] = (cls, g)
    return cache


def _dedupe_games_by_date_opponent(full_games, team_only_games, opp_only_games, no_data_games, team_name):
    """Collapse schedule entries that are really the SAME game reported twice.

    MaxPreps' schedule feed occasionally carries two contest_ids for one
    matchup — e.g. a game's start time gets edited and the original time
    slot's contest record is left behind as an orphaned duplicate. Both show
    up as separate rows keyed on (date, opponent_team_id).

    Rule:
      - Only ONE entry for a (date, opponent) has real stats -> the other(s)
        are stale no_data placeholders of that same game. Drop them.
      - TWO OR MORE entries for a (date, opponent) have real stats:
          - identical stats_fingerprint -> the same game reported twice, keep one
          - different stats_fingerprint -> a genuine doubleheader, keep BOTH
      - ALL entries for a (date, opponent) are no_data (nothing to compare)
        -> can't tell a duplicate from an unstatted doubleheader; collapse to
        one and log it so it can be reviewed manually.
    """
    PRIORITY = {"full": 0, "team_only": 1, "opp_only": 1, "no_data": 2}
    tagged = []
    for lst, cls in ((full_games, "full"), (team_only_games, "team_only"),
                      (opp_only_games, "opp_only"), (no_data_games, "no_data")):
        for g in lst:
            tagged.append({**g, "_cls": cls})

    groups = {}
    for g in tagged:
        groups.setdefault((g.get("date", ""), g.get("opponent_team_id", "")), []).append(g)

    kept = []
    for key, group in groups.items():
        if len(group) == 1:
            kept.append(group[0])
            continue

        with_data = [g for g in group if g["_cls"] != "no_data"]
        no_data   = [g for g in group if g["_cls"] == "no_data"]

        if not with_data:
            # Nothing to fingerprint-compare — collapse but flag for review.
            best = sorted(group, key=lambda g: PRIORITY[g["_cls"]])[0]
            kept.append(best)
            print(f"    [DEDUPE] {team_name} | {key} | {len(group)}x no_data entries, "
                  f"none with stats — collapsed to 1 (verify manually: could be a "
                  f"genuine unstatted doubleheader)")
            continue

        unique_by_fp = {}
        for g in with_data:
            fp = g.get("stats_fingerprint")
            if fp not in unique_by_fp:
                unique_by_fp[fp] = g
            elif PRIORITY[g["_cls"]] < PRIORITY[unique_by_fp[fp]["_cls"]]:
                dupe = unique_by_fp[fp]
                unique_by_fp[fp] = g
                print(f"    [DEDUPE] {team_name} | {key} | identical stats across "
                      f"contest_ids ({dupe['_cls']} vs {g['_cls']}) — kept 1 of 2")
            else:
                print(f"    [DEDUPE] {team_name} | {key} | identical stats across "
                      f"contest_ids ({g['_cls']}) — kept 1 of 2")
        kept.extend(unique_by_fp.values())

        for g in no_data:
            print(f"    [DEDUPE] {team_name} | {key} | dropped a no_data placeholder "
                  f"— real stats already exist for this date+opponent")

    full_out, team_only_out, opp_only_out, no_data_out = [], [], [], []
    for g in kept:
        cls = g.pop("_cls")
        # stats_fingerprint is kept (not stripped) so a future daily run's
        # game cache (_build_game_cache) can still fingerprint-compare a
        # cached game against a freshly-discovered duplicate contest_id.
        if   cls == "full":      full_out.append(g)
        elif cls == "team_only": team_only_out.append(g)
        elif cls == "opp_only":  opp_only_out.append(g)
        else:                    no_data_out.append(g)
    return full_out, team_only_out, opp_only_out, no_data_out

# ─── Workers ──────────────────────────────────────────────────────────────────

def fetch_sched_worker(team, season_suffix=None):
    path = team_url_to_path(team["teamUrl"])
    bid, version = get_build_id()
    # Up to 5 attempts: handle 404 (stale build id), 5xx, 429, and connection errors
    for attempt in range(5):
        result = _raw_fetch_schedule(bid, path, season_suffix=season_suffix)
        if result is None:
            # Hard failure (non-retryable, non-200). Brief backoff before final retry.
            if attempt < 4:
                time.sleep(1 + attempt)
                continue
            return team, None, None
        if result == "_retry":
            time.sleep(min(2 ** attempt, 10))
            continue
        if isinstance(result, dict) and result.get("_expired"):
            bid, version = refresh_build_id(version)
            continue
        contests, overall_record = result
        return team, get_game_entries(contests), overall_record
    return team, None, None

def check_game_worker(game_url, guid, ssid, team_name, team_id=None, opp_index=None):
    """Fetch one game's box-score page and classify it into one of four
    buckets (full / team_only / opp_only / no_data). Returns a dict with the
    classification + date + opponent identity + final URL, or None on a
    fetch error.

    team_id and opp_index are required for the 4-bucket classification; if
    either is missing we fall back to the legacy True/False semantics so
    older callers keep working."""
    time.sleep(DELAY)
    url = (f"https://www.maxpreps.com/local/stats/boxscore.aspx?contestid={guid}&ssid={ssid}"
           if guid and ssid else game_url)
    # Shared transport (curl_cffi → system curl → requests): this worker issues
    # the bulk of the pipeline's requests, so it uses the same browser-like
    # chain as the box-score scraper rather than plain requests.
    from scrape_box_scores import _http_get_page, _with_stats_tab
    try:
        status, html, final_url = _http_get_page(url, timeout=20, allow_redirects=True)
        if status != 200 or not html: return None

        # MaxPreps' 2026 redesign defaults the game page to its Recap tab;
        # the per-player stat tables (what _classify_game needs) only render
        # under Stats. Re-fetch the canonical (redirected) URL with
        # ?tab=stats explicitly selected — see scrape_box_scores._with_stats_tab.
        stats_url = _with_stats_tab(final_url)
        if stats_url != final_url:
            time.sleep(DELAY)
            st2, html2, final2 = _http_get_page(stats_url, timeout=20, allow_redirects=True)
            if st2 == 200 and html2:
                html, final_url = html2, (final2 or stats_url)

        soup = BeautifulSoup(html, "html.parser")
        if team_id is None:
            # Legacy mode — preserve old True/False behaviour for any caller
            # that hasn't been migrated.
            return _check_soup(soup, team_name)
        return _classify_game(soup, final_url, team_name, team_id, opp_index)
    except Exception: return None

# ─── Save / Output ────────────────────────────────────────────────────────────

def scorestream_url(team_name, state_name):
    return f"https://scorestream.com/search?q={quote_plus(team_name + ' ' + state_name + ' high school basketball')}"

def google_search_url(team_name, city, state_name):
    q = f'"{team_name}" {city} {state_name} high school basketball schedule stats'
    return f"https://www.google.com/search?q={quote_plus(q)}"

def _save_gaps(output_file, state_name, state_code, total_count, full_data, partial_data, no_data, errors, processed_teams, sport="boys", season="2025-2026"):
    total_games_checked = sum(t["gamesChecked"] for t in full_data + partial_data + no_data)
    output = {
        "meta": {
            "state": state_name, "stateCode": state_code,
            "sport": f"{sport.title()} Basketball", "season": season,
            "totalTeams": total_count, "processedTeamsCount": len(processed_teams),
            "processedTeams": list(processed_teams), "totalGamesChecked": total_games_checked,
            "teamsFullBoxScores": len(full_data), "teamsPartialBoxScores": len(partial_data),
            "teamsNoBoxScores": len(no_data), "errors_count": len(errors),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "teamsFullBoxScores": sorted(full_data, key=lambda x: x["teamName"]),
        "teamsPartialBoxScores": sorted(partial_data, key=lambda x: x["teamName"]),
        "teamsNoBoxScores": sorted(no_data, key=lambda x: x["teamName"]),
        "errors": errors,
    }
    # Atomic write: avoid leaving a half-written file if interrupted mid-save.
    tmp = output_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    os.replace(tmp, output_file)

# ─── Main ─────────────────────────────────────────────────────────────────────

def _running_under_streamlit():
    """True when this script is being executed by `streamlit run`.

    Streamlit executes the target script with __name__ == "__main__", so if a
    deployment points its 'Main file path' at THIS file instead of
    streamlit_app.py, main() runs headless: argparse silently falls back to
    its defaults (TX/boys/2025-2026) and a full state scrape starts, while the
    page renders completely blank because nothing here imports Streamlit.
    That failure is silent and expensive, so detect it and refuse.
    """
    # Check sys.modules first: under `streamlit run` the runtime has already
    # imported streamlit, whereas importing it here during a normal CLI run
    # would emit a "missing ScriptRunContext" warning into the pipeline logs
    # that streamlit_app.py parses for progress.
    if "streamlit" not in sys.modules:
        return False
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


def main():
    if _running_under_streamlit():
        try:
            import streamlit as st
            st.error(
                "**Wrong entry point.** `app.py` is the command-line gap "
                "finder — it has no user interface, which is why this page is "
                "blank.\n\n"
                "Set the app's **Main file path** to `streamlit_app.py` and "
                "reboot."
            )
            st.caption(
                "Left running, this file would have started an unattended "
                "scrape using its default arguments (TX / boys / 2025-2026)."
            )
        except Exception:
            pass
        return

    parser = argparse.ArgumentParser(description="Parallel HS Basketball Box Score Gap Finder")
    parser.add_argument("--state", default=os.environ.get("STATE", "TX"), help="State code (default: TX)")
    parser.add_argument("--sport", default=os.environ.get("SPORT", "boys"), choices=["boys", "girls"], help="boys (default) or girls")
    parser.add_argument("--season", default=os.environ.get("SEASON", "2025-2026"), help="Season (e.g., 2025-2026 or 25-26)")
    parser.add_argument("--level", default=os.environ.get("LEVEL", "varsity"),
                        choices=["varsity", "jv", "freshman"],
                        help="Team level (default: varsity). MaxPreps nests it "
                             "after the gender: /basketball/girls/jv/…")
    parser.add_argument("--gap-only", action="store_true",
                        help="Only run gap-finder; skip the auto-chained box-score "
                             "scraper. Use this when an external orchestrator (e.g. "
                             "APP/pipeline.py) drives later stages explicitly.")
    args = parser.parse_args()

    state_code  = args.state.upper()
    state_lower = state_code.lower()
    state_name  = STATE_NAMES.get(state_code, state_code)
    
    # Normalise season for input file lookup (e.g. 25-26)
    short_season = args.season

    sport_label = args.sport.lower()
    # Input from state_teams_counter: boys_basketball_all_states_25-26.json
    APP_DIR = os.path.dirname(os.path.abspath(__file__))

    # Look for input file in app directory (bundled with repo)
    input_file = os.path.join(APP_DIR, f"{sport_label}_basketball_all_states_{short_season}.json")

    if not os.path.exists(input_file):
        # Try without season suffix (e.g. boys_basketball_all_states.json)
        fallback = os.path.join(APP_DIR, f"{sport_label}_basketball_all_states.json")
        if os.path.exists(fallback):
            input_file = fallback
        else:
            print(f"Team list missing. Running state_teams_counter...")
            state_teams_counter.run(sport=args.sport, season=short_season)
            generated = os.path.join(DATA_DIR, f"{sport_label}_basketball_all_states_{short_season}.json")
            if os.path.exists(generated):
                input_file = generated
            else:
                print(f"Error: Input file not found.")
                sys.exit(1)

    # Output: tx_data_gaps_boys_2025_2026.json
    season_fn = args.season.replace("-", "_")
    # Level suffix keeps varsity filenames byte-identical to before, so existing
    # varsity outputs and any downstream consumers are unaffected.
    from scrape_box_scores import normalise_level, level_file_suffix, apply_level
    level = normalise_level(args.level)
    lvl_sfx = level_file_suffix(level)
    output_file = os.path.join(DATA_DIR, f"{state_lower}_data_gaps_{sport_label}{lvl_sfx}_{season_fn}.json")
    
    with open(input_file, encoding="utf-8") as f: data = json.load(f)
    if state_code not in data.get("byState", {}):
        print(f"Error: State {state_code} not found."); sys.exit(1)

    state_regions = data["byState"][state_code]["regions"]
    # Dedup by teamUrl in case the master list has duplicate URL entries across regions.
    # Use URL-derived name (e.g. 'Avinger Indians') so the page parser can match
    # the team header even when the stored name is stale (e.g. just 'Avinger').
    seen_urls = set()
    all_teams = []
    for r, d in state_regions.items():
        for t in d["teams"]:
            url = t.get("teamUrl", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            # The master list only holds varsity URLs; JV/freshman live at the
            # same path plus a level segment. Stages 2 and 3 both derive their
            # URLs from the teamUrl we write here, so applying the level once
            # at enumeration makes the whole downstream pipeline level-aware.
            all_teams.append({
                "teamName": name_from_url(url, t.get("teamName", "")),
                "teamUrl": apply_level(url, level),
                "region": r,
            })
    total = len(all_teams)

    # Every team gets a full pass every run — cheap (one schedule request
    # each) and necessary to catch newly-added games on an in-progress
    # season. What makes a daily re-run efficient instead of a full
    # re-scrape is the per-game cache below: any game we already have real
    # stats for is reused without hitting the box-score page again. Only new
    # games and previously no_data (possibly since-played) games get
    # (re-)checked live. See _build_game_cache / _dedupe_games_by_date_opponent.
    full_data, partial_data, no_data, errors, processed_teams = [], [], [], [], set()
    game_cache = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
            game_cache = _build_game_cache(existing)
            print(f"Loaded {len(game_cache)} already-checked games from the existing "
                  f"file — only new or still-unresolved games will be (re-)fetched.")
        except Exception as e:
            print(f"  [WARN] Could not load existing file for game cache: {e}")

    # Normalise the season into '24-25'-style URL segment for the schedule fetch.
    # Without this the gap finder asks MaxPreps for the schedule at the path-less
    # URL, which silently falls back to the CURRENT season — so passing
    # --season 2024-2025 would still scrape 2025-2026 games.
    season_suffix = _short_season(args.season)
    print(f"Season URL suffix: {season_suffix or '(current — no suffix)'}")
    print(f"Level            : {level}"
          + ("" if level == "varsity" else f" (URL segment /{level})"))

    # Build the opponent canonical-name index ONCE, then pass it into every
    # Phase 2 worker. The gap-finder's 4-bucket per-game classification uses
    # the same scrape_box_scores parser, so opponents are identified
    # canonically (full team_id + full team_name) in the gaps file too.
    from scrape_box_scores import _get_opp_index
    opp_index = _get_opp_index(args.sport, args.season)

    # Every team goes through Phase 1 every run (see comment above) — no
    # team-level filtering. The game_cache is what keeps a re-run cheap.
    teams_to_process = all_teams
    if teams_to_process:
        # Phase 1: Schedules
        print(f"Phase 1: Fetching {len(teams_to_process)} schedules ({SCHED_WORKERS} workers)...")
        sched_results = {}
        with ThreadPoolExecutor(max_workers=SCHED_WORKERS) as pool:
            futures = {pool.submit(fetch_sched_worker, t, season_suffix): t for t in teams_to_process}
            for i, fut in enumerate(as_completed(futures), 1):
                # Per-future try/except: a single failure must not abort the loop
                # and silently drop every remaining team's schedule result.
                try:
                    team, entries, overall_record = fut.result()
                except Exception as e:
                    orig_team = futures[fut]
                    print(f"  [WARN] Schedule worker crashed for {orig_team['teamName']}: {e}")
                    sched_results[orig_team["teamUrl"]] = (orig_team, None, None)
                    continue
                # Key by teamUrl (unique) not teamName — duplicate names would silently
                # overwrite each other causing teams to be skipped entirely.
                sched_results[team["teamUrl"]] = (team, entries, overall_record)
                if i % 100 == 0 or i == len(teams_to_process):
                    print(f"  Schedules: {i}/{len(teams_to_process)} done")

        # Safety net: every team submitted must produce a sched_results entry,
        # otherwise it would never reach Phase 2 and be silently lost.
        for t in teams_to_process:
            if t["teamUrl"] not in sched_results:
                print(f"  [WARN] No sched_result for {t['teamName']} — recording as error.")
                sched_results[t["teamUrl"]] = (t, None, None)

        # Phase 2: Game checks
        print(f"Phase 2: Checking games in parallel ({GAME_WORKERS} workers)...")
        agg_lock = threading.Lock()
        game_jobs = []
        for turl, (team, entries, overall_record) in sched_results.items():
            if entries is None:
                # Record error but DO NOT add to processed_teams — next run should retry.
                errors.append({"teamName": team["teamName"], "teamUrl": team["teamUrl"], "region": team["region"]})
            elif not entries:
                city_m = re.search(rf"/{state_lower}/([^/]+)/", team["teamUrl"])
                city = city_m.group(1).replace("-", " ").title() if city_m else state_name
                no_data.append({"teamName": team["teamName"], "teamUrl": team["teamUrl"], "region": team["region"],
                                "gamesChecked": 0, "gamesWithStats": 0, "gamesMissing": 0,
                                "alternativeSources": {"scoreStream": scorestream_url(team["teamName"], state_name),
                                                     "googleSearch": google_search_url(team["teamName"], city, state_name)}})
                processed_teams.add(team_url_to_path(team["teamUrl"]))
            else:
                game_jobs.append({'team': team, 'entries': entries, 'overall_record': overall_record})

        def process_team_games(job):
            # Outer try/except: a single team failure must not kill the pool and
            # silently drop every subsequent team's result.
            try:
                team, entries = job['team'], job['entries']
                overall_record = job.get('overall_record')
                t_id   = team_url_to_path(team["teamUrl"])
                t_name = team["teamName"]
                # Per-game classification buckets — populated by check_game_worker
                # using the same parser as scrape_box_scores so the gap-finder
                # numbers match what the downstream scraper will actually capture.
                full_games:     list = []
                team_only_games: list = []
                opp_only_games:  list = []
                no_data_games:   list = []
                for url, guid, ssid in entries:
                    # Cache hit: a previous run already found real stats for
                    # this exact contest_id — reuse it, no HTTP request.
                    # Never cached: no_data games always get (re-)checked
                    # live, since an unplayed/unstatted game today can have
                    # a result by the next run.
                    cached = game_cache.get((t_id, guid)) if guid else None
                    if cached is not None:
                        cls, game_rec = cached
                    else:
                        res = check_game_worker(url, guid, ssid, t_name, t_id, opp_index)
                        if res is None or not isinstance(res, dict):
                            continue   # fetch error or legacy bool — skip
                        game_rec = {
                            "date":               res.get("date", ""),
                            "opponent_team_id":   res.get("opponent_team_id", ""),
                            "opponent_team_name": res.get("opponent_team_name", ""),
                            "url":                res.get("url", url),
                            "stats_fingerprint":  res.get("stats_fingerprint"),
                        }
                        cls = res.get("classification", "no_data")
                    if   cls == "full":      full_games.append(game_rec)
                    elif cls == "team_only": team_only_games.append(game_rec)
                    elif cls == "opp_only":  opp_only_games.append(game_rec)
                    else:                    no_data_games.append(game_rec)

                # Collapse MaxPreps' stale duplicate contest_ids for the same
                # matchup before counting — see _dedupe_games_by_date_opponent.
                full_games, team_only_games, opp_only_games, no_data_games = \
                    _dedupe_games_by_date_opponent(full_games, team_only_games,
                                                    opp_only_games, no_data_games, t_name)

                games_with_stats = len(full_games) + len(team_only_games) + len(opp_only_games)
                games_missing    = len(no_data_games)
                games_checked    = games_with_stats + games_missing

                # Prefer MaxPreps' own season W-L record (server-computed from
                # completed games) over our schedule-row count whenever it's
                # available and not obviously wrong. It's immune to duplicate-
                # contest-id artifacts our (date, opponent) dedup can miss —
                # e.g. a reschedule that shifts the calendar date, not just
                # the time — so it catches cases the dedup above doesn't.
                # Never let it undercut games we've actually confirmed have
                # stats.
                if overall_record is not None and overall_record >= games_with_stats:
                    games_checked = overall_record
                    games_missing = max(0, games_checked - games_with_stats)

                entry = {
                    "teamName":       t_name,
                    "teamUrl":        team["teamUrl"],
                    "region":         team["region"],
                    "gamesChecked":   games_checked,
                    "gamesWithStats": games_with_stats,
                    "gamesMissing":   games_missing,
                    "recordGamesPlayed": overall_record,
                    "fullDataGames": {
                        "count": len(full_games),
                        "note":  "Both teams entered stats.",
                        "games": full_games,
                    },
                    "teamOnlyDataGames": {
                        "count": len(team_only_games),
                        "note":  "Only THIS team entered stats; opponent did not.",
                        "games": team_only_games,
                    },
                    "opponentOnlyDataGames": {
                        "count": len(opp_only_games),
                        "note":  "Only the OPPONENT entered stats; this team did not.",
                        "games": opp_only_games,
                    },
                    "noDataGames": {
                        "count": len(no_data_games),
                        "note":  "NEITHER team entered any stats.",
                        "games": no_data_games,
                    },
                }

                with agg_lock:
                    # Top-level team bucket (existing semantics, unchanged):
                    #  - full_data:     every checked game has SOME stats
                    #  - partial_data:  some games have stats, some don't
                    #  - no_data:       zero games have stats
                    if games_checked == 0:           no_data.append(entry)
                    elif games_missing == 0:         full_data.append(entry)
                    elif games_with_stats > 0:       partial_data.append(entry)
                    else:                            no_data.append(entry)
                    processed_teams.add(team_url_to_path(team["teamUrl"]))

                    # Print frequent progress
                    tdone = len(processed_teams)
                    pct = tdone / total * 100
                    print(f"  [{tdone:>4}/{total}] {pct:5.1f}% | Full: {len(full_data):>4} | Part: {len(partial_data):>4} | {team['teamName']}")

                    if tdone % 10 == 0 or tdone == total:
                        try:
                            _save_gaps(output_file, state_name, state_code, total, full_data, partial_data, no_data, errors, processed_teams, args.sport, args.season)
                        except Exception as save_e:
                            print(f"  [WARN] Periodic save failed: {save_e}")
            except Exception as e:
                team = job.get('team', {})
                print(f"  [ERROR] process_team_games crashed for {team.get('teamName', '?')}: {e}")
                with agg_lock:
                    errors.append({"teamName": team.get("teamName", ""), "teamUrl": team.get("teamUrl", ""),
                                   "region": team.get("region", ""), "stage": "phase2", "error": str(e)})

        with ThreadPoolExecutor(max_workers=GAME_WORKERS) as pool:
            list(pool.map(process_team_games, game_jobs))

        _save_gaps(output_file, state_name, state_code, total, full_data, partial_data, no_data, errors, processed_teams, args.sport, args.season)

        # Final reconciliation: any team in the master list that is not in
        # processed_teams AND not in errors is a silently-dropped team. Surface them.
        all_paths = {team_url_to_path(t["teamUrl"]) for t in all_teams}
        error_paths = {team_url_to_path(e["teamUrl"]) for e in errors if e.get("teamUrl")}
        missing = all_paths - processed_teams - error_paths
        if missing:
            print(f"\n[WARNING] {len(missing)} teams were not processed and have no error record:")
            for p in list(missing)[:20]:
                print(f"    {p}")
            if len(missing) > 20:
                print(f"    ... and {len(missing) - 20} more")
            print("Re-run the command to retry these teams.")
        print(f"\nGap analysis complete for {total} teams. Processed: {len(processed_teams)}, Errors: {len(errors)}, Missing: {len(missing)}.")
    else:
        print(f"Gap analysis already complete for {total} teams. Proceeding to next steps...")

    if total == 0:
        print(f"\n[WARNING] No teams found for {state_name} ({args.sport}) in season {args.season}.")
        print("This usually means MaxPreps hasn't posted the leagues for this season yet.")
        sys.exit(0)

    print(f"\nSaved {total} teams to {output_file}.")

    # Skip the auto-chained box-score scraper when an external orchestrator
    # is driving the pipeline (e.g. APP/pipeline.py wires the stages itself).
    if getattr(args, 'gap_only', False):
        print("[--gap-only] Skipping auto-chained scraper. Stop.")
        return

    print("Starting scraper...")

    # Auto-run Scraper
    try:
        from scrape_box_scores import run as scrape_run
        box_scores_out = output_file.replace("data_gaps", "box_scores")
        scrape_run(input_file=output_file, output_file=box_scores_out,
                   sport=args.sport, season=args.season, level=level)
    except Exception as e: print(f"Scraper failed: {e}")

if __name__ == "__main__":
    main()
