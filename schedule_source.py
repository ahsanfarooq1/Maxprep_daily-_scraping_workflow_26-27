"""
schedule_source.py — step 1 of the daily workflow: who plays when?

Reads the master team list and each team's MaxPreps schedule, and turns every
schedule row into a "game ref" (a dict that gap_store.GapFile.upsert_discovered
turns into a game row). No box-score page is fetched here: the schedule row
already carries the contest id, ssid, game URL, scheduled datetime and both
teams, which is all that's needed to identify and date a game.

Schedule row layout (verified against live schedule.json, 2026-10):
    c[0]   [team_row, team_row]    both teams; team_row[13]=team URL,
                                   [14]=school name, [21]=mascot
    c[1]   contest id (GUID)
    c[11]  scheduled datetime 'YYYY-MM-DDTHH:MM:SS'
    c[14]  this team's season id (ssid)
    c[18]  game URL (.../game/{a}-vs-{b}/{MM-DD-YYYY}/?c={contest})
    c[21]  contest type ('Game', ...)
"""

import re
import json
import time

from wf_core import legacy, date_from_url, date_from_value, log
from gap_store import game_key, contest_id_from_url

NULL_GUID = "00000000-0000-0000-0000-000000000000"
_TEAM_PATH_RE = re.compile(r"https?://(?:www\.)?maxpreps\.com/", re.I)


def team_path(url):
    return _TEAM_PATH_RE.sub("", url or "").rstrip("/")


def team_path_from_row_url(url):
    """Team rows inside a schedule link to '.../basketball/25-26/schedule/';
    reduce that to the bare team path ('.../basketball')."""
    return re.sub(r"(/\d{2}-\d{2})?/schedule$", "", team_path(url))


# ─── Teams ───────────────────────────────────────────────────────────────────
def load_state_teams(master_path, state_code, level="varsity"):
    """Flatten one state's teams from the master list exactly the way app.py
    does (dedupe by URL, full name from the URL slug, level applied)."""
    bx, app = legacy()
    with open(master_path, encoding="utf-8") as f:
        data = json.load(f)
    if state_code not in data.get("byState", {}):
        raise SystemExit(f"State {state_code} not found in {master_path}")
    seen, teams = set(), []
    for region, d in data["byState"][state_code]["regions"].items():
        for t in d["teams"]:
            url = t.get("teamUrl", "")
            if not url or url in seen:
                continue
            seen.add(url)
            teams.append({"teamName": app.name_from_url(url, t.get("teamName", "")),
                          "teamUrl": bx.apply_level(url, level), "region": region})
    return teams


# ─── One team's schedule ─────────────────────────────────────────────────────
def _opponent_from_row(c, our_base_path):
    """(opp_team_path, opp_name) from the two team rows in c[0], or ('','')."""
    bx, _ = legacy()
    rows = c[0] if isinstance(c, list) and c and isinstance(c[0], list) else []
    others = []
    for tr in rows:
        if not (isinstance(tr, list) and len(tr) > 14 and isinstance(tr[13], str)):
            continue
        p = bx.strip_level(team_path_from_row_url(tr[13]))
        if p != our_base_path:
            name = " ".join(x for x in (tr[14], tr[21] if len(tr) > 21 else "") if x)
            others.append((p, name.strip()))
    return others[0] if len(others) == 1 else ("", "")


def refs_from_contests(contests, team, state, sport, season, level, opp_index=None):
    """Schedule contests -> list of game-ref dicts."""
    bx, _ = legacy()
    path = team_path(team["teamUrl"])
    base = bx.strip_level(path)
    team_ssid = next((c[14] for c in contests
                      if isinstance(c, list) and len(c) > 14 and c[14] and c[14] != NULL_GUID), "")
    refs, seen = [], set()
    for c in contests:
        if not (isinstance(c, list) and len(c) > 18):
            continue
        url = c[18]
        if not (isinstance(url, str) and url.startswith("https://")):
            continue
        cid = contest_id_from_url(url) or (c[1].lower() if isinstance(c[1], str) else "")
        key = game_key(path, cid, url)
        if key in seen:
            continue
        seen.add(key)

        sched = c[11] if len(c) > 11 else ""
        date = date_from_url(url) or date_from_value(sched)
        opp_path, opp_name = _opponent_from_row(c, base)
        if not opp_path:                                   # fall back to the URL slug
            slug = bx._opp_slug_from_url(url, base) or ""
            opp_path, opp_name = bx._resolve_opponent(slug, base, opp_index or {})
            opp_name = opp_name or ""
        if opp_path and "/" in opp_path:
            opp_path = bx.apply_level(opp_path, level)
        refs.append({
            "game_key": key, "state": state, "sport": sport, "season": season, "level": level,
            "team_id": path, "team_name": team["teamName"], "team_url": team["teamUrl"],
            "region": team.get("region", ""),
            "opponent_team_id": opp_path, "opponent_team_name": opp_name,
            "game_date": date, "game_url": url, "contest_id": cid,
            "ssid": (c[14] if len(c) > 14 and c[14] and c[14] != NULL_GUID else team_ssid) or "",
            "scheduled_time": sched if isinstance(sched, str) else "",
            "game_type": c[21] if len(c) > 21 and isinstance(c[21], str) else "",
        })
    return refs


def fetch_team_schedule(team, state, sport, season, level, opp_index=None):
    """-> dict(ok, refs, record, error). Mirrors the production schedule fetch
    (build-id refresh, 5xx/429 backoff) and falls back to the schedule page's
    HTML when schedule.json is refused, like scrape_box_scores does."""
    bx, app = legacy()
    path = team_path(team["teamUrl"])
    suffix = bx._short_season(season)
    bid, version = app.get_build_id()
    err = "schedule.json failed"
    for attempt in range(5):
        res = app._raw_fetch_schedule(bid, path, season_suffix=suffix)
        if res is None:
            err = "schedule.json returned a non-200 response"
            time.sleep(1 + attempt)
            continue
        if res == "_retry":
            err = "schedule.json timed out / 5xx / 429"
            time.sleep(min(2 ** attempt, 10))
            continue
        if isinstance(res, dict) and res.get("_expired"):
            bid, version = app.refresh_build_id(version)
            err = "build id expired"
            continue
        contests, record = res
        refs = refs_from_contests(contests, team, state, sport, season, level, opp_index)
        return {"ok": True, "refs": refs, "record": record, "error": ""}

    # Fallback: schedule page HTML (no scheduled-time / team rows; date from URL).
    try:
        entries = bx.fetch_game_entries(bid, path, season_suffix=suffix)
    except Exception as e:            # network failure
        return {"ok": False, "refs": [], "record": None, "error": f"{err}; fallback: {e}"}
    if isinstance(entries, list):
        refs = []
        base = bx.strip_level(path)
        for url, guid, ssid in entries:
            cid = (guid or contest_id_from_url(url) or "").lower()
            slug = bx._opp_slug_from_url(url, base) or ""
            opp_path, opp_name = bx._resolve_opponent(slug, base, opp_index or {})
            if opp_path and "/" in opp_path:
                opp_path = bx.apply_level(opp_path, level)
            refs.append({"game_key": game_key(path, cid, url), "state": state, "sport": sport,
                         "season": season, "level": level, "team_id": path,
                         "team_name": team["teamName"], "team_url": team["teamUrl"],
                         "region": team.get("region", ""), "opponent_team_id": opp_path,
                         "opponent_team_name": opp_name or "", "game_date": date_from_url(url),
                         "game_url": url, "contest_id": cid, "ssid": ssid or "",
                         "scheduled_time": "", "game_type": ""})
        return {"ok": True, "refs": refs, "record": None, "error": ""}
    return {"ok": False, "refs": [], "record": None, "error": f"{err}; HTML fallback found nothing"}
