"""team_levels.py - every team's LEVEL (classification: 2A, 4A, AAA, Division II, C1 ...) for the Final files.

Sources, tried in this order for each team (the first one that gives a value wins; `level_source` says which):
  1. state_division    MaxPreps' official field on the team page ("stateDivisionName": "Division 4A" -> "4A")
  2. section_division  the section's division on the team page (e.g. California "Division 5")
  3. league_name       the class written at the start of the league name ("5A Area 4" -> "5A", "Aaa Region 3" -> "AAA")
  4. none              MaxPreps itself shows no level for the team -> "level": null
Private leagues keep their own label ("TAPPS 2A", "PSAL", ...) so they are never mixed up with public 2A.

Stored per state + sport (small, kept in git; the level hardly ever changes, so a team is looked up once and then
only refreshed now and then):
    <State>/{st}_team_levels_{sport}_2026_2027.json
accumulate_stats.py writes "level" and "level_source" into EVERY record (team_total + players) of the Final file.

    python team_levels.py                        # all states, both sports: look up missing/old teams, update Final files
    python team_levels.py --states AL,TX --sport boys
    python team_levels.py --apply-only           # no web requests: only rewrite the Final files from the stored levels
    python team_levels.py --refresh              # look every team up again
Needs the VPN only for the lookups (one team page per team).
"""
import argparse
import datetime as dt
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import wf_core
from wf_core import log, now_str, parse_ts

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "Scraped data 2026-27")

REFRESH_DAYS = 30          # look a team up again after this long (levels are re-assigned at most once a season)
RETRY_NONE_DAYS = 7        # a team with no level on MaxPreps is checked again sooner (sections assign late)
MAX_FETCH_FAIL = 25        # consecutive failed requests -> stop (VPN off / network down); partial progress is kept

_BLOCK = re.compile(r'"leagueName":"([^"]*)","leagueCanonicalUrl":"[^"]*","stateDivisionId":"[^"]*",'
                    r'"stateDivisionName":"([^"]*)","sectionDivisionId":"[^"]*","sectionDivisionName":"([^"]*)"')
_SD = re.compile(r'"stateDivisionName":"([^"]*)"')
_SECD = re.compile(r'"sectionDivisionName":"([^"]*)"')
_LG = re.compile(r'"leagueName":"([^"]*)"')
_LEAGUE_CLASS = (
    re.compile(r"^\s*([1-7]A)\b", re.I),
    re.compile(r"^\s*(A{1,7})\b", re.I),                     # "Aaa Region 3" (letters only)
    re.compile(r"^\s*([CD][12])\b", re.I),
    re.compile(r"^\s*Division\s+(I{1,3}|IV|VI{0,2}|\d)\b", re.I),
)


def levels_path(folder, st, sport, season="2026-2027"):
    return os.path.join(folder, f"{st.lower()}_team_levels_{sport}_{wf_core.season_filename(season)}.json")


def clean_division(s):
    """'Division 4A' -> '4A'; 'Division TAPPS 2A' -> 'TAPPS 2A'; '' stays ''."""
    s = (s or "").replace("\\u0026", "&").strip()
    s = re.sub(r"^Division\s+", "", s, flags=re.I).strip()
    return s


def from_league_name(name):
    for rx in _LEAGUE_CLASS:
        m = rx.match(name or "")
        if m:
            return m.group(1).upper()
    return ""


def parse_page(html):
    """-> (state_division, section_division, league_name) as shown on the team page ('' when absent)."""
    m = _BLOCK.search(html)
    if m:
        return m.group(2), m.group(3), m.group(1)
    sd, secd, lg = _SD.search(html), _SECD.search(html), _LG.search(html)
    return (sd.group(1) if sd else ""), (secd.group(1) if secd else ""), (lg.group(1) if lg else "")


def resolve(state_div, section_div, league_name, master_region=""):
    """-> (level or None, source)."""
    v = clean_division(state_div)
    if v:
        return v, "state_division"
    v = clean_division(section_div)
    if v:
        return v, "section_division"
    for name in (league_name, master_region):
        v = from_league_name(name)
        if v:
            return v, "league_name"
    return None, "none"


class Store:
    """{team_id: {level, level_source, state_division, section_division, league, fetched_at}} for one state+sport."""

    def __init__(self, path):
        self.path, self.teams = path, {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    self.teams = json.load(f).get("teams", {})
            except Exception as e:
                os.replace(path, f"{path}.corrupt-{now_str().replace(':', '').replace(' ', '_')}")
                log(f"[WARN] unreadable team-levels file set aside ({e})")

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write('{"generated_at": %s,\n"teams": {\n' % json.dumps(now_str()))
            items = sorted(self.teams.items())
            for i, (k, v) in enumerate(items):
                f.write(json.dumps(k) + ": " + json.dumps(v, ensure_ascii=False) + (",\n" if i < len(items) - 1 else "\n"))
            f.write("}}\n")
        os.replace(tmp, self.path)


def team_id_of(team_url):
    return re.sub(r"^https?://(?:www\.)?maxpreps\.com/", "", team_url or "", flags=re.I).rstrip("/")


def _due(entry, now, refresh):
    if refresh or not entry or not entry.get("fetched_at"):
        return True
    t = parse_ts(entry["fetched_at"])
    if t is None:
        return True
    age = (now - t).days
    return age >= (RETRY_NONE_DAYS if entry.get("level_source") in ("none", "league_name") else REFRESH_DAYS)


def fetch_team(team_url):
    """-> (ok, state_division, section_division, league_name, error)."""
    bx, _ = wf_core.legacy()
    err = ""
    for attempt in range(3):
        try:
            status, text, _ = bx._http_get_page(team_url, timeout=30)
        except Exception as e:                        # network/DNS hiccup
            err = str(e)[:80]
            time.sleep(1 + attempt)
            continue
        if status == 200 and text:
            return (True,) + parse_page(text) + ("",)
        if status == 403:
            return False, "", "", "", "403 (VPN off / geo-block)"
        if status == 404:
            return False, "", "", "", "404 team page not found"
        err = f"HTTP {status}"
        time.sleep(1 + attempt)
    return False, "", "", "", err or "failed"


def ensure_levels(folder, st, sport, season, teams, workers=6, refresh=False, offline=False, fetcher=None):
    """Make sure `<folder>/{st}_team_levels_{sport}_....json` holds a level for each team in `teams`
    ([{teamUrl, region, ...}] from the master list). Looks up the missing / old ones on MaxPreps; for any team
    that cannot be looked up it keeps the older value, or falls back to the league name of the master list.
    Returns a one-line summary."""
    store = Store(levels_path(folder, st, sport, season))
    now = dt.datetime.now()
    fetch = fetcher or fetch_team
    region = {team_id_of(t["teamUrl"]): t.get("region", "") for t in teams}
    todo = [t for t in teams if _due(store.teams.get(team_id_of(t["teamUrl"])), now, refresh)] if not offline else []
    out = {"looked_up": 0, "failed": 0}
    lock, state = threading.Lock(), {"streak": 0, "stop": False}

    def one(t):
        if state["stop"]:
            return None
        tid = team_id_of(t["teamUrl"])
        res = fetch(t["teamUrl"])
        with lock:
            if res[0]:
                state["streak"] = 0
            else:
                state["streak"] += 1
                if state["streak"] >= MAX_FETCH_FAIL:
                    state["stop"] = True
        return tid, res

    done = 0
    if todo:
        log(f"Team levels {st.upper()} {sport}: looking up {len(todo)} teams ({workers} workers)")
        with ThreadPoolExecutor(workers) as ex:
            for r in ex.map(one, todo):
                if r is None:
                    continue
                tid, (ok, sd, secd, lg, err) = r
                if not ok:
                    out["failed"] += 1
                    continue
                level, src = resolve(sd, secd, lg, region.get(tid, ""))
                store.teams[tid] = {"level": level, "level_source": src, "state_division": sd, "section_division": secd,
                                    "league": lg, "fetched_at": now_str()}
                out["looked_up"] += 1
                done += 1
                if done % 300 == 0:
                    store.save()
        if state["stop"]:
            log(f"[WARN] team levels: {MAX_FETCH_FAIL} lookups in a row failed (VPN / network?) - stopped; "
                f"the rest keep their league-name level and are retried next run")
    for t in teams:                                    # no value yet (lookup failed / offline): league-name fallback
        tid = team_id_of(t["teamUrl"])
        if tid not in store.teams:
            level, src = resolve("", "", "", region.get(tid, ""))
            store.teams[tid] = {"level": level, "level_source": src, "state_division": "", "section_division": "",
                                "league": "", "fetched_at": ""}
    store.save()
    have = sum(1 for t in teams if store.teams[team_id_of(t["teamUrl"])]["level"])
    return (f"levels {st.upper()} {sport}: {have}/{len(teams)} teams have a level "
            f"(looked up {out['looked_up']}, failed {out['failed']})")


def apply_levels(final_path, folder, st, sport, season="2026-2027"):
    """Write "level" and "level_source" (right after team_name) into every record of a Final file.
    Returns (records, records_with_level). Teams not in the store (e.g. stats-tab-only) get level null."""
    store = Store(levels_path(folder, st, sport, season))
    with open(final_path, encoding="utf-8") as f:
        recs = json.load(f)
    n = with_level = 0
    for i, r in enumerate(recs):
        e = store.teams.get(r.get("team_id"), {})
        level, src = e.get("level"), e.get("level_source", "none")
        out = {}
        for k, v in r.items():
            if k in ("level", "level_source"):
                continue
            out[k] = v
            if k == "team_name":
                out["level"], out["level_source"] = level, src
        out.setdefault("level", level)
        out.setdefault("level_source", src)
        recs[i] = out
        n += 1
        with_level += 1 if level else 0
    tmp = final_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(recs, f, indent=4, ensure_ascii=False)      # readable on purpose: humans verify these files
    os.replace(tmp, final_path)
    return n, with_level


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", default=ROOT)
    ap.add_argument("--states", help="comma list of codes (default: all 30)")
    ap.add_argument("--sport", choices=["boys", "girls", "both"], default="both")
    ap.add_argument("--season", default="2026-2027")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--refresh", action="store_true", help="look every team up again")
    ap.add_argument("--apply-only", action="store_true", help="no web requests; only (re)write the Final files")
    a = ap.parse_args()
    sys.path.insert(0, HERE)
    from run_daily import STATES
    from daily_gapfinder import find_master
    from schedule_source import load_state_teams
    codes = [c.strip().upper() for c in a.states.split(",")] if a.states else list(STATES)
    sports = ["boys", "girls"] if a.sport == "both" else [a.sport]
    from stats_tab import final_path_for
    for code in codes:
        folder = os.path.join(a.data_root, STATES[code])
        os.makedirs(folder, exist_ok=True)
        for sp in sports:
            teams = load_state_teams(find_master(sp, a.season), code, "varsity")
            print(ensure_levels(folder, code, sp, a.season, teams, a.workers, a.refresh, a.apply_only), flush=True)
            final = final_path_for(folder, code, sp, a.season)
            if os.path.exists(final):
                n, w = apply_levels(final, folder, code, sp, a.season)
                print(f"  {os.path.basename(final)}: level written to {n} records ({w} with a level)", flush=True)
            else:
                print(f"  (no Final file yet for {code} {sp})", flush=True)


if __name__ == "__main__":
    main()
