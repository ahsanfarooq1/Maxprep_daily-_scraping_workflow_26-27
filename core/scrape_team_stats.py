"""
MaxPreps Authoritative Team Stats Fetcher
=========================================
For each team in the master list, fetches the coach-edited SEASON STATS
straight from MaxPreps' print-stats endpoint and produces a season-totals
file that matches what the website shows on the team's "Stats" tab — GP,
PPG, RPG, etc. — including games where the per-game box score wasn't
uploaded but a season aggregate was.

Why this exists: the per-game accumulator (Accumulation_data.py) sums
individual box scores, so it under-counts any game where the coach skipped
the per-game upload but DID enter a season total. The print-stats page is
the same data MaxPreps renders on the website, so it's authoritative.

Workflow per team:
  1. Fetch the team's HTML stats page (also works for past seasons).
  2. Extract schoolid + ssid from the page's hyperlinks.
  3. Hit:
       https://www.maxpreps.com/print/team_stats.aspx
         ?admin=0&bygame=0&league=0&print=1&schoolid={SID}&ssid={SSID}
  4. Parse the 5 tables (averages, shooting, detailed shooting, totals,
     ratios) into per-player + season-total records.

Output: {state}_authoritative_stats_{sport}_{season}.json
  {
    "meta": { state, sport, season, totals, processedTeams, ... },
    "records": [ {team_id, team_name, record_type, Name, GP, PPG, ...}, ... ]
  }

Usage:
  python scrape_team_stats.py --state TX --sport girls --season 2025-2026
  python scrape_team_stats.py --state TX --sport boys  --season 2024-2025
  python scrape_team_stats.py --state TX --sport girls --workers 20
"""

import os
import re
import sys
import json
import time
import argparse
import threading
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR     = os.environ.get("DATA_DIR", ".")
TEAM_WORKERS = 15
DELAY        = 0.4            # per-request delay per worker

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.maxpreps.com/",
}

# Map MaxPreps' print-page column headers -> our internal field names.
# Same field names the per-game accumulator (Accumulation_data.py) uses so
# downstream consumers can swap data sources without changing their code.
HEADER_MAP = {
    'GP': 'GP', 'MPG': 'MPG', 'PPG': 'PPG',
    'DEFR': 'DEFR', 'OFFR': 'OFFR', 'RPG': 'RPG', 'APG': 'APG',
    'SPG': 'SPG', 'BPG': 'BPG', 'TPG': 'TPG', 'PFPG': 'PFPG',
    'Min': 'Min', 'Pts': 'Pts',
    'FGM': 'FGM', 'FGA': 'FGA', 'FG%': 'FG%', 'PPS': 'PPS', 'AFG%': 'AFG%',
    '3PM': '3PM', '3PA': '3PA', '3P%': '3P%',
    'FTM': 'FTM', 'FTA': 'FTA', 'FT%': 'FT%',
    '2FGM': '2FGM', '2FGA': '2FGA', '2FG%': '2FG%',
    'OReb': 'OReb', 'DReb': 'DReb', 'Reb': 'Reb',
    'Ast': 'Ast', 'Stl': 'Stl', 'Blk': 'Blk', 'TO': 'TO', 'PF': 'PF',
    'Ast:TO': 'Ast:TO', 'Stl:TO': 'Stl:TO',
    'Stl:F': 'Stl:PF', 'Blk:PF': 'Blk:PF',
    'Chr': 'Chr', 'Defl': 'Defl', 'TF': 'TF', 'DD': 'DD', 'TD': 'TD',
}

# Timestamped print.
_original_print = print
def print(*args, **kwargs):  # noqa: A001
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)


# ─── HTTP session (thread-local) ─────────────────────────────────────────────

_tls = threading.local()

def _session():
    if not hasattr(_tls, 's'):
        s = requests.Session()
        retry = Retry(
            total=4, backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET'], raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('https://', adapter)
        s.mount('http://',  adapter)
        s.headers.update(HEADERS)
        _tls.s = s
    return _tls.s


# ─── Helpers ────────────────────────────────────────────────────────────────

def _short_season(season):
    """'2024-2025' or '24-25' → '24-25'. None/empty → None."""
    if not season:
        return None
    m = re.match(r'^(?:20)?(\d{2})-(?:20)?(\d{2})$', season.strip())
    return f"{m.group(1)}-{m.group(2)}" if m else season


def _team_url_to_id(team_url):
    """https://www.maxpreps.com/tx/.../basketball/girls/ -> tx/.../basketball/girls"""
    return re.sub(r"https?://(?:www\.)?maxpreps\.com/", "", team_url).rstrip('/')


def _name_from_url(team_url, fallback=""):
    """Recover the full team name (with mascot) from the URL slug."""
    m = re.match(r"https?://(?:www\.)?maxpreps\.com/([^/]+)/([^/]+)/([^/]+)/", team_url)
    if m:
        slug = m.group(3).replace("-", " ").title()
        if slug:
            return slug.replace("Aandm", "A&M").replace("aandm", "a&m")
    return fallback


def _safe_num(text):
    """Convert a stat-cell value to int/float, or None for blank/dash cells."""
    if text is None:
        return None
    t = text.strip()
    if not t or t in ('-', '—', '–'):
        return None
    try:
        f = float(t)
        return int(f) if f == int(f) else f
    except (TypeError, ValueError):
        return None


# ─── Discover schoolid + ssid from the team's stats page ────────────────────

# Two patterns — in MaxPreps' HTML they appear in either order across links.
_ID_PATTERNS = [
    re.compile(r'schoolid=([a-f0-9-]{8,40})[^"\'<>]{0,200}?ssid=([a-f0-9-]{8,40})', re.I),
    re.compile(r'ssid=([a-f0-9-]{8,40})[^"\'<>]{0,200}?schoolid=([a-f0-9-]{8,40})', re.I),
]


def _discover_ids(team_url, season_suffix=None):
    """Returns (schoolid, ssid) or (None, None) on failure."""
    base = team_url.rstrip('/')
    url = f"{base}/{season_suffix}/stats/" if season_suffix else f"{base}/stats/"
    try:
        time.sleep(DELAY)
        r = _session().get(url, timeout=20)
        if r.status_code != 200:
            return None, None
        body = r.text
    except Exception:
        return None, None

    m = _ID_PATTERNS[0].search(body)
    if m:
        return m.group(1), m.group(2)
    m = _ID_PATTERNS[1].search(body)
    if m:
        return m.group(2), m.group(1)
    return None, None


# ─── Fetch + parse the print-stats page ─────────────────────────────────────

def _fetch_print_stats_html(schoolid, ssid):
    url = (f"https://www.maxpreps.com/print/team_stats.aspx"
           f"?admin=0&bygame=0&league=0&print=1"
           f"&schoolid={schoolid}&ssid={ssid}")
    try:
        time.sleep(DELAY)
        r = _session().get(url, timeout=25)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def _parse_print_stats(html):
    """Parse the print-stats HTML into per-player + season-total dicts.

    Returns (per_player, season_total, status) where:
      per_player    = { 'M. Sebek(So)': {'GP': 37, 'PPG': 5.3, ...}, ... }
      season_total  = {'GP': 37, 'PPG': ..., ...}
      status        = 'has_data' | 'empty' | 'unparseable'
    """
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    if not tables:
        return {}, {}, 'unparseable'

    per_player: dict[str, dict] = {}
    season_total: dict = {}
    found_any = False

    for t in tables:
        rows = t.find_all('tr')
        if not rows:
            continue
        # Headers = first row's cells; only consider tables that look like stat tables.
        header_cells = [c.get_text(strip=True) for c in rows[0].find_all(['th', 'td'])]
        if not any(h in header_cells for h in ('GP', 'PPG', 'Pts', 'OReb', 'Ast:TO')):
            continue

        for row in rows[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
            if len(cells) < 2:
                continue
            first = cells[0].strip()
            second = cells[1].strip() if len(cells) > 1 else ''

            if first.isdigit() and second:
                # Player row: # | Athlete Name | stat columns…
                name = second
                target = per_player.setdefault(name, {'Name': name})
            elif first.lower() == 'season totals' or second.lower() == 'season totals':
                target = season_total
                # The "Season Totals" row sometimes has the label in cells[0]
                # without a numeric #, so we need to align cell→header offset.
                # Skip ahead so we start aligning from the column after the label.
            else:
                continue

            # Walk header columns and grab the matching cell values.
            for i, h in enumerate(header_cells):
                if i >= len(cells):
                    break
                if i < 2:
                    continue           # skip # and Athlete Name
                key = HEADER_MAP.get(h)
                if not key:
                    continue
                val = _safe_num(cells[i])
                if val is not None:
                    target[key] = val
            found_any = True

    status = 'has_data' if (found_any and (per_player or season_total)) else 'empty'
    return per_player, season_total, status


# ─── Per-team worker ─────────────────────────────────────────────────────────

def _process_team(team, season_suffix=None):
    """Fetch and parse one team's print-stats page.

    Returns (team_id, team_name, status, records) where:
      status   = 'has_data' | 'empty' | 'ids_missing' | 'fetch_failed' | 'unparseable'
      records  = list of dicts ready for the final output (empty if no stats)
    """
    team_url  = team.get('teamUrl', '')
    team_id   = _team_url_to_id(team_url)
    team_name = _name_from_url(team_url, team.get('teamName', ''))

    schoolid, ssid = _discover_ids(team_url, season_suffix)
    if not schoolid or not ssid:
        return team_id, team_name, 'ids_missing', []

    html = _fetch_print_stats_html(schoolid, ssid)
    if html is None:
        return team_id, team_name, 'fetch_failed', []

    per_player, season_total, status = _parse_print_stats(html)
    if status == 'unparseable':
        return team_id, team_name, 'unparseable', []
    if not per_player and not season_total:
        return team_id, team_name, 'empty', []

    # Build output records: one team_total, plus one per player.
    records: list[dict] = []
    if season_total:
        rec = {
            'team_id':     team_id,
            'team_name':   team_name,
            'schoolId':    schoolid,
            'ssid':        ssid,
            'record_type': 'team_total',
            'Name':        'Season Totals',
        }
        rec.update(season_total)
        records.append(rec)
    for pname, pstats in per_player.items():
        rec = {
            'team_id':     team_id,
            'team_name':   team_name,
            'schoolId':    schoolid,
            'ssid':        ssid,
            'record_type': 'player',
            'Name':        pname,
        }
        rec.update(pstats)
        records.append(rec)

    return team_id, team_name, 'has_data', records


# ─── Output helper ───────────────────────────────────────────────────────────

def _save_atomic(out_file, state_code, sport_label, season, results):
    """Write the output file atomically.

    results is a dict keyed by team_id with shape:
      { team_id: { 'team_name', 'status', 'records'(list) } }
    """
    counts = {
        'has_data':      sum(1 for v in results.values() if v['status'] == 'has_data'),
        'empty':         sum(1 for v in results.values() if v['status'] == 'empty'),
        'ids_missing':   sum(1 for v in results.values() if v['status'] == 'ids_missing'),
        'fetch_failed': sum(1 for v in results.values() if v['status'] == 'fetch_failed'),
        'unparseable': sum(1 for v in results.values() if v['status'] == 'unparseable'),
    }
    all_records = []
    for tid, v in results.items():
        all_records.extend(v.get('records', []))

    output = {
        'meta': {
            'state':              state_code,
            'sport':              sport_label,
            'season':             season,
            'totalTeamsProcessed': len(results),
            'teamsWithStats':     counts['has_data'],
            'teamsWithoutStats':  counts['empty'],
            'teamsFailed': (counts['ids_missing'] + counts['fetch_failed']
                            + counts['unparseable']),
            'failureBreakdown':   counts,
            'totalRecords':       len(all_records),
            'lastUpdated':        time.strftime('%Y-%m-%d %H:%M:%S'),
            'processedTeams':     list(results.keys()),
        },
        'records': all_records,
    }

    tmp = out_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    os.replace(tmp, out_file)


# ─── Driver ──────────────────────────────────────────────────────────────────

def _load_master_for_state(sport, season, state_code):
    """Find the right master file and return the team list for `state_code`.

    Tries (in order):
      {sport}_basketball_all_states_{YY-YY}.json    (preferred)
      {sport}_basketball_all_states_{season}.json
      {sport}_basketball_all_states.json            (current-season cache)
    Returns (teams_list, master_file_path) or (None, None) if nothing found.
    """
    candidates = []
    short = _short_season(season)
    if short:
        candidates.append(f"{sport}_basketball_all_states_{short}.json")
    if season:
        candidates.append(f"{sport}_basketball_all_states_{season}.json")
    candidates.append(f"{sport}_basketball_all_states.json")

    for fname in candidates:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            sd = data.get('byState', {}).get(state_code.upper())
            if not sd:
                continue
            teams: list = []
            seen = set()
            for r, d in sd.get('regions', {}).items():
                for t in d.get('teams', []):
                    url = t.get('teamUrl')
                    if url and url not in seen:
                        seen.add(url)
                        teams.append(t)
            if teams:
                return teams, path
    return None, None


def run(state="TX", sport="boys", season="2025-2026", workers=None, output_file=None):
    if workers is None or workers <= 0:
        workers = TEAM_WORKERS

    sport_label = "Girls Basketball" if sport == "girls" else "Boys Basketball"
    season_suffix = _short_season(season)

    state_code = state.upper()
    state_lower = state.lower()
    teams, master_file = _load_master_for_state(sport, season, state_code)
    if teams is None:
        print(f"[ERROR] No master file found for {sport} {state_code} (season {season}).")
        print("        Run state_teams_counter.py first to build it.")
        return None

    if output_file is None:
        season_fn = (season or "current").replace("-", "_")
        output_file = os.path.join(DATA_DIR,
                                   f"{state_lower}_authoritative_stats_{sport}_{season_fn}.json")

    print(f"State              : {state_code} ({sport_label})")
    print(f"Season             : {season} (URL suffix: {season_suffix or '(current)'})")
    print(f"Master file        : {master_file}  ({len(teams)} teams)")
    print(f"Output             : {output_file}")
    print(f"Workers            : {workers}")
    print("-" * 70)

    # Resume: skip teams already in the existing output (per processedTeams).
    results: dict = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, encoding='utf-8') as f:
                existing = json.load(f)
            prev_records = existing.get('records', [])
            prev_processed = set(existing.get('meta', {}).get('processedTeams', []))
            # Rebuild results dict from the existing records.
            by_team: dict = {}
            for r in prev_records:
                tid = r.get('team_id')
                if not tid:
                    continue
                by_team.setdefault(tid, []).append(r)
            for tid in prev_processed:
                results[tid] = {
                    'team_name': (by_team.get(tid, [{}])[0].get('team_name', '')),
                    'status':    ('has_data' if by_team.get(tid) else 'empty'),
                    'records':   by_team.get(tid, []),
                }
            print(f"Resuming: {len(results)} team(s) already in output.")
        except Exception as e:
            print(f"[WARN] Could not load existing output for resume: {e}")
            results = {}

    todo = [t for t in teams if _team_url_to_id(t.get('teamUrl', '')) not in results]
    print(f"To process now     : {len(todo)} (of {len(teams)} total)")
    print()

    agg_lock = threading.Lock()
    done = 0
    counts_running = {'has_data': 0, 'empty': 0, 'ids_missing': 0, 'fetch_failed': 0, 'unparseable': 0}
    # Pre-fill running counts from resumed results.
    for v in results.values():
        counts_running[v['status']] = counts_running.get(v['status'], 0) + 1

    def commit(team_id, team_name, status, records):
        nonlocal done
        with agg_lock:
            done += 1
            results[team_id] = {'team_name': team_name, 'status': status, 'records': records}
            counts_running[status] = counts_running.get(status, 0) + 1
            tag = {
                'has_data':     'OK ',
                'empty':        'no-stats',
                'ids_missing':  'no-ids ',
                'fetch_failed': 'fail   ',
                'unparseable':  'parse  ',
            }.get(status, '???    ')
            n_players = max(0, len([r for r in records if r.get('record_type') == 'player']))
            print(f"  [{done:>4}/{len(todo)}] {tag} | players={n_players:>2} | "
                  f"running OK={counts_running['has_data']:>4} "
                  f"empty={counts_running['empty']:>4} "
                  f"err={counts_running['ids_missing']+counts_running['fetch_failed']+counts_running['unparseable']:>3} "
                  f"| {team_name}")
            # Periodic save every 10 teams.
            if done % 10 == 0 or done == len(todo):
                try:
                    _save_atomic(output_file, state_code, sport_label, season, results)
                except Exception as e:
                    print(f"  [WARN] Periodic save failed: {e}")

    if not todo:
        print("Nothing to do — every team is already in the output.")
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_process_team, t, season_suffix): t for t in todo
            }
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    team_id, team_name, status, records = fut.result()
                except Exception as e:
                    team_id = _team_url_to_id(t.get('teamUrl', ''))
                    team_name = _name_from_url(t.get('teamUrl', ''), t.get('teamName', ''))
                    status = 'fetch_failed'
                    records = []
                    print(f"  [ERROR] worker crashed for {team_name}: {e}")
                commit(team_id, team_name, status, records)

    # Final save (also rewrites meta with final totals).
    _save_atomic(output_file, state_code, sport_label, season, results)

    # ── Summary ─────────────────────────────────────────────────────────────
    final = {'has_data': 0, 'empty': 0, 'ids_missing': 0, 'fetch_failed': 0, 'unparseable': 0}
    for v in results.values():
        final[v['status']] = final.get(v['status'], 0) + 1
    fail_total = final['ids_missing'] + final['fetch_failed'] + final['unparseable']
    print()
    print("=" * 70)
    print(f"  Total teams in master:       {len(teams)}")
    print(f"  Successfully scraped (data): {final['has_data']}")
    print(f"  No stats uploaded by coach:  {final['empty']}")
    print(f"  Failed (ids/fetch/parse):    {fail_total}")
    if fail_total:
        print(f"    - schoolid/ssid not found: {final['ids_missing']}")
        print(f"    - print-page fetch failed: {final['fetch_failed']}")
        print(f"    - parse error:             {final['unparseable']}")
    print(f"  Output file: {output_file}")
    print("=" * 70)
    if fail_total:
        print("  Re-run this script to retry failed teams (resume is automatic).")
    return output_file


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state",   default="TX", help="State code (default: TX)")
    ap.add_argument("--sport",   default="boys", choices=["boys", "girls"])
    ap.add_argument("--season",  default="2025-2026",
                    help="Season (e.g. 2025-2026, 2024-2025, or 25-26)")
    ap.add_argument("--workers", type=int, default=TEAM_WORKERS,
                    help=f"Parallel worker count (default {TEAM_WORKERS})")
    ap.add_argument("--output",  default=None, help="Override output file path")
    args = ap.parse_args()
    run(state=args.state, sport=args.sport, season=args.season,
        workers=args.workers, output_file=args.output)


if __name__ == "__main__":
    main()
