"""Stats-tab (MaxPreps print-stats page) layer for the 2026-27 workflow.

Why: during the season a team's data is either missing or partial. The stats tab is the
coach-entered season total, so wherever it exists it WINS over what we accumulate from box scores
(same rule as the old pipeline's merge_all_stats_tab: stats-tab replaces the team's records;
guard: a stats-tab row with GP=0 never replaces box-score data that has games).

For each run it fetches the stats tab only for the teams that matter (see `pick_teams`) and stores
  {st}_all_stats_tab_{sport}_{YYYY_YYYY}.json   flat list of accumulated-format records
                                                (team_total + players), merge-compatible
and records per-team status on the gap file `teams` entry (stats_tab_status, ..._last_attempt,
..._last_success, ..._gp, ..._players). A team that had stats before is never downgraded by a
later empty/failed fetch. Legacy code is used read-only (accumulate_from_stats_tab._process_team).
"""
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from wf_core import log, now_str, parse_ts, CORE_DIR


def stab_path_for(box_path):
    return box_path.replace("_box_scores_", "_all_stats_tab_")


def final_path_for(folder, st, sport, season):
    ss = season[2:4] + "_" + season[7:9] if len(season) >= 9 else season
    return os.path.join(folder, f"Final_{st.lower()}_accumulated_{sport}_{ss}.json")


class StatsTabStore:
    """Flat list of accumulated-format records, replaced per team, saved atomically."""

    def __init__(self, path):
        self.path, self.records, self.dirty = path, [], False
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    d = json.load(f)
                self.records = d if isinstance(d, list) else []
            except Exception as e:
                os.replace(path, f"{path}.corrupt-{now_str().replace(':', '').replace(' ', '_')}")
                log(f"[WARN] unreadable stats-tab file set aside ({e})")

    def replace_team(self, team_id, records):
        self.records = [r for r in self.records if r.get("team_id") != team_id] + records
        self.dirty = True

    def team_ids(self):
        return {r["team_id"] for r in self.records if r.get("record_type") == "team_total"}

    def save(self):
        if not self.dirty and os.path.exists(self.path):
            return
        if not self.records and not os.path.exists(self.path):
            return                                   # no empty file on runs that found nothing
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.records, f, indent=4, ensure_ascii=False)
        os.replace(tmp, self.path)
        self.dirty = False


def pick_teams(gf, window, scope="window"):
    """Teams whose stats tab we (re)fetch: {team_url: team_name}.
    window = teams with a game dated inside the requested window ("search by date", default)
    played = every team with any game dated up to the window end
    all    = every team in the gap file"""
    lo, hi = window[0].isoformat(), window[1].isoformat()
    urls = {}
    for r in gf.games.values():
        if not r.get("on_schedule", True):
            continue
        d = r.get("game_date") or ""
        if scope == "all" or (scope == "played" and d and d <= hi) or (scope == "window" and lo <= d <= hi):
            urls[r["team_url"]] = r["team_name"]
    return urls


def fetch_team(team_url, tgc, season):
    """-> (name, records, status). Thin wrapper over the legacy per-team fetch."""
    if CORE_DIR not in sys.path:
        sys.path.insert(0, CORE_DIR)
    import accumulate_from_stats_tab as A
    from scrape_team_stats import _short_season
    return A._process_team(team_url, tgc, _short_season(season))


def run_stats_tab(gf, store, teams, season, workers=8, ttl_hours=6, force=False, now=None,
                  fetcher=None, max_fetch_fail=25):
    """Fetch the stats tab for `teams` ({team_url: name}); update store + gap-file team rows."""
    import datetime as dt
    now = now or dt.datetime.now()
    fetch = fetcher or fetch_team
    todo = []
    for url in teams:
        t = gf.teams.setdefault(url, {"teamUrl": url})
        last = parse_ts(t.get("stats_tab_last_attempt"))
        if not force and last and (now - last).total_seconds() < ttl_hours * 3600:
            continue
        todo.append(url)
    out = {"requested": len(teams), "fetched": 0, "has_data": 0, "no_stats": 0, "failed": 0,
           "skipped_recent": len(teams) - len(todo)}
    if not todo:
        return out
    tgc = {}
    for r in gf.games.values():
        if r.get("on_schedule", True) and (r.get("game_date") or "9") <= now.date().isoformat():
            tgc[r["team_url"]] = tgc.get(r["team_url"], 0) + 1
    log(f"Stats tab: fetching {len(todo)} teams ({workers} workers)")

    def one(url):
        try:
            return url, fetch(url, tgc.get(url, 0), season)
        except Exception as e:
            return url, (teams[url], [], f"error:{type(e).__name__}")

    streak = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(one, u) for u in todo]
        for i, fut in enumerate(as_completed(futs), 1):
            if fut.cancelled():
                continue
            url, (name, recs, status) = fut.result()
            t = gf.teams[url]
            t["stats_tab_last_attempt"] = now_str(now)
            out["fetched"] += 1
            if recs:
                tt = next((r for r in recs if r["record_type"] == "team_total"), {})
                store.replace_team(recs[0]["team_id"], recs)
                t.update(stats_tab_status="has_data", stats_tab_last_success=now_str(now),
                         stats_tab_gp=tt.get("GP"),
                         stats_tab_players=sum(r["record_type"] == "player" for r in recs))
                out["has_data"] += 1
                streak = 0
            elif status in ("empty", "unparseable"):
                if t.get("stats_tab_status") != "has_data":     # never downgrade a team that had stats
                    t["stats_tab_status"] = "no_stats"
                out["no_stats"] += 1
                streak = 0
            else:
                if t.get("stats_tab_status") != "has_data":
                    t["stats_tab_status"] = "failed:" + status
                out["failed"] += 1
                streak += 1
                if streak >= max_fetch_fail:
                    log(f"[STATS TAB] {streak} consecutive failures - stopping early (VPN?)")
                    for f in futs:
                        f.cancel()
                    break
            if i % 200 == 0:
                log(f"  stats tab {i}/{len(todo)}  has_data={out['has_data']}")
    store.save()
    log(f"Stats tab: {out['has_data']} with stats, {out['no_stats']} not uploaded, "
        f"{out['failed']} failed, {out['skipped_recent']} fetched recently (skipped)")
    return out
