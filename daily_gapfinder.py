"""
daily_gapfinder.py — date-driven Gap Finder (2026-27 workflow, phase 1)

Daily flow, per state/sport:
  1. read the master team list                          (schedule_source)
  2. load the state's Gap Finder file (or start one)    (gap_store)
  3. fetch every team's schedule; add NEW games dated in the window, reconcile
     known games (date/URL changes, games that left the schedule)
  4. select what needs an attempt: new games in the window + non-successful
     games whose retry back-off has elapsed             (gap_store.select_due)
  5. attempt them — one fetch per game                  (game_checker)
  6. record status / retry count / timestamp / log on every game row
  7. upsert successful records into the box-score dataset, no duplicates
                                                        (dataset_store)
  8. save the Gap Finder JSON + Excel tracker (checkpointing along the way)

Examples
  python daily_gapfinder.py --state TX --sport boys                  # yesterday
  python daily_gapfinder.py --state TX --sport both --date 2026-10-11
  python daily_gapfinder.py --state TX,OK --sport girls --date-from 2026-10-01 --date-to 2026-10-11
  python daily_gapfinder.py --state TX --sport boys --skip-schedule  # retries only
"""

import os
import sys
import argparse
import datetime as dt
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wf_core import (HERE, SUCCESS, NO_DATA, FAILED, PENDING, RETRY, legacy, log, now_str,
                     resolve_window, season_filename, short_season, today)
from gap_store import GapFile, Policy, select_due, record_attempt, apply_expiry, mark_duplicates
from dataset_store import BoxScoreDataset


MASTER_DIR = os.path.join(HERE, "Input Master File")


# ─── Paths ───────────────────────────────────────────────────────────────────
def paths_for(state, sport, season, level, out_dir):
    bx, app = legacy()
    sfx = bx.level_file_suffix(level)
    fn = season_filename(season)
    s = state.lower()
    return (os.path.join(out_dir, f"{s}_data_gaps_{sport}{sfx}_{fn}.json"),
            os.path.join(out_dir, f"{s}_box_scores_{sport}{sfx}_{fn}.json"))


def _shown_path(p):
    """Path relative to this folder for logging; the full path if it's on another drive."""
    try:
        return os.path.relpath(p, HERE)
    except ValueError:
        return p


def find_master(sport, season, explicit=None):
    from wf_core import CORE_DIR
    if explicit:
        return explicit
    ss = short_season(season)
    for d in (MASTER_DIR, HERE, CORE_DIR):          # "Input Master File" first, then legacy spots
        for name in (f"{sport}_basketball_all_states_{ss}.json", f"{sport}_basketball_all_states.json"):
            p = os.path.join(d, name)
            if os.path.exists(p):
                return p
    raise SystemExit(f"No master team list found for {sport} {season} in {MASTER_DIR}, {HERE} or {CORE_DIR}")


# ─── One state / sport ───────────────────────────────────────────────────────
def run_one(a, state, sport, schedule_fetcher=None, checker=None, now=None, as_of=None, stats_tab_fetcher=None):
    """Run the whole daily flow for one state+sport. `schedule_fetcher` and
    `checker` are injectable so the flow can be tested offline."""
    bx, app = legacy()
    now = now or dt.datetime.now()
    as_of = as_of or today()
    state = state.upper()
    state_name = app.STATE_NAMES.get(state, state)
    level = bx.normalise_level(a.level)

    explicit = bool(a.date or a.date_from or a.date_to)
    window = resolve_window(a.date, a.date_from, a.date_to, a.days_back, as_of)
    catch_up = 0 if explicit else a.catch_up_days
    disc_from = window[0] - dt.timedelta(days=catch_up)

    out_dir = a.output_dir or os.path.join(a.output_root, f"{state_name.replace(' ', '_')}_scraped_data")
    os.makedirs(out_dir, exist_ok=True)
    gap_path, box_path = paths_for(state, sport, a.season, level, out_dir)

    policy = Policy(max_attempts=a.max_attempts, nodata_backoff_h=a.nodata_backoff_hours,
                    failed_backoff_h=a.failed_backoff_hours, retry_window_days=a.retry_window_days,
                    ignore_backoff=explicit, force_exhausted=a.force_exhausted,
                    recheck_success=a.recheck_success, recheck_partial=a.recheck_partial,
                    recheck_contests={c.lower() for c in (a.recheck_contest or [])})

    log("=" * 78)
    log(f"{state} {sport} {a.season} level={level}  window {window[0]} .. {window[1]}"
        f"{'' if explicit else f' (default: {a.days_back}d back, +{catch_up}d catch-up)'}")
    log(f"gap file : {gap_path}")
    log(f"dataset  : {box_path}")

    gf = GapFile.load(gap_path, state_code=state, state_name=state_name, sport=sport,
                      season=a.season, level=level)
    ds = BoxScoreDataset(box_path)
    stats = {"state": state, "sport": sport, "new_games": 0, "updated_games": 0, "left_schedule": 0,
             "attempted": 0, "SUCCESS": 0, "NO_DATA": 0, "FAILED": 0, "records_added": 0,
             "records_updated": 0, "records_unchanged": 0, "schedule_errors": 0, "aborted": False}

    master = find_master(sport, a.season, a.master)
    from schedule_source import load_state_teams, fetch_team_schedule
    teams = load_state_teams(master, state, level)
    if a.limit_teams:
        teams = teams[:a.limit_teams]
    opp_index = bx._get_opp_index(sport, a.season)
    log(f"teams    : {len(teams)}   (master: {_shown_path(master)})")
    fetch_sched = schedule_fetcher or (lambda t: fetch_team_schedule(t, state, sport, a.season, level, opp_index))
    from game_checker import check_game
    check = checker or (lambda row: check_game(row, opp_index, a.delay))

    for t in teams:                                       # roster
        r = gf.teams.setdefault(t["teamUrl"], {"teamUrl": t["teamUrl"], "schedule_status": "",
                                               "schedule_last_attempt": "", "schedule_error": ""})
        r["teamName"], r["region"] = t["teamName"], t["region"]

    # ── 3. discovery ─────────────────────────────────────────────────────────
    if not a.skip_schedule:
        log(f"Schedules: fetching {len(teams)} ({a.sched_workers} workers)")
        by_team = {}
        for r in gf.games.values():
            by_team.setdefault(r["team_url"], []).append(r)
        ok_n = 0
        with ThreadPoolExecutor(max_workers=a.sched_workers) as pool:
            futs = {pool.submit(fetch_sched, t): t for t in teams}
            for i, fut in enumerate(as_completed(futs), 1):
                t = futs[fut]
                ros = gf.teams[t["teamUrl"]]
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"ok": False, "refs": [], "record": None, "error": f"{type(e).__name__}: {e}"}
                ros["schedule_last_attempt"] = now_str(now)
                if not res["ok"]:
                    ros["schedule_status"], ros["schedule_error"] = "ERROR", res["error"]
                    stats["schedule_errors"] += 1
                else:
                    ok_n += 1
                    ros.update(schedule_status="OK", schedule_error="", recordGamesPlayed=res.get("record"))
                    seen = set()
                    for ref in res["refs"]:
                        seen.add(ref["game_key"])
                        known = ref["game_key"] in gf.games
                        d = ref["game_date"]
                        in_scope = (not d) or a.inventory_all or (disc_from.isoformat() <= d <= window[1].isoformat())
                        if known or in_scope:
                            act = gf.upsert_discovered(ref, now)
                            if act == "new":
                                stats["new_games"] += 1
                            elif act == "updated":
                                stats["updated_games"] += 1
                    for r in by_team.get(t["teamUrl"], []):     # vanished from a schedule we just read OK
                        if (r["game_key"] not in seen and r["status"] != SUCCESS and r.get("on_schedule", True)):
                            r["on_schedule"] = False
                            r["log_message"] = "No longer on the team's schedule (cancelled or rescheduled?)"
                            stats["left_schedule"] += 1
                if i % 200 == 0 or i == len(teams):
                    log(f"  schedules {i}/{len(teams)}  (errors {stats['schedule_errors']})")
        if teams and ok_n == 0:
            log("[ABORT] every schedule fetch failed - VPN down or MaxPreps unreachable. Nothing changed.")
            stats["aborted"] = True
            return stats
        log(f"Discovery: +{stats['new_games']} new games, {stats['updated_games']} changed, "
            f"{stats['left_schedule']} left the schedule")
    else:
        log("Discovery skipped (--skip-schedule): retry-only run")

    # ── 4. selection ─────────────────────────────────────────────────────────
    apply_expiry(gf.games.values(), as_of, policy, now, window)
    due = select_due(list(gf.games.values()), now, as_of, window, policy)
    if a.limit_games:
        due = due[:a.limit_games]
    reasons = {}
    for _, why in due:
        reasons[why] = reasons.get(why, 0) + 1
    log(f"Due this run: {len(due)}  {reasons or ''}   (inventory: {gf.status_counts()})")

    if a.dry_run:
        for r, why in due[:25]:
            log(f"  [dry-run] {why:14s} {r['game_date']} {r['team_name']} vs {r['opponent_team_name']}  {r['game_url']}")
        log("Dry run: nothing written.")
        return stats

    # ── 5-7. attempt, record, upsert ─────────────────────────────────────────
    processed_teams = set()
    infra_streak, ckpt = 0, 0

    def checkpoint(final=False):
        mark_duplicates(gf.games.values())
        if ds.dirty or (final and len(ds)):          # no empty dataset file on discovery-only runs
            ds.save(processed_teams=processed_teams)
        gf.save(export_excel=final and not a.no_excel)     # dataset first: a crash between them only re-checks games, never duplicates

    if a.no_scrape or not due:
        log("No game attempts this run." if not due else "--no-scrape: discovery only.")
    else:
        log(f"Attempting {len(due)} games ({a.game_workers} workers)")
        pool = ThreadPoolExecutor(max_workers=a.game_workers)
        futs = {pool.submit(check, r): r for r, _ in due}
        try:
            for i, fut in enumerate(as_completed(futs), 1):
                row = futs[fut]
                try:
                    out = fut.result()
                except Exception as e:                       # checker contract is "never raises"; be safe
                    from gap_store import Outcome
                    out = Outcome(FAILED, f"Worker crashed: {type(e).__name__}: {e}")
                record_attempt(row, out, policy, now)
                stats["attempted"] += 1
                stats[out.status] = stats.get(out.status, 0) + 1
                if out.status == SUCCESS and out.record:
                    stats["records_" + ds.upsert(out.record)] += 1
                    processed_teams.add(row["team_id"])
                infra_streak = infra_streak + 1 if out.infra else 0
                if infra_streak >= a.max_infra_failures:
                    log(f"[ABORT] {infra_streak} consecutive geo-block/network failures - stopping "
                        f"(retry budgets were NOT charged). Check the VPN and re-run.")
                    stats["aborted"] = True
                    break
                if i % 50 == 0 or i == len(futs):
                    log(f"  {i}/{len(futs)}  ok={stats[SUCCESS]} no_data={stats[NO_DATA]} failed={stats[FAILED]}")
                ckpt += 1
                if ckpt >= a.checkpoint_every:
                    ckpt = 0
                    checkpoint()
        except KeyboardInterrupt:
            log("[INTERRUPTED] saving progress; completed attempts are kept, the rest stay due")
            stats["aborted"] = True
        finally:
            for f in futs:
                f.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
    checkpoint(final=True)

    # ── 7b. stats tab (priority source) + accumulated files ──────────────────
    if not a.no_stats_tab and not a.no_scrape and not stats["aborted"]:
        from stats_tab import StatsTabStore, pick_teams, run_stats_tab, stab_path_for
        store = StatsTabStore(stab_path_for(box_path))
        picked = pick_teams(gf, window, a.stats_tab_scope)
        log(f"Stats tab scope '{a.stats_tab_scope}': {len(picked)} teams")
        stats["stats_tab"] = run_stats_tab(gf, store, picked, a.season, workers=a.stats_tab_workers,
                                           ttl_hours=0 if explicit else a.stats_tab_ttl_hours,
                                           force=a.stats_tab_force, now=now, fetcher=stats_tab_fetcher)
        gf.save(export_excel=False)
    if not a.no_accumulate and not a.no_scrape and not stats["aborted"]:
        from accumulate_stats import accumulate_state
        log("Accumulating -> " + accumulate_state(out_dir, state, sport, a.season, not a.no_stats_tab))

    # ── 8. report ────────────────────────────────────────────────────────────
    stats["inventory"] = gf.status_counts()
    gf.run_log.append({"at": now_str(), "window": [str(window[0]), str(window[1])],
                       **{k: stats[k] for k in ("new_games", "attempted", "SUCCESS", "NO_DATA", "FAILED",
                                                "records_added", "records_updated")}})
    gf.save(export_excel=not a.no_excel)
    log(f"Done {state} {sport}: attempted {stats['attempted']} -> SUCCESS {stats[SUCCESS]}, "
        f"NO_DATA {stats[NO_DATA]}, FAILED {stats[FAILED]} | records +{stats['records_added']} "
        f"~{stats['records_updated']} ={stats['records_unchanged']}")
    log(f"Inventory now: {stats['inventory']}")
    return stats


# ─── CLI ─────────────────────────────────────────────────────────────────────
def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--state", required=True, help="State code(s), comma-separated: TX or TX,OK")
    p.add_argument("--sport", default="boys", choices=["boys", "girls", "both"])
    p.add_argument("--season", default="2026-2027")
    p.add_argument("--level", default="varsity", choices=["varsity", "jv", "freshman"])
    p.add_argument("--master", default=None, help="Master team list (default: Input Master File/{sport}_basketball_all_states_{YY-YY}.json)")
    p.add_argument("--output-root", default=os.path.join(HERE, "output"))
    p.add_argument("--output-dir", default=None, help="Exact folder for this run's files (overrides --output-root layout)")
    g = p.add_argument_group("which games")
    g.add_argument("--date", help="Process one game date, YYYY-MM-DD (default: yesterday)")
    g.add_argument("--date-from"), g.add_argument("--date-to")
    g.add_argument("--days-back", type=int, default=1, help="Default window = today minus N days (1 = yesterday)")
    g.add_argument("--catch-up-days", type=int, default=3,
                   help="On default runs also pick up games first seen up to N days before the window "
                        "(covers a skipped/failed earlier run). Ignored with an explicit date.")
    g.add_argument("--inventory-all", action="store_true",
                   help="Record EVERY scheduled game (incl. future) as PENDING, not just the window")
    r = p.add_argument_group("retries")
    r.add_argument("--max-attempts", type=int, default=6, help="Attempts per retry cycle before giving up")
    r.add_argument("--nodata-backoff-hours", type=float, default=12.0)
    r.add_argument("--failed-backoff-hours", type=float, default=1.0)
    r.add_argument("--retry-window-days", type=int, default=14, help="Stop retrying this long after the game date")
    r.add_argument("--force-exhausted", action="store_true", help="Retry games that already gave up")
    r.add_argument("--recheck-success", action="store_true", help="Re-scrape SUCCESS games inside the window")
    r.add_argument("--recheck-partial", action="store_true", help="Re-scrape SUCCESS games where only one team has stats")
    r.add_argument("--recheck-contest", action="append", metavar="CONTEST_ID", help="Re-scrape one game (repeatable)")
    m = p.add_argument_group("mode / performance")
    m.add_argument("--skip-schedule", action="store_true", help="Retry-only: don't re-fetch schedules")
    m.add_argument("--no-scrape", action="store_true", help="Discovery only: update the gap file, scrape nothing")
    m.add_argument("--dry-run", action="store_true", help="Show what would be attempted; write nothing")
    m.add_argument("--no-excel", action="store_true")
    s = p.add_argument_group("stats tab (priority source) + accumulated files")
    s.add_argument("--no-stats-tab", action="store_true", help="Skip the stats-tab fetch (box scores only)")
    s.add_argument("--stats-tab-scope", default="window", choices=["window", "played", "all"],
                   help="Teams to fetch: window = teams with a game in the date window (default); "
                        "played = every team with a game up to the window end; all = every team")
    s.add_argument("--stats-tab-workers", type=int, default=8)
    s.add_argument("--stats-tab-ttl-hours", type=float, default=6.0,
                   help="Skip a team whose stats tab was fetched this recently (default runs only; "
                        "an explicit --date always refreshes)")
    s.add_argument("--stats-tab-force", action="store_true", help="Ignore the TTL and refetch")
    s.add_argument("--no-accumulate", action="store_true", help="Don't rebuild the accumulated/Final files")
    m.add_argument("--sched-workers", type=int, default=20)
    m.add_argument("--game-workers", type=int, default=20)
    m.add_argument("--delay", type=float, default=0.3, help="Per-request delay (s)")
    m.add_argument("--checkpoint-every", type=int, default=200, help="Save after this many attempts")
    m.add_argument("--max-infra-failures", type=int, default=25,
                   help="Abort after this many consecutive geo-block/network failures")
    m.add_argument("--limit-teams", type=int, default=None)
    m.add_argument("--limit-games", type=int, default=None)
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    sports = ["boys", "girls"] if a.sport == "both" else [a.sport]
    rc = 0
    for state in [s.strip() for s in a.state.split(",") if s.strip()]:
        for sport in sports:
            st = run_one(a, state, sport)
            rc = max(rc, 2 if st.get("aborted") else 0)
    return rc


if __name__ == "__main__":
    sys.exit(main())
