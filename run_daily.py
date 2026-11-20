"""run_daily.py - the one command a scheduler (GitHub Actions / cron / Task Scheduler) runs.

For every state (parallel, N at a time) it runs daily_gapfinder.py for boys + girls:
  box scores for yesterday's games (+ retry backlog) -> stats tab for the teams that played
  -> accumulated + Final files, all written to --data-root/<State>/.

  python run_daily.py --data-root "Scraped data 2026-27"                 # daily (no schedule refetch)
  python run_daily.py --data-root "Scraped data 2026-27" --refresh-schedule   # weekly: also find new games
  python run_daily.py --states TX,OK --date 2026-11-10                    # a specific day / states
  python run_daily.py --skip-if-done-today                                # 2nd trigger of the day: no-op if done

Exit code: 0 = every state ran; 1 = at least one state aborted/crashed (the others still finished and
their files are valid - re-running is safe, finished work is never repeated or duplicated).
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
MARKER = "_last_daily_run.json"

STATES = {
    "TX": "Texas", "NM": "New Mexico", "OK": "Oklahoma", "AR": "Arkansas", "LA": "Louisiana",
    "IN": "Indiana", "OH": "Ohio", "WA": "Washington", "MI": "Michigan", "CO": "Colorado",
    "WI": "Wisconsin", "AZ": "Arizona", "KY": "Kentucky", "NV": "Nevada", "UT": "Utah",
    "TN": "Tennessee", "MS": "Mississippi", "VA": "Virginia", "WV": "West Virginia", "KS": "Kansas",
    "MO": "Missouri", "AL": "Alabama", "ID": "Idaho", "OR": "Oregon", "CA": "California",
    "WY": "Wyoming", "IL": "Illinois", "GA": "Georgia", "FL": "Florida", "NC": "North Carolina",
}


# --boost: more parallel requests for the states that take longest (useful on a slow VPN, where each
# request spends most of its time waiting). (game workers, stats-tab workers). Default is 5 / 8.
BOOST = {"TX": (24, 16), "CA": (14, 12), "FL": (14, 12), "GA": (14, 12), "AL": (14, 12),
         "NC": (12, 12), "AR": (12, 12), "IN": (10, 10), "OK": (10, 10), "IL": (10, 10)}


def say(logs, msg):
    """print + append to the day's _run_summary.log"""
    print(msg, flush=True)
    try:
        with open(os.path.join(logs, "_run_summary.log"), "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except OSError:
        pass


def utc_today():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def run_state(code, a, logs):
    out_dir = os.path.join(a.data_root, STATES[code])
    os.makedirs(out_dir, exist_ok=True)
    cmd = [sys.executable, "-u", os.path.join(HERE, "daily_gapfinder.py"),
           "--state", code, "--sport", "both", "--season", a.season,
           "--game-workers", str(a.game_workers), "--output-dir", out_dir, "--no-excel"]
    if a.boost and code in BOOST:
        gw, sw = BOOST[code]
        cmd[cmd.index("--game-workers") + 1] = str(max(gw, a.game_workers))
        cmd += ["--stats-tab-workers", str(sw)]
    if a.date:
        cmd += ["--date", a.date]
    if a.date_from:
        cmd += ["--date-from", a.date_from]
    if a.date_to:
        cmd += ["--date-to", a.date_to]
    if a.refresh_schedule:
        cmd += ["--inventory-all"]        # keep every scheduled game so total_games_scheduled stays complete
    else:
        cmd += ["--skip-schedule"]
    if a.no_stats_tab:
        cmd += ["--no-stats-tab"]
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    t0 = time.time()
    # one folder per day (_logs/YYYY-MM-DD/); a second run the same day appends to the same file
    with open(os.path.join(logs, f"{code}.log"), "a", encoding="utf-8") as lf:
        lf.write(f"\n===== run started {time.strftime('%Y-%m-%d %H:%M:%S')}: {' '.join(cmd[2:])} =====\n")
        lf.flush()
        try:
            rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, cwd=HERE, env=env,
                                timeout=a.state_timeout_min * 60).returncode
        except subprocess.TimeoutExpired:
            rc = 124
            lf.write(f"\n[run_daily] killed after {a.state_timeout_min} min\n")
    say(logs, f"{time.strftime('%H:%M:%S')} {code:2} rc={rc} ({int(time.time() - t0)}s)")
    return code, rc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", default=os.path.join(HERE, "Scraped data 2026-27"))
    ap.add_argument("--states", help="comma list of codes (default: all 30)")
    ap.add_argument("--season", default="2026-2027")
    ap.add_argument("--date"), ap.add_argument("--date-from"), ap.add_argument("--date-to")
    ap.add_argument("--refresh-schedule", action="store_true",
                    help="also re-read every team's schedule (finds newly added/moved games); weekly is enough")
    ap.add_argument("--no-stats-tab", action="store_true")
    ap.add_argument("--parallel", type=int, default=10, help="states processed at the same time")
    ap.add_argument("--game-workers", type=int, default=5)
    ap.add_argument("--state-timeout-min", type=int, default=120, help="kill one state after this long")
    ap.add_argument("--boost", action="store_true",
                    help="more workers for the slowest states (TX, CA, FL, ...) and start them first; for slow links")
    ap.add_argument("--skip-if-done-today", action="store_true",
                    help="exit 0 immediately if a full run already completed today (UTC)")
    a = ap.parse_args()

    marker = os.path.join(a.data_root, MARKER)
    if a.skip_if_done_today and os.path.exists(marker):
        try:
            m = json.load(open(marker, encoding="utf-8"))
            if m.get("date") == utc_today() and m.get("ok"):
                print(f"Already completed today ({m.get('finished')}); nothing to do.")
                return 0
        except Exception:
            pass

    codes = [c.strip().upper() for c in a.states.split(",")] if a.states else list(STATES)
    bad = [c for c in codes if c not in STATES]
    if bad:
        sys.exit(f"Unknown state code(s): {bad}")
    if a.boost:
        codes.sort(key=lambda c: -(BOOST[c][0] if c in BOOST else 0))   # slowest states start first
    logs = os.path.join(a.data_root, "_logs", time.strftime("%Y-%m-%d"))     # local date of the run
    os.makedirs(logs, exist_ok=True)
    say(logs, f"{time.strftime('%Y-%m-%d %H:%M:%S')} run_daily {' '.join(sys.argv[1:])}")
    say(logs, f"run_daily: {len(codes)} states, {a.parallel} at a time, "
              f"{'WITH schedule refresh' if a.refresh_schedule else 'schedule not refetched'}")
    with ThreadPoolExecutor(a.parallel) as ex:
        rcs = dict(ex.map(lambda c: run_state(c, a, logs), codes))
    failed = {c: r for c, r in rcs.items() if r}
    ok = not failed
    if not a.states or len(codes) == len(STATES):
        with open(marker, "w", encoding="utf-8") as f:
            json.dump({"date": utc_today(), "ok": ok, "failed": failed,
                       "finished": time.strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
    say(logs, f"DONE. problems: {failed or 'none'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
