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


def utc_today():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def run_state(code, a, logs):
    out_dir = os.path.join(a.data_root, STATES[code])
    os.makedirs(out_dir, exist_ok=True)
    cmd = [sys.executable, "-u", os.path.join(HERE, "daily_gapfinder.py"),
           "--state", code, "--sport", "both", "--season", a.season,
           "--game-workers", str(a.game_workers), "--output-dir", out_dir, "--no-excel"]
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
    with open(os.path.join(logs, f"{code}.log"), "w", encoding="utf-8") as lf:
        try:
            rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, cwd=HERE, env=env,
                                timeout=a.state_timeout_min * 60).returncode
        except subprocess.TimeoutExpired:
            rc = 124
            lf.write(f"\n[run_daily] killed after {a.state_timeout_min} min\n")
    print(f"{time.strftime('%H:%M:%S')} {code:2} rc={rc} ({int(time.time() - t0)}s)", flush=True)
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
    logs = os.path.join(a.data_root, "_logs")
    os.makedirs(logs, exist_ok=True)
    print(f"run_daily: {len(codes)} states, {a.parallel} at a time, "
          f"{'WITH schedule refresh' if a.refresh_schedule else 'schedule not refetched'}", flush=True)
    with ThreadPoolExecutor(a.parallel) as ex:
        rcs = dict(ex.map(lambda c: run_state(c, a, logs), codes))
    failed = {c: r for c, r in rcs.items() if r}
    ok = not failed
    if not a.states or len(codes) == len(STATES):
        with open(marker, "w", encoding="utf-8") as f:
            json.dump({"date": utc_today(), "ok": ok, "failed": failed,
                       "finished": time.strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
    print("DONE. problems:", failed or "none", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
