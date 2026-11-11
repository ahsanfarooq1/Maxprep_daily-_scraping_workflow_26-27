"""Stage 4 for the 2026-27 workflow: box scores (+ stats tab) -> accumulated season stats.

Mirrors the old APP/pipeline.py stage 4:
  4a per-game accumulator over the box-score file   (Accumulation_data.process_stats)
  4b stats-tab merge: STATS TAB WINS for any team that has it (merge_all_stats_tab.merge, incl.
     its GP=0 guard); teams that have a stats tab but no box score are added too
  4c TotalGamesChecked repair                        (fix_total_games_checked.py)
Legacy modules are used read-only. In "Scraped data 2026-27/<State>/" it reads
    {st}_box_scores_{sport}_2026_2027.json  and  {st}_all_stats_tab_{sport}_2026_2027.json
and writes
    {st}_accumulated_stats_{sport}_2026_2027.json   (per-game accumulator output)
    Final_{st}_accumulated_{sport}_26_27.json        (the file to share)

    python accumulate_stats.py                       # all states, both sports
    python accumulate_stats.py --state TX,OK --sport boys
    python accumulate_stats.py --no-stats-tab        # box scores only
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor

import wf_core
from stats_tab import final_path_for

os.environ.setdefault("PYTHONIOENCODING", "utf-8")   # legacy modules print arrows; Windows cp1252 would crash
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "Scraped data 2026-27")
CORE = wf_core.CORE_DIR


def add_total_games_scheduled(final, gaps):
    """Add `total_games_scheduled` to every team_total record: the number of games the gap finder
    found on that team's schedule (rows still on the schedule, stale duplicate contests excluded).
    Teams with no schedule rows (e.g. schedule fetch failed) get 0."""
    n = {}
    if os.path.exists(gaps):
        with open(gaps, encoding="utf-8") as f:
            for r in json.load(f).get("games", []):
                if r.get("on_schedule", True) and not r.get("duplicate_of"):
                    n[r["team_id"]] = n.get(r["team_id"], 0) + 1
    with open(final, encoding="utf-8") as f:
        recs = json.load(f)
    for i, r in enumerate(recs):
        if r.get("record_type") == "team_total":
            val = n.get(r.get("team_id"), 0)
            out = {}
            for k, v in r.items():                      # key order: GP, TotalGamesChecked, total_games_scheduled, ...
                if k != "total_games_scheduled":
                    out[k] = v
                if k == "TotalGamesChecked":
                    out["total_games_scheduled"] = val
            out.setdefault("total_games_scheduled", val)
            recs[i] = out
    tmp = final + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(recs, f, indent=4, ensure_ascii=False)
    os.replace(tmp, final)
    return sum(1 for r in recs if r.get("record_type") == "team_total" and r["total_games_scheduled"])


def _align_names(acc, stab):
    """The merge joins on (team_id, team_name). The accumulator writes the master-list name, the
    stats tab a URL-derived one, so a team can differ by name only (e.g. an apostrophe) and would be
    kept twice. Give each stats-tab record the accumulator's name for the same team_id.
    Returns the path to merge with (a temp copy if anything changed, else `stab`)."""
    with open(acc, encoding="utf-8") as f:
        names = {r["team_id"]: r["team_name"] for r in json.load(f) if r.get("record_type") == "team_total"}
    with open(stab, encoding="utf-8") as f:
        recs = json.load(f)
    changed = 0
    for r in recs:
        want = names.get(r.get("team_id"))
        if want and want != r.get("team_name"):
            r["team_name"] = want
            changed += 1
    if not changed:
        return stab
    out = stab + ".aligned.tmp"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(recs, f, ensure_ascii=False)
    print(f"  [stats tab] aligned {changed} record names to the accumulator's team names")
    return out


def accumulate_state(folder, st, sport, season="2026-2027", use_stats_tab=True):
    """Returns a one-line result message."""
    st = st.lower()
    fn = wf_core.season_filename(season)
    box = os.path.join(folder, f"{st}_box_scores_{sport}_{fn}.json")
    stab = os.path.join(folder, f"{st}_all_stats_tab_{sport}_{fn}.json")
    acc = os.path.join(folder, f"{st}_accumulated_stats_{sport}_{fn}.json")
    final = final_path_for(folder, st, sport, season)
    have_box = os.path.exists(box)
    have_stab = use_stats_tab and os.path.exists(stab) and os.path.getsize(stab) > 2
    if not have_box and not have_stab:
        return "skip (no box-score or stats-tab data yet)"
    if CORE not in sys.path:
        sys.path.insert(0, CORE)
    if have_box:
        from Accumulation_data import process_stats
        process_stats(input_file=box, output_file=acc)
    else:
        with open(acc, "w", encoding="utf-8") as f:
            json.dump([], f)
    if have_stab:
        from merge_all_stats_tab import merge
        stab_in = _align_names(acc, stab)
        try:
            res = merge(acc, stab_in, final)
        finally:
            if stab_in != stab and os.path.exists(stab_in):
                os.remove(stab_in)
        if not res:
            return "stats-tab merge FAILED"
        src = f"stats tab wins for {res['replaced']} matched + {res['unmatched']} stats-tab-only teams"
    else:
        shutil.copyfile(acc, final)
        src = "box scores only"
    if have_box:
        env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
        r = subprocess.run([sys.executable, "-X", "utf8", os.path.join(CORE, "fix_total_games_checked.py"),
                            "--input", final, "--box-scores", box, "--output", final],
                           capture_output=True, text=True, env=env, encoding="utf-8", errors="replace")
        if r.returncode != 0:
            return "TGC fix FAILED: " + (r.stderr or r.stdout)[-300:]
    gaps = os.path.join(folder, f"{st}_data_gaps_{sport}_{fn}.json")
    with_sched = add_total_games_scheduled(final, gaps)
    return f"ok -> {os.path.basename(final)}  ({src}; total_games_scheduled on {with_sched} teams)"


def _job(j):
    folder, st, sport, season, use = j
    return st, sport, accumulate_state(folder, st, sport, season, use)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", help="comma list of state codes (default: all folders)")
    ap.add_argument("--sport", choices=["boys", "girls", "both"], default="both")
    ap.add_argument("--season", default="2026-2027")
    ap.add_argument("--no-stats-tab", action="store_true")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    want = {s.strip().lower() for s in a.state.split(",")} if a.state else None
    sports = ["boys", "girls"] if a.sport == "both" else [a.sport]
    jobs = []
    for name in sorted(os.listdir(ROOT)):
        folder = os.path.join(ROOT, name)
        if not os.path.isdir(folder):
            continue
        for st in sorted({f.split("_")[0] for f in os.listdir(folder) if "_data_gaps_" in f}):
            if not want or st in want:
                jobs += [(folder, st, sp, a.season, not a.no_stats_tab) for sp in sports]
    with ProcessPoolExecutor(a.workers) as ex:
        for st, sp, msg in ex.map(_job, jobs):
            print(f"{st.upper()} {sp:5} {msg}", flush=True)


if __name__ == "__main__":
    main()
