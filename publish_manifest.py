"""publish_manifest.py - the small, always-committed files that describe a day's data.

Writes into <data-root>/_reports/ :
  manifest.json             one entry per Final_*_accumulated_*_26_27.json: sha256, bytes, team records,
                            player records, teams with GP>0, total GP, generated_at.
                            The Pinecone ingestion can compare this with its last manifest and ingest only
                            the files whose sha256 changed.
  run_report_<date>.json    per state/sport game counts by status, per game date (last 21 days), and the
                            contest ids of every FAILED game  -> "where did the pipeline go wrong?"
It also checks the new manifest against the previous one (a Final file that disappeared, or whose
team / player / total-GP numbers went DOWN, is a regression: the numbers only ever grow in-season).

    python publish_manifest.py                       # report-only: prints warnings, exit 0
    python publish_manifest.py --strict              # exit 3 if a regression was found
    python publish_manifest.py --summary-md FILE     # also append a markdown table (GitHub job summary)
"""
import argparse
import collections
import datetime as dt
import glob
import hashlib
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RECENT_DAYS = 21


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def final_entry(path, rel):
    recs = json.load(open(path, encoding="utf-8"))
    teams = [r for r in recs if r.get("record_type") == "team_total"]
    players = [r for r in recs if r.get("record_type") == "player"]
    return {
        "file": rel,
        "sha256": sha256(path),
        "bytes": os.path.getsize(path),
        "team_records": len(teams),
        "player_records": len(players),
        "teams_with_games": sum(1 for r in teams if (r.get("GP") or 0) > 0),
        "total_gp": sum((r.get("GP") or 0) for r in teams),
    }


def gap_summary(path, today):
    g = json.load(open(path, encoding="utf-8")).get("games", [])
    rows = g.items() if isinstance(g, dict) else [(r.get("contest_id"), r) for r in g]
    # a game is listed once per team (two rows, same contest_id): count each game once, keeping the row
    # with the "best" status so a game is only SUCCESS / FAILED / ... once
    rank = {"SUCCESS": 0, "FAILED": 1, "RETRY": 2, "NO_DATA": 3, "PENDING": 4}
    best = {}
    for cid, r in rows:
        cur = best.get(cid)
        if cur is None or rank.get(r.get("status"), 9) < rank.get(cur.get("status"), 9):
            best[cid] = r
    status = collections.Counter()
    recent = collections.defaultdict(collections.Counter)
    failed = []
    cutoff = (today - dt.timedelta(days=RECENT_DAYS)).isoformat()
    for cid, r in best.items():
        s = r.get("status") or "?"
        status[s] += 1
        d = r.get("game_date") or ""
        if d >= cutoff and d <= today.isoformat():
            recent[d][s] += 1
        if s == "FAILED":
            failed.append(cid)
    return {"games": len(best), "status": dict(status), "recent_by_date": {d: dict(c) for d, c in sorted(recent.items())},
            "failed_contest_ids": sorted(failed)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", default=os.path.join(HERE, "Scraped data 2026-27"))
    ap.add_argument("--date", help="report date YYYY-MM-DD (default: today, UTC)")
    ap.add_argument("--strict", action="store_true", help="exit 3 when a regression is found")
    ap.add_argument("--summary-md", help="append a markdown summary to this file (GITHUB_STEP_SUMMARY)")
    a = ap.parse_args()

    today = dt.date.fromisoformat(a.date) if a.date else dt.datetime.now(dt.timezone.utc).date()
    out_dir = os.path.join(a.data_root, "_reports")
    os.makedirs(out_dir, exist_ok=True)
    manifest_path = os.path.join(out_dir, "manifest.json")
    old = {}
    if os.path.exists(manifest_path):
        try:
            old = {e["file"]: e for e in json.load(open(manifest_path, encoding="utf-8")).get("files", [])}
        except (OSError, ValueError):
            old = {}

    files, report = [], {}
    for fp in sorted(glob.glob(os.path.join(a.data_root, "*", "Final_*_accumulated_*_26_27.json"))):
        rel = os.path.relpath(fp, a.data_root).replace("\\", "/")
        files.append(final_entry(fp, rel))
        parts = os.path.basename(fp).split("_")                # Final_<st>_accumulated_<sport>_26_27.json
        st, sport = parts[1], parts[3]
        gaps = os.path.join(os.path.dirname(fp), f"{st}_data_gaps_{sport}_2026_2027.json")
        if os.path.exists(gaps):
            report[f"{st.upper()}_{sport}"] = gap_summary(gaps, today)

    problems = []
    new = {e["file"]: e for e in files}
    for rel, e0 in old.items():
        e1 = new.get(rel)
        if e1 is None:
            problems.append(f"{rel}: Final file is gone")
            continue
        for k in ("team_records", "player_records", "teams_with_games", "total_gp"):
            if e1[k] < e0[k]:
                problems.append(f"{rel}: {k} went down {e0[k]} -> {e1[k]}")

    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"generated_at": now, "files": files}, f, indent=1)
    rep_path = os.path.join(out_dir, f"run_report_{today.isoformat()}.json")
    with open(rep_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"date": today.isoformat(), "generated_at": now, "regressions": problems,
                   "final_files": len(files), "states_sports": report}, f, indent=1)

    tot = collections.Counter()
    for e in files:
        tot["teams"] += e["team_records"]
        tot["players"] += e["player_records"]
        tot["teams_with_games"] += e["teams_with_games"]
    failed = sum(len(v["failed_contest_ids"]) for v in report.values())
    print(f"manifest: {len(files)} Final files, {tot['teams']} teams ({tot['teams_with_games']} with games), "
          f"{tot['players']} players; {failed} FAILED games listed in {os.path.basename(rep_path)}")
    for p in problems:
        print("REGRESSION:", p)
    if a.summary_md:
        with open(a.summary_md, "a", encoding="utf-8") as f:
            f.write(f"### Data summary {today.isoformat()}\n\n")
            f.write(f"- Final files: **{len(files)}**, teams: **{tot['teams']}** "
                    f"(**{tot['teams_with_games']}** with games), players: **{tot['players']}**\n")
            f.write(f"- FAILED games waiting for a retry: **{failed}**\n")
            f.write(f"- Regressions vs the previous manifest: **{len(problems)}**\n")
            for p in problems:
                f.write(f"  - {p}\n")
            f.write("\n")
    return 3 if (problems and a.strict) else 0


if __name__ == "__main__":
    sys.exit(main())
