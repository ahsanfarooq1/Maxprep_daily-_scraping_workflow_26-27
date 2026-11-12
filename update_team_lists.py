"""Update Team_list_26-27/{boys,girls}_basketball_teams_list_26-27.json from the Final files.

Each list entry {team_name, city, state, data_availablity} is matched (via the 2026-27 master file,
which gives the MaxPreps team URL) to its team_total record in
    <data-root>/<State>/Final_<st>_accumulated_<sport>_26_27.json
and data_availablity is set by comparing GP with total_games_scheduled:
    GP == 0                          -> "none"     (also: team not in any Final file)
    GP > 0                           -> "partial"
("full" is intentionally not produced while the season is in progress; a team's schedule on
 MaxPreps only lists the games published so far, so GP == total_games_scheduled does not mean complete.)

    python update_team_lists.py                       # uses ./Scraped data 2026-27
    python update_team_lists.py --data-root <path>
"""
import argparse
import collections
import glob
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
MASTER_DIR = os.path.join(HERE, "core")


def norm(s):
    return " ".join((s or "").lower().split())


def team_key(url_or_id):
    """'https://www.maxpreps.com/tx/alvin/alvin-yellowjackets/basketball/' or
    'tx/alvin/alvin-yellowjackets/basketball/girls' -> 'tx/alvin/alvin-yellowjackets'"""
    p = url_or_id.split("maxpreps.com/")[-1].strip("/").split("/")
    return "/".join(p[:3]).lower()


def load_master(sport):
    m = json.load(open(os.path.join(MASTER_DIR, f"{sport}_basketball_all_states_26-27.json"), encoding="utf-8"))
    by_name = collections.defaultdict(set)          # (name, city, state) -> {team_key}
    by_state = collections.defaultdict(list)        # st -> [(city_slug, team_slug, team_key)]
    for st in m["byState"].values():
        for reg in st["regions"].values():
            for t in reg["teams"]:
                k = team_key(t["teamUrl"])
                by_name[(norm(t["teamName"]), norm(t["city"]), norm(t["state"]))].add(k)
                st_, city_, slug_ = k.split("/")
                by_state[st_].append((city_, slug_, k))
    return by_name, by_state


def load_finals(data_root, sport, by_state):
    """team_key -> (GP, total_games_scheduled). Some opponents are accumulated under a short slug with
    no state/city (e.g. 'western-yell-county-havana' = team 'western-yell-county-wolverines' in havana);
    their games are credited to the real master team when exactly one team fits."""
    out, credited = {}, []
    for fp in glob.glob(os.path.join(data_root, "*", f"Final_*_accumulated_{sport}_26_27.json")):
        st = os.path.basename(fp).split("_")[1].lower()
        for r in json.load(open(fp, encoding="utf-8")):
            if r.get("record_type") != "team_total":
                continue
            gp, sch = r.get("GP") or 0, r.get("total_games_scheduled") or 0
            tid = r["team_id"]
            if "/" in tid:
                out[team_key(tid)] = (gp, sch)
                continue
            if not gp:
                continue
            fits = [k for city, slug, k in by_state.get(st, [])
                    if tid.endswith("-" + city) and slug.startswith(tid[: -len(city) - 1] + "-")]
            if len(fits) == 1:
                old = out.get(fits[0], (0, 0))
                out[fits[0]] = (max(old[0], gp), old[1])
                credited.append((tid, fits[0]))
    return out, credited


def availability(gp, scheduled):
    return "partial" if gp > 0 else "none"          # no "full" during the season


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", default=os.path.join(HERE, "Scraped data 2026-27"))
    ap.add_argument("--list-dir", help="default: <data-root>/Team_list_26-27")
    a = ap.parse_args()
    list_dir = a.list_dir or os.path.join(a.data_root, "Team_list_26-27")
    for sport in ("boys", "girls"):
        path = os.path.join(list_dir, f"{sport}_basketball_teams_list_26-27.json")
        teams = json.load(open(path, encoding="utf-8"))
        master, by_state = load_master(sport)
        finals, credited = load_finals(a.data_root, sport, by_state)
        cnt, unmatched, ambiguous = collections.Counter(), 0, 0
        for t in teams:
            keys = master.get((norm(t["team_name"]), norm(t["city"]), norm(t["state"])), set())
            hits = [finals[k] for k in keys if k in finals]
            if not keys:
                unmatched += 1
            if len(keys) > 1:
                ambiguous += 1
            gp, sched = max(hits) if hits else (0, 0)
            t["data_availablity"] = availability(gp, sched)
            cnt[t["data_availablity"]] += 1
        for tid, k in credited:
            print(f"  short-slug record '{tid}' credited to {k}")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            json.dump(teams, f, indent=4, ensure_ascii=False)
        os.replace(tmp, path)
        print(f"{sport}: {len(teams)} teams -> {dict(cnt)}  "
              f"(not in master: {unmatched}, ambiguous name: {ambiguous}, Final team records: {len(finals)})")


if __name__ == "__main__":
    main()
