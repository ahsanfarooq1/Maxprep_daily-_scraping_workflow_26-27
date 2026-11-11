"""
Scrape team-level + per-player season stats from MaxPreps' print-stats
endpoint for a list of teams, and emit the result in the EXACT same flat
record format as Accumulation_data.py (one team_total + N player rows per
team, same field names, same types).

Use this for teams whose per-game pipeline missed data — e.g. the teams
flagged by find_stats_only_teams.py, or any custom team list.

Input formats accepted (auto-detected):
  1. find_stats_only_teams.py output  -- reads the 'flaggedTeams' list.
  2. A plain JSON list of team URLs   -- ["https://...basketball/girls/", ...]
  3. A JSON list of dicts             -- [{"teamUrl":"...","teamName":"...","gap_gamesChecked":N}, ...]
  4. A data-gaps JSON file            -- merges teamsFullBoxScores +
                                          teamsPartialBoxScores + teamsNoBoxScores.

Usage:
  python accumulate_from_stats_tab.py --input stats_only_check/tx_stats_only_teams_girls_2025_2026.json
  python accumulate_from_stats_tab.py --input my_teams.json --season 2024-2025 --workers 20
  python accumulate_from_stats_tab.py --input Texas_scraped_data/tx_data_gaps_girls_2025_2026.json
"""

import os
import sys
import json
import time
import argparse
import threading
import requests
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from scrape_team_stats import (
    _discover_ids,
    _fetch_print_stats_html,
    _parse_print_stats,
    _short_season,
    _team_url_to_id,
    _name_from_url,
    TEAM_WORKERS,
    DELAY,
)


_original_print = print
def print(*args, **kwargs):  # noqa: A001
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)


# ─── Format mapper: print-stats parse → accumulated_file record ─────────────

def _calc_per_32(rec):
    """Same Per_32 calc the accumulator uses (Pts/Reb/Ast/Stl/Blk per 32 min)."""
    m = rec.get('Min') or 0
    if m <= 0:
        return None
    return {
        'Pts': round((rec.get('Pts', 0) / m) * 32, 1),
        'Reb': round((rec.get('Reb', 0) / m) * 32, 1),
        'Ast': round((rec.get('Ast', 0) / m) * 32, 1),
        'Stl': round((rec.get('Stl', 0) / m) * 32, 1),
        'Blk': round((rec.get('Blk', 0) / m) * 32, 1),
    }


def stats_to_accumulated_record(team_id, team_name, name, stats, record_type,
                                total_games_checked=None):
    """Convert one row of print-stats data into the accumulated-file shape.

    Field names, types, and order match Accumulation_data.py.format_record so
    downstream consumers (scouting-report generators etc.) see no difference.
    """

    def _f(field, default=0.0):
        v = stats.get(field)
        return float(v) if v is not None else default

    def _i(field, default=0):
        v = stats.get(field)
        try:
            return int(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    rec = {
        'team_id':     team_id,
        'team_name':   team_name,
        'record_type': record_type,
        'Name':        name,
        'GP':          _i('GP'),
    }
    # TotalGamesChecked: only on team_total rows, only when we know the
    # gap-finder count (the standalone accumulator does the same — see
    # Accumulation_data.py).
    if record_type == 'team_total' and total_games_checked is not None:
        rec['TotalGamesChecked'] = int(total_games_checked)

    # Per-game averages (floats, 1-decimal source)
    for f in ('MPG', 'PPG', 'DEFR', 'OFFR', 'RPG', 'APG',
              'SPG', 'BPG', 'TPG', 'PFPG'):
        rec[f] = _f(f)

    # Shooting counters + derived
    rec['Min']  = _i('Min')
    rec['Pts']  = _i('Pts')
    rec['FGM']  = _i('FGM')
    rec['FGA']  = _i('FGA')
    rec['FG%']  = _f('FG%')
    rec['PPS']  = _f('PPS')
    rec['AFG%'] = _f('AFG%')

    # Detailed shooting
    rec['3PM']  = _i('3PM')
    rec['3PA']  = _i('3PA')
    rec['3P%']  = _f('3P%')
    rec['FTM']  = _i('FTM')
    rec['FTA']  = _i('FTA')
    rec['FT%']  = _f('FT%')
    rec['2FGM'] = _i('2FGM')
    rec['2FGA'] = _i('2FGA')
    rec['2FG%'] = _f('2FG%')

    # Totals counters
    for f in ('OReb', 'DReb', 'Reb', 'Ast', 'Stl', 'Blk', 'TO', 'PF'):
        rec[f] = _i(f)

    # Ratios (floats)
    for f in ('Ast:TO', 'Stl:TO', 'Stl:PF', 'Blk:PF'):
        rec[f] = _f(f)

    # Misc
    rec['Chr']  = _i('Chr')
    rec['Defl'] = _i('Defl')
    rec['TF']   = _i('TF')
    rec['DD']   = _i('DD')
    rec['TD']   = _i('TD')

    rec['Per_32'] = _calc_per_32(rec)
    return rec


# ─── Per-team worker ─────────────────────────────────────────────────────────

def _process_team(team_url, total_games_checked, season_suffix):
    """Fetch + parse one team's print-stats page → list of accumulated records.

    Returns (team_name, records_list, status) where status is one of:
      'has_data' / 'empty' / 'ids_missing' / 'fetch_failed' / 'unparseable'
    """
    team_id = _team_url_to_id(team_url)
    team_name = _name_from_url(team_url)

    schoolid, ssid = _discover_ids(team_url, season_suffix)
    if not schoolid or not ssid:
        return team_name, [], 'ids_missing'
    html = _fetch_print_stats_html(schoolid, ssid)
    if html is None:
        return team_name, [], 'fetch_failed'

    per_player, season_total, status = _parse_print_stats(html)
    if status != 'has_data' or not (per_player or season_total):
        return team_name, [], status

    records = []
    if season_total:
        records.append(stats_to_accumulated_record(
            team_id, team_name, 'Season Totals', season_total, 'team_total',
            total_games_checked=total_games_checked,
        ))
    for pname, pstats in per_player.items():
        records.append(stats_to_accumulated_record(
            team_id, team_name, pname, pstats, 'player',
        ))
    return team_name, records, 'has_data'


# ─── Input parsing (auto-detect format) ──────────────────────────────────────

def _load_team_list(input_file):
    """Returns a list of {teamUrl, teamName?, total_games_checked?} dicts."""
    with open(input_file, encoding='utf-8') as f:
        data = json.load(f)

    teams = []

    # Format 1: find_stats_only_teams.py output
    if isinstance(data, dict) and 'flaggedTeams' in data:
        for t in data['flaggedTeams']:
            # Field name varies across find_stats_only_teams.py versions:
            #   old: 'gap_gamesChecked' (when input was a gaps file)
            #   new: 'acc_TotalGamesChecked' (when input is the accumulated file)
            tgc = (t.get('acc_TotalGamesChecked')
                   or t.get('gap_gamesChecked')
                   or t.get('TotalGamesChecked'))
            teams.append({
                'teamUrl': t.get('teamUrl', ''),
                'teamName': t.get('teamName', ''),
                'total_games_checked': tgc,
            })
        return teams, 'stats_only_teams'

    # Format 4: data-gaps file (full/partial/no_box union)
    if isinstance(data, dict) and any(k in data for k in
            ('teamsFullBoxScores', 'teamsPartialBoxScores', 'teamsNoBoxScores')):
        seen = set()
        for bucket in ('teamsFullBoxScores', 'teamsPartialBoxScores', 'teamsNoBoxScores'):
            for t in data.get(bucket, []):
                url = t.get('teamUrl', '')
                if url and url not in seen:
                    seen.add(url)
                    teams.append({
                        'teamUrl': url,
                        'teamName': t.get('teamName', ''),
                        'total_games_checked': t.get('gamesChecked'),
                    })
        return teams, 'data_gaps'

    # Format 2/3: list (URLs or dicts)
    if isinstance(data, list):
        for t in data:
            if isinstance(t, str):
                teams.append({'teamUrl': t})
            elif isinstance(t, dict):
                teams.append({
                    'teamUrl': t.get('teamUrl') or t.get('team_url', ''),
                    'teamName': t.get('teamName') or t.get('team_name', ''),
                    'total_games_checked': (t.get('total_games_checked')
                                            or t.get('gap_gamesChecked')
                                            or t.get('gamesChecked')),
                })
        return teams, 'list'

    raise ValueError(
        "Unrecognised input. Expected:\n"
        "  - find_stats_only_teams output (dict with 'flaggedTeams')\n"
        "  - gaps file (dict with teamsFullBoxScores/teamsPartialBoxScores/teamsNoBoxScores)\n"
        "  - a list of URLs or {teamUrl, ...} dicts"
    )


# ─── Driver ──────────────────────────────────────────────────────────────────

def run(input_file, season, workers, output_file, level='varsity'):
    if not os.path.exists(input_file):
        print(f"[ERROR] Input file not found: {input_file}")
        return None

    teams, src_format = _load_team_list(input_file)
    # Drop entries without a URL (safety)
    teams = [t for t in teams if t.get('teamUrl')]

    # Force every team URL to the requested level. A gaps file produced by
    # `app.py --level jv` already carries it (apply_level is idempotent), but a
    # varsity gaps file passed with --level jv would otherwise silently scrape
    # VARSITY stats while stage 3 scraped JV box scores — a mismatch that would
    # be very hard to spot in the merged output.
    from scrape_box_scores import normalise_level, apply_level
    level = normalise_level(level)
    if level != 'varsity':
        for t in teams:
            t['teamUrl'] = apply_level(t['teamUrl'], level)

    season_suffix = _short_season(season)
    out_dir = os.path.dirname(output_file)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    print(f"Input            : {input_file} (detected as: {src_format})")
    print(f"Teams to scrape  : {len(teams)}")
    print(f"Season URL suffix: {season_suffix or '(current)'}")
    print(f"Level            : {level}"
          + ("" if level == 'varsity' else f" (URL segment /{level})"))
    print(f"Workers          : {workers}")
    print(f"Output           : {output_file}")
    print("-" * 70)

    if not teams:
        # Still write an empty file so downstream can see the run happened.
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)
        print("No teams to process — wrote empty output file.")
        return output_file

    all_records: list = []
    success = 0
    skipped: list = []
    done = 0
    lock = threading.Lock()

    def worker(t):
        nonlocal done, success
        try:
            name, records, status = _process_team(
                t['teamUrl'], t.get('total_games_checked'), season_suffix,
            )
        except Exception as e:
            with lock:
                done += 1
                skipped.append({'teamUrl': t['teamUrl'],
                                'teamName': t.get('teamName', ''),
                                'reason': f'exception: {e}'})
                print(f"  [{done:>4}/{len(teams)}] CRASH | "
                      f"{t.get('teamName','?')}: {e}")
            return
        with lock:
            done += 1
            if records:
                all_records.extend(records)
                success += 1
                n_players = sum(1 for r in records if r['record_type'] == 'player')
                gp = next((r['GP'] for r in records if r['record_type'] == 'team_total'), 0)
                print(f"  [{done:>4}/{len(teams)}] OK     | {name:35s}  "
                      f"GP={gp:>2}  players={n_players}")
            else:
                skipped.append({'teamUrl': t['teamUrl'],
                                'teamName': t.get('teamName', name),
                                'reason': status})
                print(f"  [{done:>4}/{len(teams)}] skip:{status:<12} | {name}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, t) for t in teams]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                with lock:
                    done += 1
                    print(f"  [{done:>4}/{len(teams)}] CRASH outer: {e}")

    # Atomic write — same shape (flat array) as Accumulation_data.py output.
    tmp = output_file + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(all_records, f, indent=4, ensure_ascii=False)
    os.replace(tmp, output_file)

    # Also write a sidecar report so we know what skipped and why.
    sidecar = output_file.replace('.json', '_report.json')
    report = {
        'sourceInput':         os.path.abspath(input_file),
        'season':              season,
        'teamsInInput':        len(teams),
        'teamsScraped':        success,
        'teamsSkipped':        len(skipped),
        'totalRecords':        len(all_records),
        'team_totalRecords':   sum(1 for r in all_records if r['record_type'] == 'team_total'),
        'playerRecords':       sum(1 for r in all_records if r['record_type'] == 'player'),
        'skipReasons':         dict(Counter(s['reason'] for s in skipped)),
        'skipped':             skipped,
        'generatedAt':         time.strftime('%Y-%m-%d %H:%M:%S'),
        'outputFile':          os.path.abspath(output_file),
    }
    tmp2 = sidecar + ".tmp"
    with open(tmp2, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    os.replace(tmp2, sidecar)

    print()
    print("=" * 70)
    print(f"  Teams scraped : {success}")
    print(f"  Teams skipped : {len(skipped)}")
    if skipped:
        for reason, n in Counter(s['reason'] for s in skipped).most_common():
            print(f"    {n:>4}: {reason}")
    print(f"  Records total : {len(all_records)}  "
          f"({sum(1 for r in all_records if r['record_type']=='team_total')} team_totals + "
          f"{sum(1 for r in all_records if r['record_type']=='player')} players)")
    print(f"  Output         : {output_file}")
    print(f"  Sidecar report : {sidecar}")
    print("=" * 70)
    return output_file


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input',  required=True,
                    help='find_stats_only_teams output, gaps file, or a JSON team list.')
    ap.add_argument('--season',  default='2025-2026',
                    help='Season for the print-stats URL (default 2025-2026).')
    ap.add_argument('--workers', type=int, default=TEAM_WORKERS,
                    help=f'Parallel worker count (default {TEAM_WORKERS}).')
    ap.add_argument('--level',   default='varsity',
                    choices=['varsity', 'jv', 'freshman'],
                    help='Team level (default: varsity). Applied to every team '
                         'URL so this stage matches the box-score stage.')
    ap.add_argument('--output',  default=None,
                    help='Output file path. Default: same folder as input, '
                         'name derived from input.')
    args = ap.parse_args()

    if args.output is None:
        base, ext = os.path.splitext(args.input)
        # Try sensible default names based on input convention
        if 'stats_only_teams' in args.input:
            args.output = args.input.replace('stats_only_teams', 'stats_tab_accumulated')
        elif 'data_gaps' in args.input:
            args.output = args.input.replace('data_gaps', 'stats_tab_accumulated')
        else:
            args.output = f"{base}_stats_tab_accumulated{ext}"

    run(args.input, args.season, args.workers, args.output, level=args.level)


if __name__ == '__main__':
    main()
