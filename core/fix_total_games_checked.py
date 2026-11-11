"""
Post-process an existing accumulated / final file to enforce the rule:

    TotalGamesChecked = max(current_TotalGamesChecked, distinct_contest_ids_in_box_scores)

The gap finder occasionally undercounts: it enumerates the schedule from the
schedule.json endpoint and stamps `gamesChecked = N`, but the scraper later
walks the same schedule and discovers N+1 games (MaxPreps' schedule endpoint
can return slightly different counts across calls within the same run).

That gap propagates: TotalGamesChecked on team_total records is set from the
gap finder's count, but GP is set from what the accumulator actually summed
— so GP can exceed TotalGamesChecked, which is nonsensical (we can't have
more games of data than we checked).

This script rewrites every team_total in a final file with the corrected
value using the box-score file as ground truth — the distinct count of
contest_ids per (team_id, team_name) is what we actually verified.

The fix applies regardless of record provenance:
  - team_totals from per-game accumulation: max(TGC, distinct_box_count)
  - team_totals from stats-tab fallback:   same rule (the box-score count
    represents reality on the schedule, regardless of whose stats we used).

The original file is NEVER modified. Output is written to a new file.

Usage:
  python fix_total_games_checked.py \\
      --input      Final_scraped_data/Final_tx_accumulated_boys_25_26.json \\
      --box-scores Texas_scraped_data/tx_box_scores_boys_2025_2026.json \\
      --output     Final_scraped_data/Final_tx_accumulated_boys_25_26.json
"""

import os
import json
import time
import argparse
from collections import defaultdict


_original_print = print
def print(*args, **kwargs):  # noqa: A001
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)


def _build_box_count_lookup(box_scores_path):
    """For each (team_id, team_name) in the box-score file, count the number
    of distinct contest_ids attributed to that team's `team.team_id` field.
    That's the authoritative "games of data we actually scraped for them"."""
    with open(box_scores_path, encoding='utf-8') as f:
        bs = json.load(f)
    games = bs.get('games', bs) if isinstance(bs, dict) else bs

    by_team = defaultdict(set)
    for g in games:
        team = g.get('team') or {}
        tid  = team.get('team_id') or ''
        tname = team.get('team_name') or ''
        cid  = g.get('contest_id')
        if tid and cid:
            by_team[(tid, tname)].add(cid)

    return {k: len(v) for k, v in by_team.items()}


def fix(input_path, box_scores_path, output_path):
    if not os.path.exists(input_path):
        print(f'[ERROR] Input file not found: {input_path}')
        return None
    if not os.path.exists(box_scores_path):
        print(f'[ERROR] Box scores file not found: {box_scores_path}')
        return None

    with open(input_path, encoding='utf-8') as f:
        records = json.load(f)
    if not isinstance(records, list):
        print('[ERROR] Input must be a flat JSON list of records.')
        return None

    box_count_lookup = _build_box_count_lookup(box_scores_path)
    print(f'Box-score teams found: {len(box_count_lookup)}')

    updated = []
    bumped = 0
    unchanged = 0
    no_box_match = 0
    no_tgc_field = 0
    examples_bumped = []
    for r in records:
        if r.get('record_type') != 'team_total':
            updated.append(r)
            continue
        key = (r.get('team_id'), r.get('team_name'))
        box_count = box_count_lookup.get(key, 0)
        if key not in box_count_lookup:
            no_box_match += 1
        current = r.get('TotalGamesChecked')
        gp = int(r.get('GP') or 0)
        # Authoritative count = max of every known signal:
        #  - current TotalGamesChecked (gap finder's count)
        #  - distinct box-score contest_ids for this team (what we scraped)
        #  - GP on the record (per-game accumulation count OR stats-tab GP)
        target_tgc = max(int(current or 0), int(box_count), gp)

        if current is None:
            # Record never had a TotalGamesChecked field at all. Add one in
            # the canonical position (right after GP).
            no_tgc_field += 1
            new_rec = {}
            for k, v in r.items():
                new_rec[k] = v
                if k == 'GP':
                    new_rec['TotalGamesChecked'] = target_tgc
            updated.append(new_rec)
            continue
        if target_tgc != int(current):
            bumped += 1
            if len(examples_bumped) < 10:
                examples_bumped.append({
                    'team_name': r.get('team_name'),
                    'team_id':   r.get('team_id'),
                    'GP':        gp,
                    'old_TGC':   current,
                    'box_count': box_count,
                    'new_TGC':   target_tgc,
                })
            r = {**r}
            r['TotalGamesChecked'] = target_tgc
            updated.append(r)
        else:
            unchanged += 1
            updated.append(r)

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    tmp = output_path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(updated, f, indent=4, ensure_ascii=False)
    os.replace(tmp, output_path)

    print('=' * 72)
    print(f'  Input              : {input_path}')
    print(f'  Box scores         : {box_scores_path}')
    print(f'  Output             : {output_path}')
    print('-' * 72)
    print(f'  team_total rows       : {sum(1 for r in records if r.get("record_type") == "team_total")}')
    print(f'  TotalGamesChecked bumped : {bumped}')
    print(f'  TotalGamesChecked unchanged : {unchanged}')
    print(f'  team_totals without box-score data : {no_box_match}')
    print(f'  team_totals previously missing TGC field : {no_tgc_field}')
    if examples_bumped:
        print('  Sample bumps:')
        for e in examples_bumped:
            print(f'    {e["team_name"]:40s}  GP={e["GP"]:>3}  '
                  f'TGC: {e["old_TGC"]} -> {e["new_TGC"]} '
                  f'(box_count={e["box_count"]})')
    print('=' * 72)
    return {
        'bumped':         bumped,
        'unchanged':      unchanged,
        'no_box_match':   no_box_match,
        'no_tgc_field':   no_tgc_field,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input',      required=True,
                    help='Final/accumulated JSON file to fix.')
    ap.add_argument('--box-scores', required=True,
                    help='The box_scores JSON file for the same dataset.')
    ap.add_argument('--output',     required=True,
                    help='Output file (may be the same as --input to overwrite).')
    args = ap.parse_args()
    fix(args.input, args.box_scores, args.output)


if __name__ == '__main__':
    main()
