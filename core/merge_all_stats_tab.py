"""
Produce a FINAL accumulated file by overlaying the full Stats-tab scrape
on top of a per-game accumulated file. Stats-tab WINS wherever it has data.

Match key: (team_id, team_name). Both must agree exactly — this is
deliberate, the user wants the join to be conservative and verifiable.

For each (team_id, team_name) present in the stats-tab file:
  - drop ALL of that team's records from the accumulated file
    (team_total + every player row)
  - append the stats-tab file's records for that team instead.

For every other team in the accumulated file: keep the records untouched.
Result: stats-tab is the source of truth where it exists; per-game is
the source of truth everywhere else.

The previous accumulated file is NEVER modified. Output is a new file.

Usage:
  python merge_all_stats_tab.py \\
      --accumulated Texas_scraped_data/tx_accumulated_stats_boys_2025_2026_updated.json \\
      --stats-tab   Texas_scraped_data/tx_all_stats_tab_boys_2025_2026.json \\
      --output      Final_scraped_data/Final_tx_accumulated_boys_25_26.json
"""

import os
import json
import time
import argparse
from collections import defaultdict


_original_print = print
def print(*args, **kwargs):  # noqa: A001
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)


def merge(accumulated_path, stats_tab_path, output_path):
    if not os.path.exists(accumulated_path):
        print(f'[ERROR] Accumulated file not found: {accumulated_path}')
        return None
    if not os.path.exists(stats_tab_path):
        print(f'[ERROR] Stats-tab file not found: {stats_tab_path}')
        return None

    with open(accumulated_path, encoding='utf-8') as f:
        prev = json.load(f)
    with open(stats_tab_path, encoding='utf-8') as f:
        stab = json.load(f)
    if not isinstance(prev, list) or not isinstance(stab, list):
        print('[ERROR] Both inputs must be flat JSON lists.')
        return None

    # Build per-team lookups by (team_id, team_name) key.
    #   - stab_team_total: stats-tab's team_total row, used to read its GP
    #   - acc_team_total : accumulated's team_total row, used to read its GP
    stab_team_total = {(r['team_id'], r['team_name']): r for r in stab
                       if r.get('record_type') == 'team_total'}
    acc_team_total  = {(r['team_id'], r['team_name']): r for r in prev
                       if r.get('record_type') == 'team_total'}
    stab_keys = set(stab_team_total)
    acc_keys  = set(acc_team_total)
    matched_keys   = stab_keys & acc_keys
    unmatched_keys = stab_keys - acc_keys

    # GP=0 guard: if stats-tab GP == 0 AND accumulated GP > 0, the stats-tab
    # row is empty/placeholder and the per-game pipeline already captured
    # real data — KEEP the accumulated record. The stats-tab records for
    # this team are also dropped (we don't want to append zeroes either).
    skipped_for_gp_guard = set()
    for k in list(stab_keys):
        stab_gp = int(stab_team_total[k].get('GP') or 0)
        acc_gp  = int(acc_team_total.get(k, {}).get('GP') or 0)
        if stab_gp == 0 and acc_gp > 0:
            skipped_for_gp_guard.add(k)

    # The set of keys we will actually REPLACE = stats-tab keys minus the
    # guard-skipped ones. Used both to drop from prev and to filter what we
    # append from stats-tab.
    replace_keys = stab_keys - skipped_for_gp_guard

    # Walk previous file: drop every record whose (team_id, team_name)
    # is in REPLACE_KEYS (stats-tab teams that pass the GP guard).
    # Records for guard-skipped teams stay untouched.
    merged = []
    replaced_team_totals = 0
    replaced_player_rows = 0
    kept_team_totals = 0
    kept_player_rows = 0
    for r in prev:
        k = (r.get('team_id'), r.get('team_name'))
        if k in replace_keys:
            if r.get('record_type') == 'team_total':
                replaced_team_totals += 1
            elif r.get('record_type') == 'player':
                replaced_player_rows += 1
            continue
        merged.append(r)
        if r.get('record_type') == 'team_total':
            kept_team_totals += 1
        elif r.get('record_type') == 'player':
            kept_player_rows += 1

    # Append stats-tab records ONLY for teams in replace_keys — never the
    # guard-skipped ones (they'd be zero-only and pollute the output).
    stab_to_append = [r for r in stab
                      if (r.get('team_id'), r.get('team_name')) in replace_keys]
    new_team_totals = sum(1 for r in stab_to_append if r.get('record_type') == 'team_total')
    new_player_rows = sum(1 for r in stab_to_append if r.get('record_type') == 'player')
    merged.extend(stab_to_append)

    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    tmp = output_path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=4, ensure_ascii=False)
    os.replace(tmp, output_path)

    print('=' * 72)
    print(f'  Accumulated input  : {accumulated_path}')
    print(f'  Stats-tab input    : {stats_tab_path}')
    print(f'  Output             : {output_path}')
    print('-' * 72)
    print(f'  Acc team_totals in input        : {len(acc_keys)}')
    print(f'  Stab team_totals in input       : {len(stab_keys)}')
    print(f'  Matched against acc             : {len(matched_keys)}')
    print(f'  Stab teams NOT in acc           : {len(unmatched_keys)}')
    print(f'  GP=0 guard skipped              : {len(skipped_for_gp_guard)} (stab GP=0 but acc GP>0 — kept acc)')
    print(f'  → Teams actually replaced       : {len(replace_keys)}')
    if unmatched_keys:
        print('    Sample unmatched stab teams:')
        for k in list(unmatched_keys)[:5]:
            print(f'      {k}')
    if skipped_for_gp_guard:
        print('    Sample guard-skipped teams (kept acc, stab was empty):')
        for k in list(skipped_for_gp_guard)[:5]:
            acc_gp = acc_team_total[k].get('GP')
            print(f'      acc_GP={acc_gp:>3}  {k}')
    print('-' * 72)
    print(f'  prev records replaced (dropped) : {replaced_team_totals} team_totals + {replaced_player_rows} players')
    print(f'  prev records kept               : {kept_team_totals} team_totals + {kept_player_rows} players')
    print(f'  stab records appended           : {new_team_totals} team_totals + {new_player_rows} players')
    print(f'  TOTAL records in output         : {len(merged)}')
    print('=' * 72)

    # Sanity counts
    out_team_totals = sum(1 for r in merged if r.get('record_type') == 'team_total')
    out_player_rows = sum(1 for r in merged if r.get('record_type') == 'player')
    print(f'  Output team_totals : {out_team_totals}')
    print(f'  Output player rows : {out_player_rows}')
    print(f'  Sum check          : {out_team_totals + out_player_rows} '
          f'(matches total: {out_team_totals + out_player_rows == len(merged)})')

    return {
        'output':             output_path,
        'acc_team_totals':    len(acc_keys),
        'stab_team_totals':   len(stab_keys),
        'replaced':           len(matched_keys),
        'unmatched':          len(unmatched_keys),
        'records_total':      len(merged),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--accumulated', required=True,
                    help='Per-game accumulated file (typically the *_updated.json).')
    ap.add_argument('--stats-tab',   required=True,
                    help='Full stats-tab scrape file (tx_all_stats_tab_*.json).')
    ap.add_argument('--output',      required=True,
                    help='Final output path (will be created).')
    args = ap.parse_args()
    merge(args.accumulated, args.stats_tab, args.output)


if __name__ == '__main__':
    main()
