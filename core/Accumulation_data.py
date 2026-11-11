import json
import os
import re
import time
import glob
from collections import defaultdict

# Timestamped print: every log line gets a "[YYYY-MM-DD HH:MM:SS]" prefix.
_original_print = print
def print(*args, **kwargs):
    _original_print(time.strftime('[%Y-%m-%d %H:%M:%S]'), *args, **kwargs)


def _build_master_name_lookup():
    """
    Build team_id -> full team name from all master list JSON files found
    next to this script. Used to ensure accumulated stats always carry the
    correct full name (e.g. 'Boerne Greyhounds') regardless of what the
    gaps file or box scores stored.
    """
    lookup = {}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for master_file in glob.glob(os.path.join(script_dir, '*basketball_all_states*.json')):
        try:
            with open(master_file, encoding='utf-8') as f:
                data = json.load(f)
            for state_data in data.get('byState', {}).values():
                for region_data in state_data.get('regions', {}).values():
                    for t in region_data.get('teams', []):
                        url = t.get('teamUrl', '')
                        tid = url.replace('https://www.maxpreps.com/', '').rstrip('/')
                        name = t.get('teamName', '')
                        if tid and name:
                            name = name.replace('Aandm', 'A&M').replace('aandm', 'a&m')
                            lookup[tid] = name
                        elif tid:
                            # Derive name from URL slug if master list lacks it.
                            m = re.match(r'[^/]+/[^/]+/([^/]+)/', tid)
                            if m:
                                lookup[tid] = m.group(1).replace('-', ' ').title()
        except Exception:
            pass
    return lookup


def _slugify(name):
    """Match the slug scheme used by scrape_box_scores.py for opponent team_ids."""
    return re.sub(r'[^a-z0-9]+', '-', (name or '').lower()).strip('-')


def _gaps_path_for(input_file):
    """Derive the matching data-gaps file path from a box-scores input path.

    e.g. 'tx_box_scores_girls_2025_2026.json' → 'tx_data_gaps_girls_2025_2026.json'.
    Returns None if the input path doesn't follow the expected pattern OR
    the derived gaps file doesn't exist next to the input.
    """
    if not input_file:
        return None
    folder = os.path.dirname(os.path.abspath(input_file)) or "."
    fname = os.path.basename(input_file)
    if "box_scores" not in fname:
        return None
    candidate = os.path.join(folder, fname.replace("box_scores", "data_gaps"))
    return candidate if os.path.exists(candidate) else None


def _load_games_checked_lookup(gaps_file):
    """Build {(team_id, team_name): gamesChecked} from a data-gaps file.

    team_id is derived from teamUrl by stripping the maxpreps.com prefix and
    trailing slash so it matches the canonical team_id we use in the
    accumulated-stats output. team_name is matched exactly (the gap finder
    now writes URL-derived names, so they match what the accumulator emits)."""
    if not gaps_file or not os.path.exists(gaps_file):
        return {}
    try:
        with open(gaps_file, encoding='utf-8') as f:
            gaps = json.load(f)
    except Exception as e:
        print(f"  [WARN] Could not read gaps file {gaps_file}: {e}")
        return {}
    lookup = {}
    for bucket in ('teamsFullBoxScores', 'teamsPartialBoxScores', 'teamsNoBoxScores'):
        for t in gaps.get(bucket, []):
            url = t.get('teamUrl', '')
            if not url:
                continue
            tid = url.replace('https://www.maxpreps.com/', '').rstrip('/')
            tname = (t.get('teamName') or '').replace('Aandm', 'A&M').replace('aandm', 'a&m')
            gc = t.get('gamesChecked')
            if tid and tname and gc is not None:
                lookup[(tid, tname)] = gc
    return lookup

def safe_float(value):
    try:
        if value is None:
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def calculate_percentage(made, attempted):
    if attempted == 0:
        return 0.0
    return round((made / attempted) * 100)

def calculate_ratio(num, den):
    if den == 0:
        return 0.0
    return round(num / den, 2)

def process_stats(input_file=None, output_file=None):
    if input_file is None:
        input_file = '/Users/sultan/Documents/Projects/Scraper/all_games_stats_2024_2025.json'
    if output_file is None:
        output_file = '/Users/sultan/Documents/Projects/Scraper/accumulated_stats_2024_2025.json'

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    # Handle both flat list and wrapped {"meta": ..., "games": [...]} format
    data = raw.get("games", raw) if isinstance(raw, dict) else raw

    # Load master name lookup — full team names from boys/girls_basketball_all_states.json
    master_names = _build_master_name_lookup()
    if master_names:
        print(f"  Master name lookup loaded: {len(master_names)} teams.")
    else:
        print("  Master name lookup not found — team names will use box score values.")

    # Load gap-finder "gamesChecked" counts so each team_total record can carry
    # a TotalGamesChecked field alongside its GP. Lets scouting reports show
    # phrases like "based on 28 of 37 games" without needing two files.
    gaps_path = _gaps_path_for(input_file)
    games_checked_lookup = _load_games_checked_lookup(gaps_path)
    if games_checked_lookup:
        print(f"  Games-checked lookup loaded from {os.path.basename(gaps_path)}: "
              f"{len(games_checked_lookup)} teams.")
    else:
        print("  Games-checked lookup not found — team_total records will not include TotalGamesChecked.")

    # A "scraped" team has a full canonical path ID (e.g. tx/city/team/basketball).
    # The scraper always generates SHORT slug IDs for opponents (regardless of whether
    # that opponent is also a scraped team), so we can't tell from the team_id alone
    # whether an opponent corresponds to a scraped team. We build a slug->full_id map
    # to detect this and avoid double-counting / ghost team records.
    def _is_full_path(team_id):
        return team_id.count('/') >= 3

    # Map opponent-slug -> scraped full team_id, using both the master list and
    # team-side appearances in the box scores.
    print("  Building slug -> scraped team_id map...")
    slug_to_full = {}
    for tid, name in master_names.items():
        slug = _slugify(name)
        if slug:
            slug_to_full[slug] = tid
        # Also map the last URL segment (e.g. 'avinger-indians') in case the
        # name -> slug conversion drifts from the URL.
        last = tid.split('/')[-2] if tid.count('/') >= 3 else None
        if last:
            slug_to_full.setdefault(last, tid)
    for game in data:
        if game.get('is_deleted'):
            continue
        t_id = game.get('team', {}).get('team_id', '')
        t_name = game.get('team', {}).get('team_name', '')
        if _is_full_path(t_id):
            for s in {_slugify(t_name), t_id.split('/')[-2] if t_id.count('/') >= 3 else ''}:
                if s:
                    slug_to_full.setdefault(s, t_id)

    def _resolve_team_id(team_id):
        """If team_id is a short slug that matches a known scraped team, return the
        full canonical id; otherwise return team_id unchanged."""
        if _is_full_path(team_id) or not team_id:
            return team_id
        return slug_to_full.get(team_id, team_id)

    # Pass 1: determine each player's primary team.
    # - Count team-side appearances for all teams.
    # - Count opponent-side appearances ONLY when the opponent is NOT a scraped team
    #   (i.e. its slug doesn't resolve to a full canonical id). This prevents players
    #   on scraped teams from having their stats wrongly assigned to a slug ghost id.
    print("  Pass 1: building player->primary-team map...")
    player_team_game_count = defaultdict(lambda: defaultdict(int))
    for game in data:
        if game.get('is_deleted'):
            continue
        # Team side
        t_id = game.get('team', {}).get('team_id', '')
        if t_id:
            game_players = set()
            for section in ('shooting', 'detailed_shooting', 'totals', 'misc'):
                for p in game.get(section, {}).get('team', {}).get('players', []):
                    p_name = f"{p['player_name']}({p.get('class', '')})"
                    game_players.add(p_name)
            for p_name in game_players:
                player_team_game_count[p_name][t_id] += 1
        # Opponent side — skip if this slug resolves to a known scraped team.
        o_id_raw = game.get('opponent', {}).get('team_id', '')
        if o_id_raw and not _is_full_path(o_id_raw):
            resolved = _resolve_team_id(o_id_raw)
            if resolved == o_id_raw:  # truly unscraped opponent
                opp_players = set()
                for section in ('shooting', 'detailed_shooting', 'totals', 'misc'):
                    for p in game.get(section, {}).get('opponent', {}).get('players', []):
                        p_name = f"{p['player_name']}({p.get('class', '')})"
                        opp_players.add(p_name)
                for p_name in opp_players:
                    player_team_game_count[p_name][o_id_raw] += 1

    # Tiebreak: prefer full-canonical IDs over slug IDs so a player whose stats
    # are split between their scraped team and a slug ghost lands on the real team.
    def _pick_primary(counts):
        max_count = max(counts.values())
        candidates = [tid for tid, c in counts.items() if c == max_count]
        full = [c for c in candidates if _is_full_path(c)]
        return full[0] if full else candidates[0]

    player_primary_team = {
        p_name: _pick_primary(counts)
        for p_name, counts in player_team_game_count.items()
    }
    print(f"  Pass 1 complete. {len(player_primary_team)} unique players mapped.")

    # Pass 2: accumulate stats, skipping any player whose primary team differs
    # from the team currently being processed (eliminates cross-team ghost records).

    # team_stats[team_id] = { player_name: { stats } }
    # Plus a special key "Season Totals"
    all_teams_data = {}

    for i, game in enumerate(data, 1):
        if game.get('is_deleted'):
            continue
        
        if i % 1000 == 0 or i == len(data):
            print(f"  Accumulating: {i}/{len(data)} games processed...")
        
        # We need to process both 'team' and 'opponent' stats from the perspective of their respective teams
        # But wait, the JSON structure has 'shooting', 'detailed_shooting', 'totals', 'misc' sections.
        # Each has 'team' and 'opponent'.
        
        # Helper to process a side (team or opponent)
        def process_side(side_key, team_info):
            if not team_info or not team_info.get('team_id'):
                return
            
            t_id = team_info['team_id']
            # Use master list full name if available, fall back to box score name
            t_name = master_names.get(t_id) or team_info['team_name']

            if t_id not in all_teams_data:
                all_teams_data[t_id] = {
                    'team_name': t_name,
                    'players': defaultdict(lambda: {
                        'GP': 0, 'Min': 0.0, 'Pts': 0.0, 'FGM': 0.0, 'FGA': 0.0,
                        '3PM': 0.0, '3PA': 0.0, 'FTM': 0.0, 'FTA': 0.0, '2FGM': 0.0, '2FGA': 0.0,
                        'OReb': 0.0, 'DReb': 0.0, 'Reb': 0.0, 'Ast': 0.0, 'Stl': 0.0, 'Blk': 0.0,
                        'TO': 0.0, 'PF': 0.0, 'Chr': 0.0, 'Defl': 0.0, 'TF': 0.0,
                        'DD': 0, 'TD': 0
                    }),
                    'season_totals': {
                        'GP': 0, 'Min': 0.0, 'Pts': 0.0, 'FGM': 0.0, 'FGA': 0.0,
                        '3PM': 0.0, '3PA': 0.0, 'FTM': 0.0, 'FTA': 0.0, '2FGM': 0.0, '2FGA': 0.0,
                        'OReb': 0.0, 'DReb': 0.0, 'Reb': 0.0, 'Ast': 0.0, 'Stl': 0.0, 'Blk': 0.0,
                        'TO': 0.0, 'PF': 0.0, 'Chr': 0.0, 'Defl': 0.0, 'TF': 0.0,
                        'DD': 0, 'TD': 0
                    }
                }

            team_players = all_teams_data[t_id]['players']
            season_t = all_teams_data[t_id]['season_totals']
            
            # Map of player_name -> combined_stats for this game
            game_players_stats = defaultdict(lambda: {
                'Min': 0.0, 'Pts': 0.0, 'FGM': 0.0, 'FGA': 0.0,
                '3PM': 0.0, '3PA': 0.0, 'FTM': 0.0, 'FTA': 0.0, '2FGM': 0.0, '2FGA': 0.0,
                'OReb': 0.0, 'DReb': 0.0, 'Reb': 0.0, 'Ast': 0.0, 'Stl': 0.0, 'Blk': 0.0,
                'TO': 0.0, 'PF': 0.0, 'Chr': 0.0, 'Defl': 0.0, 'TF': 0.0
            })

            def _is_primary(p_name):
                """Return True only if this player's primary team is the current team."""
                primary = player_primary_team.get(p_name)
                return primary is None or primary == t_id

            # 1. Shooting stats
            shooting_side = game.get('shooting', {}).get(side_key, {})
            if shooting_side and shooting_side.get('players'):
                for p in shooting_side['players']:
                    p_name = f"{p['player_name']}({p.get('class', '')})"
                    if not _is_primary(p_name):
                        continue
                    stats = game_players_stats[p_name]
                    stats['Min'] += safe_float(p.get('minutes_played'))
                    stats['Pts'] += safe_float(p.get('points'))
                    stats['FGM'] += safe_float(p.get('fg_made'))
                    stats['FGA'] += safe_float(p.get('fg_attempts'))

            # 2. Detailed Shooting
            detailed_side = game.get('detailed_shooting', {}).get(side_key, {})
            if detailed_side and detailed_side.get('players'):
                for p in detailed_side['players']:
                    p_name = f"{p['player_name']}({p.get('class', '')})"
                    if not _is_primary(p_name):
                        continue
                    stats = game_players_stats[p_name]
                    stats['3PM'] += safe_float(p.get('3pt_made'))
                    stats['3PA'] += safe_float(p.get('3pt_attempts'))
                    stats['FTM'] += safe_float(p.get('ft_made'))
                    stats['FTA'] += safe_float(p.get('ft_attempts'))
                    stats['2FGM'] += safe_float(p.get('2pt_made'))
                    stats['2FGA'] += safe_float(p.get('2pt_attempts'))

            # 3. Totals
            totals_side = game.get('totals', {}).get(side_key, {})
            if totals_side and totals_side.get('players'):
                for p in totals_side['players']:
                    p_name = f"{p['player_name']}({p.get('class', '')})"
                    if not _is_primary(p_name):
                        continue
                    stats = game_players_stats[p_name]
                    stats['OReb'] += safe_float(p.get('offensive_rebounds'))
                    stats['DReb'] += safe_float(p.get('defensive_rebounds'))
                    stats['Reb'] += safe_float(p.get('rebounds'))
                    stats['Ast'] += safe_float(p.get('assists'))
                    stats['Stl'] += safe_float(p.get('steals'))
                    stats['Blk'] += safe_float(p.get('blocks'))
                    stats['TO'] += safe_float(p.get('turnovers'))
                    stats['PF'] += safe_float(p.get('personal_fouls'))

            # 4. Misc
            misc_side = game.get('misc', {}).get(side_key, {})
            if misc_side and misc_side.get('players'):
                for p in misc_side['players']:
                    p_name = f"{p['player_name']}({p.get('class', '')})"
                    if not _is_primary(p_name):
                        continue
                    stats = game_players_stats[p_name]
                    stats['Chr'] += safe_float(p.get('charges_taken'))
                    stats['Defl'] += safe_float(p.get('deflections'))
                    stats['TF'] += safe_float(p.get('technical_fouls'))

            # Update accumulated stats and Check Double-Double / Triple-Double for this game
            if game_players_stats:
                season_t['GP'] += 1
            
            for p_name, g_stats in game_players_stats.items():
                p_acc = team_players[p_name]
                p_acc['GP'] += 1
                for key in g_stats:
                    p_acc[key] += g_stats[key]
                    season_t[key] += g_stats[key]
                
                # Double-Double / Triple-Double check
                categories = [g_stats['Pts'], g_stats['Reb'], g_stats['Ast'], g_stats['Stl'], g_stats['Blk']]
                double_digit_count = sum(1 for x in categories if x >= 10)
                if double_digit_count >= 2:
                    p_acc['DD'] += 1
                    season_t['DD'] += 1
                if double_digit_count >= 3:
                    p_acc['TD'] += 1
                    season_t['TD'] += 1

        process_side('team', game.get('team'))
        # Process opponent side only for truly-unscraped opponents. If the slug
        # resolves to a scraped team, that team's real stats live under its full
        # canonical id — accumulating here would create a ghost slug record.
        opp_info = game.get('opponent', {})
        opp_id_raw = opp_info.get('team_id', '') if opp_info else ''
        if (opp_info and opp_id_raw and not _is_full_path(opp_id_raw)
                and _resolve_team_id(opp_id_raw) == opp_id_raw):
            process_side('opponent', opp_info)

    # Final calculation and formatting
    final_output_list = []
    skipped_empty = 0
    for t_id, t_data in all_teams_data.items():
        team_name = t_data['team_name']
        players_accumulated = t_data['players']
        season_totals_accumulated = t_data['season_totals']

        # Drop empty ghost records: slug-id teams (unscraped opponents) whose box
        # score appearances never included any player stats. They contribute zero
        # data and just clutter the output. Keep full-canonical teams even at
        # zero so the scraped-team list stays complete.
        if (not _is_full_path(t_id)
                and season_totals_accumulated['GP'] == 0
                and not players_accumulated):
            skipped_empty += 1
            continue
        
        def format_record(name, acc, record_type):
            gp = acc['GP']
            gp_calc = gp if gp > 0 else 1
            
            res = {
                'team_id': t_id,
                'team_name': team_name,
                'record_type': record_type, # 'player' or 'team_total'
                'Name': name,
                'GP': gp,
                'MPG': round(acc['Min'] / gp_calc, 1),
                'PPG': round(acc['Pts'] / gp_calc, 1),
                'DEFR': round(acc['DReb'] / gp_calc, 1),
                'OFFR': round(acc['OReb'] / gp_calc, 1),
                'RPG': round(acc['Reb'] / gp_calc, 1),
                'APG': round(acc['Ast'] / gp_calc, 1),
                'SPG': round(acc['Stl'] / gp_calc, 1),
                'BPG': round(acc['Blk'] / gp_calc, 1),
                'TPG': round(acc['TO'] / gp_calc, 1),
                'PFPG': round(acc['PF'] / gp_calc, 1),
                
                'Min': int(acc['Min']),
                'Pts': int(acc['Pts']),
                'FGM': int(acc['FGM']),
                'FGA': int(acc['FGA']),
                'FG%': calculate_percentage(acc['FGM'], acc['FGA']),
                'PPS': round(acc['Pts'] / acc['FGA'], 1) if acc['FGA'] > 0 else 0.0,
                'AFG%': calculate_percentage(acc['FGM'] + 0.5 * acc['3PM'], acc['FGA']),
                
                '3PM': int(acc['3PM']),
                '3PA': int(acc['3PA']),
                '3P%': calculate_percentage(acc['3PM'], acc['3PA']),
                'FTM': int(acc['FTM']),
                'FTA': int(acc['FTA']),
                'FT%': calculate_percentage(acc['FTM'], acc['FTA']),
                '2FGM': int(acc['2FGM']),
                '2FGA': int(acc['2FGA']),
                '2FG%': calculate_percentage(acc['2FGM'], acc['2FGA']),
                
                'OReb': int(acc['OReb']),
                'DReb': int(acc['DReb']),
                'Reb': int(acc['Reb']),
                'Ast': int(acc['Ast']),
                'Stl': int(acc['Stl']),
                'Blk': int(acc['Blk']),
                'TO': int(acc['TO']),
                'PF': int(acc['PF']),
                
                'Ast:TO': calculate_ratio(acc['Ast'], acc['TO']),
                'Stl:TO': calculate_ratio(acc['Stl'], acc['TO']),
                'Stl:PF': calculate_ratio(acc['Stl'], acc['PF']),
                'Blk:PF': calculate_ratio(acc['Blk'], acc['PF']),
                'Chr': int(acc['Chr']),
                'Defl': int(acc['Defl']),
                'TF': int(acc['TF']),
                'DD': acc['DD'],
                'TD': acc['TD']
            }
            
            # Per 32 calculation
            if acc['Min'] > 0:
                res['Per_32'] = {
                    'Pts': round((acc['Pts'] / acc['Min']) * 32, 1),
                    'Reb': round((acc['Reb'] / acc['Min']) * 32, 1),
                    'Ast': round((acc['Ast'] / acc['Min']) * 32, 1),
                    'Stl': round((acc['Stl'] / acc['Min']) * 32, 1),
                    'Blk': round((acc['Blk'] / acc['Min']) * 32, 1)
                }
            else:
                res['Per_32'] = None
                
            return res

        # Add team totals object — annotated with TotalGamesChecked from the
        # gap-finder file when we have a match. We only add this to team_total
        # rows (not players) and only when a match exists, so player records
        # and unmatched team_totals stay untouched.
        #
        # Use max(gap_count, GP). The gap finder occasionally undercounts (its
        # schedule walker can disagree with the scraper's by 1-2 games when
        # MaxPreps returns a slightly different schedule shape per call). GP
        # is the count of distinct games we actually accumulated stats for,
        # so TotalGamesChecked must never be less than GP — otherwise the
        # output reports we "checked" fewer games than we have data for.
        season_record = format_record("Season Totals", season_totals_accumulated, "team_total")
        gc = games_checked_lookup.get((t_id, team_name))
        if gc is not None:
            gp = int(season_record.get('GP') or 0)
            total_games_checked = max(int(gc), gp)
            # Reinsert keys in order so TotalGamesChecked lands right after GP.
            new_rec = {}
            for k, v in season_record.items():
                new_rec[k] = v
                if k == 'GP':
                    new_rec['TotalGamesChecked'] = total_games_checked
            season_record = new_rec
        final_output_list.append(season_record)

        # Add individual player objects (TotalGamesChecked NOT added to these)
        for p_name, p_acc in players_accumulated.items():
            final_output_list.append(format_record(p_name, p_acc, "player"))

    # ── Zero-stat placeholders for teams the gap finder saw but with no data ──
    # If a team's gap-finder entry shows gamesChecked > 0 but gamesWithStats = 0
    # (no team uploaded for any game on their schedule), the team produces zero
    # box-score records and so never lands in all_teams_data. Without this step
    # they'd vanish from the accumulated output, which downstream consumers
    # can't tell apart from "team doesn't exist".
    #
    # Emit a team_total row with all stat fields = 0 and TotalGamesChecked set
    # to whatever the gap finder verified, so scouting-report generators can
    # render "based on 0 of N games — no data exists on MaxPreps for this team".
    emitted_team_ids = {r.get('team_id') for r in final_output_list
                        if r.get('record_type') == 'team_total'}
    placeholders_added = 0
    for (t_id, t_name), gc in games_checked_lookup.items():
        if t_id in emitted_team_ids:
            continue
        canonical_name = master_names.get(t_id) or t_name
        placeholder = {
            'team_id':     t_id,
            'team_name':   canonical_name,
            'record_type': 'team_total',
            'Name':        'Season Totals',
            'GP':          0,
            'TotalGamesChecked': gc,
            'MPG': 0.0, 'PPG': 0.0, 'DEFR': 0.0, 'OFFR': 0.0,
            'RPG': 0.0, 'APG': 0.0, 'SPG': 0.0, 'BPG': 0.0,
            'TPG': 0.0, 'PFPG': 0.0,
            'Min': 0, 'Pts': 0, 'FGM': 0, 'FGA': 0,
            'FG%': 0.0, 'PPS': 0.0, 'AFG%': 0.0,
            '3PM': 0, '3PA': 0, '3P%': 0.0,
            'FTM': 0, 'FTA': 0, 'FT%': 0.0,
            '2FGM': 0, '2FGA': 0, '2FG%': 0.0,
            'OReb': 0, 'DReb': 0, 'Reb': 0, 'Ast': 0,
            'Stl': 0, 'Blk': 0, 'TO': 0, 'PF': 0,
            'Ast:TO': 0.0, 'Stl:TO': 0.0, 'Stl:PF': 0.0, 'Blk:PF': 0.0,
            'Chr': 0, 'Defl': 0, 'TF': 0,
            'DD': 0, 'TD': 0,
            'Per_32': None,
        }
        final_output_list.append(placeholder)
        placeholders_added += 1
    if placeholders_added:
        print(f"  Emitted {placeholders_added} zero-stat placeholder team_total "
              f"record(s) for teams whose box scores had no data anywhere.")

    tmp = output_file + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(final_output_list, f, indent=4, ensure_ascii=False)
    os.replace(tmp, output_file)
    
    print(f"Accumulation complete. Created {len(final_output_list)} records. Skipped {skipped_empty} empty slug-ghost teams.")
    print(f"Data saved to {output_file}")
    
    # Print sample for the first few records
    if final_output_list:
        print("\n--- SAMPLE OUTPUT DATA ---")
        for rec in final_output_list[:3]:
            print(f"Type: {rec['record_type']}, Team: {rec['team_name']}, Name: {rec['Name']}, PPG: {rec['PPG']}")


if __name__ == "__main__":
    process_stats()
