# Date-driven Gap Finder — 2026-27 workflow (phase 1)

Everything here is **new code in this folder**. Nothing in the existing
pipeline (`app.py`, `scrape_box_scores.py`, `APP/pipeline.py`, workflows…) was
modified; it is only *imported read-only* for the HTTP transport (curl_cffi →
curl → requests) and the box-score parser, so results match production
exactly (verified: 8/8 live SUCCESS records identical to `scrape_game()`).

Needs the VPN like the rest of the project. Optional: `pip install openpyxl`
for the Excel tracker (a CSV is written instead if it's missing).

## How the existing Gap Finder worked (and what changed)

`app.py --gap-only` re-fetched **every team's schedule and every game's box
score page** each run, with only a cache of games that already had stats
(`_build_game_cache`). It had no per-game status, timestamps, retry count or
log, and no notion of a date — so "what happened yesterday?" meant a full scan.

This workflow reuses its pieces (`name_from_url`, build-id handling,
`_raw_fetch_schedule`, `parse_game_page`, `_stats_fingerprint`, opponent
index, level helpers) but makes the **game** the unit of work:

| File | Role |
|---|---|
| `daily_gapfinder.py` | CLI + the 8-step daily flow |
| `schedule_source.py` | teams from the master file; schedule → game refs (date, URL, contest id, ssid, opponent) — **no box-score fetch** |
| `gap_store.py` | the Gap Finder file: schema, statuses, retry/back-off, due-game selection, Excel tracker, legacy-shape export |
| `game_checker.py` | one fetch per game → SUCCESS / NO_DATA / FAILED (+ the box-score record) |
| `dataset_store.py` | dedup-safe append/update of the box-score dataset |
| `wf_core.py` | statuses, date helpers, read-only legacy import |
| `test_offline.py` | 22 offline tests |

## Gap Finder file

Same name/location convention as before:
`{state}_data_gaps_{sport}[_jv]_{YYYY_YYYY}.json`, default folder
`output/{State}_scraped_data/` (`--output-dir` to override). It contains:

* `games` — **one row per team-game**, the source of truth (fields below)
* `teams` — roster + last schedule-fetch result per team
* `meta` — totals, `statusCounts`, run history
* `teamsFullBoxScores` / `teamsPartialBoxScores` / `teamsNoBoxScores` / `errors` —
  the **legacy team buckets, regenerated on every save**, so the stats-tab and
  box-score stages can read this file unchanged. An old-format file is
  auto-migrated into game rows the first time it is loaded.
* a sibling `*_tracker.xlsx` for Excel review (below)

Game row fields: `state, sport, season, level, team_id, team_name, team_url,
region, opponent_team_id, opponent_team_name, game_date (ISO), game_url,
contest_id, ssid, scheduled_time, game_type, status, data_retrieved,
data_class (full/team_only/opp_only), first_seen, last_attempt, last_success,
retry_count, next_retry_after, retry_exhausted, on_schedule, duplicate_of,
http_status, log_message, log_history (last 10), stats_fingerprint, record_hash`.
Identity: `game_key = team_id|contest_id`.

### Excel tracker
`*_tracker.xlsx` → sheet **Games** (State, Team, Opponent, Game Date, Game URL,
Status, Last Attempt, Retry Count, Log Message, + Data Class, Next Retry After,
IDs…; autofilter on, header frozen, status colour-coded, problems sorted first:
FAILED → RETRY → NO_DATA → PENDING → SUCCESS) and sheet **By Team**
(counts per team + "Needs Attention").

## Statuses

| Status | Meaning |
|---|---|
| `PENDING` | identified from the schedule, not attempted yet (also: scheduled in the future) |
| `SUCCESS` | player stats retrieved (`data_class` says whether both teams or one side entered them) |
| `NO_DATA` | page reachable (200/404) but no stats published — genuinely unavailable *so far* |
| `FAILED` | technical problem: HTTP 5xx/429, timeout, parse error |
| `RETRY` | explicitly re-queued: a SUCCESS game whose date/URL changed on the schedule, or `--recheck-*` |

`retry_count` = scraping attempts made so far.
A geo-block / network outage is `FAILED` but **does not use up the retry budget**
(`infra`), and 25 in a row abort the run so a dropped VPN can't burn through
every game's retries.

## Date logic

* Default window = **yesterday** (US Central, so Oct 12 → Oct 11).
* `--date 2026-10-11` or `--date-from/--date-to` for a specific day/range.
* Default runs also pick up games first seen up to `--catch-up-days 3` earlier
  (a skipped/failed earlier run doesn't lose a day). Not applied with explicit dates.
* Every run fetches each team's schedule (1 request/team — the only way to see
  new games) but fetches a **game page only if the game is due**:
  new games in the window, plus non-successful games whose back-off elapsed.
  Historical SUCCESS games cost zero requests.
* `--inventory-all` records every scheduled game as PENDING (first-time seeding).

## Retries (all configurable)

| Flag | Default |
|---|---|
| `--max-attempts` | 6 per cycle, then `retry_exhausted` ("gave up after N attempts") |
| `--nodata-backoff-hours` | 12 → doubles (24, 48, 72 cap) |
| `--failed-backoff-hours` | 1 → doubles (cap 24) |
| `--retry-window-days` | 14 after the game date, then stop |
| `--force-exhausted` | retry games that gave up |
| `--recheck-success` / `--recheck-partial` / `--recheck-contest ID` | deliberately re-scrape SUCCESS games |

Explicit `--date` runs ignore the back-off timer (you asked for that day).
A failed *re*check never downgrades a game that already has data.
Schedule changes are handled: moved date/URL → game re-queued (`RETRY`/budget
reset); game vanished from a successfully-read schedule → `on_schedule=false`,
no more requests; stale duplicate contest ids for the same date+opponent are
flagged `duplicate_of` (same rule as the old gap finder).

## No duplicates

`{state}_box_scores_{sport}_{season}.json` keeps the production shape
(`meta` + `games`). Key = `(team_id, contest_id)`:
new → **appended**; same content → **untouched**; changed content → **replaced in
place**; duplicates already in the file are dropped on load. Order on every
save: dataset first, gap file second, both atomic (temp file + `os.replace`),
with a checkpoint every 200 attempts — an interrupted run just leaves the
unfinished games due, and re-processing them is a no-op for records already
stored. An unreadable file is set aside as `*.corrupt-<ts>`, never overwritten.

## Run it

```bash
# the daily job: yesterday's games + retry backlog, both sports
python daily_gapfinder.py --state TX,OK,AR --sport both

# a specific day / range
python daily_gapfinder.py --state TX --sport boys --date 2026-10-11
python daily_gapfinder.py --state TX --sport girls --date-from 2026-10-01 --date-to 2026-10-11

# retries only (no schedule fetch), preview, discovery only
python daily_gapfinder.py --state TX --sport boys --skip-schedule
python daily_gapfinder.py --state TX --sport boys --dry-run
python daily_gapfinder.py --state TX --sport boys --no-scrape
```
Exit code 2 = run aborted (all schedules failed / repeated geo-blocks).
Master list: `Input Master File/{sport}_basketball_all_states_26-27.json`, else this
folder, else the repo root (`--master` to override). Legacy modules are looked up in the parent
folder (`MAXPREPS_CORE_DIR` to override).

## Test it

```bash
python -m unittest test_offline -v        # 22 offline tests, no network
# live smoke test (VPN on) — past season with real games, 6 teams:
python daily_gapfinder.py --state TX --sport boys --season 2025-2026 \
  --date-from 2026-02-01 --date-to 2026-02-14 --limit-teams 6 --output-dir _smoke
python daily_gapfinder.py ...same command...   # rerun: SUCCESS rows untouched, only NO_DATA re-checked, 0 duplicate records
```

## Dashboard (frontend)

```bash
streamlit run dashboard.py
```
Read-only view of the gap files in `output/` (change the folder in the sidebar):

* **Overview** — status counts, % success per state/sport, games per date
* **To scrape now** — exactly the games the next run would attempt, with the reason (new / retry / recheck); upcoming games listed separately
* **Teams to re-scrape** — teams with retryable NO_DATA/FAILED/RETRY games (how many are due now, oldest game, next retry time, last log) plus teams whose schedule fetch failed; pick a team to see its games
* **All games** — filter by status/state/sport/text, download CSV
* **Gave up** — games no longer retried (attempts used, window passed, duplicate, off schedule)
* **Run** — builds the `daily_gapfinder.py` command and starts it in the background (dry run ticked by default), with a live log

The sidebar's date mode (yesterday / one date / range), max attempts, retry
window and "ignore back-off" mirror the CLI flags, so "due" matches what a run
with the same settings would do. It never writes a gap file itself.

## Not in this phase
Stats-tab, accumulation (stage 4) and `APP/pipeline.py`/GitHub-workflow wiring.
The dataset file and the legacy buckets are shaped so those can consume this
output unchanged later.

## `core/` - bundled legacy modules
The workflow reuses the old pipeline's proven code. Unchanged copies live in `core/` so this repo runs on its own:
`scrape_box_scores.py` (HTTP transport + box-score parser), `app.py`, `scrape_team_stats.py`,
`accumulate_from_stats_tab.py`, `Accumulation_data.py`, `merge_all_stats_tab.py`,
`fix_total_games_checked.py`, plus the two 26-27 master lists (used for opponent/team-name lookup).
`wf_core.CORE_DIR` uses `./core` when present (override with `MAXPREPS_CORE_DIR`).
Not modified here - if the old repo's copies change, re-copy them.

## Final accumulated files (`accumulate_stats.py`)
`Final_{st}_accumulated_{sport}_26_27.json` = per-game accumulation from box scores, with the stats tab
replacing a team's records wherever it exists (GP=0 guard kept). Each team_total also carries
`total_games_scheduled` (games the gap finder found on the team's schedule), right after `TotalGamesChecked`.
