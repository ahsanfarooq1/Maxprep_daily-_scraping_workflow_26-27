"""
gap_store.py — the Gap Finder file: game inventory + scraping-status tracker.

One file per state/sport/season/level, named exactly like the existing
pipeline's ({state}_data_gaps_{sport}[_jv]_{season}.json). It holds

    meta     run totals + status counts (legacy keys kept)
    teams    roster of teams + the outcome of their last schedule fetch
    games    ONE ROW PER (team, game) — the source of truth for this workflow
    teamsFullBoxScores / teamsPartialBoxScores / teamsNoBoxScores / errors
             the legacy per-team buckets, regenerated from `games` on every
             save so stages that still read the old shape keep working

A game row's identity is  game_key = "{team_id}|{contest_id}"  (falling back
to the normalised game URL when a contest id is missing). The same key is what
dataset_store uses, so re-processing a game can never duplicate it.

Status model (see wf_core): PENDING / SUCCESS / NO_DATA / FAILED / RETRY.
`retry_count` is the number of scraping attempts made so far.
"""

import os
import re
import json
import hashlib
import datetime as dt
from dataclasses import dataclass, field

from wf_core import (PENDING, SUCCESS, NO_DATA, FAILED, RETRY, STATUSES,
                     CLS_FULL, CLS_TEAM, CLS_OPP, CLS_NONE,
                     now_str, parse_ts, iso_to_mdy, log)

SCHEMA_VERSION = 2
LOG_HISTORY_MAX = 10

# Column order for Excel/CSV (label, row field).
EXCEL_COLUMNS = [
    ("State", "state"), ("Team", "team_name"), ("Opponent", "opponent_team_name"),
    ("Game Date", "game_date"), ("Game URL", "game_url"), ("Status", "status"),
    ("Last Attempt", "last_attempt"), ("Retry Count", "retry_count"),
    ("Log Message", "log_message"),
    ("Data Retrieved", "data_retrieved"), ("Data Class", "data_class"),
    ("Next Retry After", "next_retry_after"), ("Retries Exhausted", "retry_exhausted"),
    ("On Schedule", "on_schedule"), ("Level", "level"), ("Sport", "sport"),
    ("Region", "region"), ("Team ID", "team_id"), ("Opponent ID", "opponent_team_id"),
    ("Contest ID", "contest_id"), ("First Seen", "first_seen"),
    ("Last Success", "last_success"),
]


# ─── Policy (all retry knobs in one place) ───────────────────────────────────
@dataclass
class Policy:
    max_attempts: int = 6              # attempts per retry cycle before giving up
    nodata_backoff_h: float = 12.0     # NO_DATA: wait 12h, 24h, 48h, ... (doubling)
    nodata_cap_h: float = 72.0
    failed_backoff_h: float = 1.0      # FAILED: wait 1h, 2h, 4h, ... (doubling)
    failed_cap_h: float = 24.0
    retry_window_days: int = 14        # stop retrying a game this long after its date
    ignore_backoff: bool = False       # explicit --date/range runs set this
    force_exhausted: bool = False      # retry games that already gave up
    recheck_success: bool = False      # re-scrape SUCCESS rows inside the window
    recheck_partial: bool = False      # re-scrape SUCCESS rows where one side has no stats
    recheck_contests: set = field(default_factory=set)   # contest ids to recheck


@dataclass
class Outcome:
    """Result of one scraping attempt (built by game_checker)."""
    status: str                  # SUCCESS | NO_DATA | FAILED
    message: str = ""
    data_class: str = ""         # full | team_only | opp_only | no_data
    record: dict = None          # box-score record when SUCCESS
    record_hash: str = ""
    fingerprint: str = ""
    http_status: int = None
    final_url: str = ""
    opponent_team_id: str = ""
    opponent_team_name: str = ""
    infra: bool = False          # geo-block / network down: not the game's fault


# ─── Identity ────────────────────────────────────────────────────────────────
def norm_url(url):
    """Stable form of a game URL for keys: lower-case, no fragment, no tab/utm
    params — but keeps the `c=` contest id, which is what identifies the game."""
    if not url:
        return ""
    base, _, query = url.split("#")[0].partition("?")
    keep = [p for p in query.split("&") if p.lower().startswith("c=")]
    return base.rstrip("/").lower() + ("?" + "&".join(keep).lower() if keep else "")


def contest_id_from_url(url):
    m = re.search(r"[?&]c=([A-Za-z0-9_-]+)", url or "")
    return m.group(1).lower() if m else ""


def game_key(team_id, contest_id, game_url=""):
    ident = (contest_id or "").lower() or ("url:" + norm_url(game_url))
    return f"{team_id}|{ident}"


def new_row(**f):
    """A fresh game row in PENDING state with every column present."""
    row = {
        "game_key": "", "state": "", "sport": "", "season": "", "level": "varsity",
        "team_id": "", "team_name": "", "team_url": "", "region": "",
        "opponent_team_id": "", "opponent_team_name": "",
        "game_date": "", "game_url": "", "contest_id": "", "ssid": "",
        "scheduled_time": "", "game_type": "",
        "status": PENDING, "data_retrieved": False, "data_class": "",
        "first_seen": now_str(), "last_attempt": "", "last_success": "",
        "retry_count": 0, "cycle_attempts": 0, "next_retry_after": "",
        "retry_exhausted": False, "on_schedule": True, "duplicate_of": "",
        "http_status": None, "log_message": "Identified from schedule; not scraped yet",
        "log_history": [], "stats_fingerprint": "", "record_hash": "",
    }
    row.update(f)
    if not row["game_key"]:
        row["game_key"] = game_key(row["team_id"], row["contest_id"], row["game_url"])
    return row


def _push_log(row, ts, msg):
    row["log_message"] = msg
    hist = row.setdefault("log_history", [])
    hist.append(f"{ts} | {row['status']} | {msg}")
    del hist[:-LOG_HISTORY_MAX]


# ─── Scheduling: which games need an attempt this run? ───────────────────────
def _d(row):
    try:
        return dt.date.fromisoformat(row.get("game_date") or "")
    except ValueError:
        return None


def due_reason(row, now, as_of, window, policy):
    """Why `row` should be attempted this run, or None.

    window = (from_date, to_date) the user asked for (default: yesterday).
    Anything non-successful and still inside its retry window is also picked
    up as a backlog item, so yesterday's NO_DATA game keeps being re-checked
    without anyone having to widen the date range.
    """
    if not row.get("on_schedule", True) or row.get("duplicate_of"):
        return None
    status, d = row["status"], _d(row)
    in_window = bool(d and window[0] <= d <= window[1])

    if status == SUCCESS:
        if row["contest_id"] in policy.recheck_contests:
            return "recheck-contest"
        if policy.recheck_success and in_window:
            return "recheck-success"
        if (policy.recheck_partial and row.get("data_class") in (CLS_TEAM, CLS_OPP)
                and d and (in_window or (as_of - d).days <= policy.retry_window_days)):
            return "recheck-partial"
        return None

    if d and d > as_of:
        return None                       # scheduled, not played yet
    if d and not in_window and (as_of - d).days > policy.retry_window_days:
        return None                       # past the retry window (see apply_expiry)
    if row.get("retry_exhausted") and not policy.force_exhausted:
        return None
    if not policy.ignore_backoff:
        nra = parse_ts(row.get("next_retry_after"))
        if nra and now < nra:
            return None
    if status == PENDING:
        return "new" if in_window or not row["retry_count"] else "pending"
    return "retry"                        # NO_DATA / FAILED / RETRY


def select_due(rows, now, as_of, window, policy):
    out = []
    for r in rows:
        why = due_reason(r, now, as_of, window, policy)
        if why:
            out.append((r, why))
    # Oldest games first so a --limit-games cap drains the backlog in order.
    out.sort(key=lambda rw: (rw[0].get("game_date") or "9999", rw[0]["team_id"]))
    return out


def apply_expiry(rows, as_of, policy, now=None, window=None):
    """Mark non-successful games older than the retry window as exhausted so
    permanently unavailable games stop generating requests. Games inside the
    requested date window are never expired (a deliberate backfill of an old
    date must work). Returns count."""
    n = 0
    for r in rows:
        d = _d(r)
        if window and d and window[0] <= d <= window[1]:
            continue
        if (r["status"] in (NO_DATA, FAILED, RETRY, PENDING) and d
                and (as_of - d).days > policy.retry_window_days
                and not r.get("retry_exhausted")):
            r["retry_exhausted"] = True
            _push_log(r, now_str(now), f"Gave up: retry window of {policy.retry_window_days} "
                                       f"days after the game date has passed "
                                       f"(last: {r.get('log_message', '')})")
            n += 1
    return n


def _backoff(status, attempts, policy):
    base, cap = ((policy.failed_backoff_h, policy.failed_cap_h) if status == FAILED
                 else (policy.nodata_backoff_h, policy.nodata_cap_h))
    return min(base * (2 ** max(attempts - 1, 0)), cap)


def record_attempt(row, out, policy, now=None):
    """Fold one attempt's Outcome into the row (status, counters, timestamps,
    log). Never downgrades a row that already holds good data."""
    now = now or dt.datetime.now()
    ts = now_str(now)
    row["last_attempt"] = ts
    row["http_status"] = out.http_status
    row["retry_count"] += 1
    row["cycle_attempts"] += 1
    had_data = bool(row.get("data_retrieved"))

    if out.opponent_team_id and not row.get("opponent_team_id"):
        row["opponent_team_id"] = out.opponent_team_id
    if out.opponent_team_name and not row.get("opponent_team_name"):
        row["opponent_team_name"] = out.opponent_team_name

    if out.status == SUCCESS:
        row.update(status=SUCCESS, data_retrieved=True, data_class=out.data_class,
                   last_success=ts, next_retry_after="", retry_exhausted=False,
                   stats_fingerprint=out.fingerprint, record_hash=out.record_hash,
                   cycle_attempts=0)
        _push_log(row, ts, out.message or "Data scraped successfully")
        return

    if out.infra:
        # Geo-block / network down says nothing about the game: don't burn its
        # retry budget (undo the increment), just try again soon.
        row["retry_count"] -= 1
        row["cycle_attempts"] -= 1
        if not had_data:
            row["status"] = FAILED
        row["next_retry_after"] = now_str(now + dt.timedelta(hours=policy.failed_backoff_h))
        _push_log(row, ts, out.message)
        return

    exhausted = row["cycle_attempts"] >= policy.max_attempts
    prior = row["status"]
    if had_data:
        # A recheck that found nothing / failed must not erase good data.
        row["status"] = SUCCESS if (prior == SUCCESS or exhausted) else RETRY
        msg = f"Recheck {out.status}: {out.message}"
    else:
        row["status"] = out.status
        msg = out.message
    if exhausted:
        row["retry_exhausted"] = True
        row["next_retry_after"] = ""
        msg += f" | gave up after {row['cycle_attempts']} attempts"
    else:
        wait = _backoff(out.status, row["cycle_attempts"], policy)
        row["next_retry_after"] = now_str(now + dt.timedelta(hours=wait))
    _push_log(row, ts, msg)


def queue_retry(row, reason, now=None):
    """Put a game back in the queue (status RETRY) with a fresh retry budget."""
    row["status"] = RETRY
    row["retry_exhausted"] = False
    row["cycle_attempts"] = 0
    row["next_retry_after"] = ""
    _push_log(row, now_str(now), reason)


def mark_duplicates(rows):
    """Collapse MaxPreps' stale duplicate contest ids (same team, date and
    opponent): when one of them has data, the others are placeholders of the
    same game, so they stop being retried — the same rule the existing gap
    finder applies. Doubleheaders with data on both sides are untouched."""
    groups = {}
    for r in rows:
        if r.get("game_date") and r.get("opponent_team_id"):
            groups.setdefault((r["team_id"], r["game_date"], r["opponent_team_id"]), []).append(r)
    n = 0
    for grp in groups.values():
        if len(grp) < 2:
            continue
        winner = next((r for r in grp if r["status"] == SUCCESS), None)
        if not winner:
            continue
        for r in grp:
            if r is not winner and r["status"] != SUCCESS and not r.get("duplicate_of"):
                r["duplicate_of"] = winner["contest_id"]
                _push_log(r, now_str(), f"Duplicate of contest {winner['contest_id']} "
                                        f"(same date+opponent already has data)")
                n += 1
    return n


# ─── The file ────────────────────────────────────────────────────────────────
class GapFile:
    def __init__(self, path, state_code="", state_name="", sport="boys",
                 season="2026-2027", level="varsity"):
        self.path = path
        self.state_code, self.state_name = state_code, state_name
        self.sport, self.season, self.level = sport, season, level
        self.games = {}        # game_key -> row
        self.teams = {}        # team_url -> roster entry
        self.run_log = []      # last few run summaries
        self.created = now_str()

    # ---- load / save ----
    @classmethod
    def load(cls, path, **kw):
        gf = cls(path, **kw)
        if not os.path.exists(path):
            return gf
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            # Never overwrite something we couldn't read: set it aside.
            bad = f"{path}.corrupt-{dt.datetime.now():%Y%m%d-%H%M%S}"
            os.replace(path, bad)
            log(f"[WARN] {os.path.basename(path)} unreadable ({e}); moved to {bad}, starting fresh")
            return gf
        meta = data.get("meta", {})
        gf.created = meta.get("created", gf.created)
        gf.run_log = meta.get("run_log", [])
        for t in data.get("teams", []):
            gf.teams[t["teamUrl"]] = t
        if "games" in data:
            for r in data["games"]:
                base = new_row()
                base.update(r)
                gf.games[base["game_key"]] = base
        else:
            gf._migrate_legacy(data)
        return gf

    def _migrate_legacy(self, data):
        """Adopt a pre-v2 gap file (team buckets only) as game rows so history
        isn't thrown away: games with stats -> SUCCESS, the rest -> NO_DATA."""
        n = 0
        for bucket in ("teamsFullBoxScores", "teamsPartialBoxScores", "teamsNoBoxScores"):
            for t in data.get(bucket, []):
                turl = t.get("teamUrl", "")
                tid = re.sub(r"https?://(?:www\.)?maxpreps\.com/", "", turl).rstrip("/")
                self.teams.setdefault(turl, {"teamName": t.get("teamName", ""), "teamUrl": turl,
                                             "region": t.get("region", ""), "schedule_status": "OK",
                                             "schedule_last_attempt": "", "schedule_error": ""})
                for key, st, cls in (("fullDataGames", SUCCESS, CLS_FULL),
                                     ("teamOnlyDataGames", SUCCESS, CLS_TEAM),
                                     ("opponentOnlyDataGames", SUCCESS, CLS_OPP),
                                     ("noDataGames", NO_DATA, CLS_NONE)):
                    for g in t.get(key, {}).get("games", []):
                        mdy = g.get("date", "")
                        m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", mdy)
                        iso = f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}" if m else ""
                        r = new_row(state=self.state_code, sport=self.sport, season=self.season,
                                    level=self.level, team_id=tid, team_name=t.get("teamName", ""),
                                    team_url=turl, region=t.get("region", ""),
                                    opponent_team_id=g.get("opponent_team_id", ""),
                                    opponent_team_name=g.get("opponent_team_name", ""),
                                    game_date=iso, game_url=g.get("url", ""),
                                    contest_id=contest_id_from_url(g.get("url", "")),
                                    status=st, data_class=cls, data_retrieved=(st == SUCCESS),
                                    stats_fingerprint=g.get("stats_fingerprint") or "",
                                    last_attempt=(data.get("meta", {}).get("last_updated", "")),
                                    retry_count=1, cycle_attempts=1,
                                    log_message="Migrated from legacy gap file")
                        if st == NO_DATA:   # unresolved -> make it eligible again, but bounded
                            r["next_retry_after"] = ""
                        self.games.setdefault(r["game_key"], r)
                        n += 1
        if n:
            log(f"Migrated {n} games from the legacy-format gap file")

    def save(self, export_excel=True):
        rows = sorted(self.games.values(), key=lambda r: (r["team_name"], r["game_date"], r["contest_id"]))
        buckets = self._legacy_buckets(rows)
        counts = self.status_counts()
        errors = [t for t in self.teams.values() if t.get("schedule_status") == "ERROR"]
        meta = {
            "schema_version": SCHEMA_VERSION,
            "state": self.state_name, "stateCode": self.state_code,
            "sport": f"{self.sport.title()} Basketball", "season": self.season, "level": self.level,
            "created": self.created, "last_updated": now_str(),
            "totalTeams": len(self.teams),
            "processedTeamsCount": sum(1 for t in self.teams.values() if t.get("schedule_status") == "OK"),
            "processedTeams": [re.sub(r"https?://(?:www\.)?maxpreps\.com/", "", u).rstrip("/")
                               for u, t in self.teams.items() if t.get("schedule_status") == "OK"],
            "totalGames": len(rows), "totalGamesChecked": sum(
                b["gamesChecked"] for k in ("full", "partial", "none") for b in buckets[k]),
            "statusCounts": counts,
            "teamsFullBoxScores": len(buckets["full"]), "teamsPartialBoxScores": len(buckets["partial"]),
            "teamsNoBoxScores": len(buckets["none"]), "errors_count": len(errors),
            "run_log": self.run_log[-30:],
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write('{\n"meta": ' + json.dumps(meta, indent=2, ensure_ascii=False))
            f.write(',\n"teams": ' + _json_lines(sorted(self.teams.values(), key=lambda t: t["teamName"])))
            f.write(',\n"games": ' + _json_lines(rows))
            f.write(',\n"teamsFullBoxScores": ' + _json_lines(buckets["full"]))
            f.write(',\n"teamsPartialBoxScores": ' + _json_lines(buckets["partial"]))
            f.write(',\n"teamsNoBoxScores": ' + _json_lines(buckets["none"]))
            f.write(',\n"errors": ' + _json_lines([
                {"teamName": t["teamName"], "teamUrl": t["teamUrl"], "region": t.get("region", ""),
                 "stage": "schedule", "error": t.get("schedule_error", "")} for t in errors]))
            f.write("\n}\n")
        os.replace(tmp, self.path)          # atomic: an interrupted save can't corrupt the file
        if export_excel:
            try:
                self.export_tracker(rows)
            except Exception as e:           # the JSON is the source of truth; never fail a run on Excel
                log(f"[WARN] tracker export failed: {e}")

    # ---- views ----
    def status_counts(self):
        c = {s: 0 for s in STATUSES}
        for r in self.games.values():
            c[r["status"]] = c.get(r["status"], 0) + 1
        return c

    def _legacy_buckets(self, rows):
        """Per-team entries in the exact shape app.py writes, so downstream
        stages (stats-tab, box-score scraper) can read this file unchanged."""
        by_team = {}
        for r in rows:
            by_team.setdefault(r["team_url"], []).append(r)
        full, partial, none = [], [], []
        for turl, t in self.teams.items():
            if t.get("schedule_status") == "ERROR" and turl not in by_team:
                continue                                 # reported under "errors"
            lists = {CLS_FULL: [], CLS_TEAM: [], CLS_OPP: [], CLS_NONE: []}
            for r in by_team.get(turl, []):
                if r["status"] == PENDING or not r.get("on_schedule", True) or r.get("duplicate_of"):
                    continue
                cls = r["data_class"] if r["status"] == SUCCESS or r["data_retrieved"] else CLS_NONE
                lists[cls if cls in lists else CLS_NONE].append({
                    "date": iso_to_mdy(r["game_date"]), "opponent_team_id": r["opponent_team_id"],
                    "opponent_team_name": r["opponent_team_name"], "url": r["game_url"],
                    "stats_fingerprint": r.get("stats_fingerprint") or None,
                    "status": r["status"]})
            with_stats = len(lists[CLS_FULL]) + len(lists[CLS_TEAM]) + len(lists[CLS_OPP])
            missing = len(lists[CLS_NONE])
            entry = {"teamName": t["teamName"], "teamUrl": turl, "region": t.get("region", ""),
                     "gamesChecked": with_stats + missing, "gamesWithStats": with_stats,
                     "gamesMissing": missing,
                     "recordGamesPlayed": t.get("recordGamesPlayed"),
                     "fullDataGames": {"count": len(lists[CLS_FULL]), "note": "Both teams entered stats.", "games": lists[CLS_FULL]},
                     "teamOnlyDataGames": {"count": len(lists[CLS_TEAM]), "note": "Only THIS team entered stats; opponent did not.", "games": lists[CLS_TEAM]},
                     "opponentOnlyDataGames": {"count": len(lists[CLS_OPP]), "note": "Only the OPPONENT entered stats; this team did not.", "games": lists[CLS_OPP]},
                     "noDataGames": {"count": missing, "note": "NEITHER team entered any stats (or not scraped successfully yet).", "games": lists[CLS_NONE]}}
            if entry["gamesChecked"] == 0:
                none.append(entry)
            elif missing == 0:
                full.append(entry)
            elif with_stats > 0:
                partial.append(entry)
            else:
                none.append(entry)
        return {"full": full, "partial": partial, "none": none}

    # ---- discovery ----
    def upsert_discovered(self, ref, now=None):
        """Insert a game found on a team's schedule, or reconcile it with the
        row we already hold. Returns 'new' | 'updated' | 'unchanged'."""
        key = ref["game_key"]
        row = self.games.get(key)
        if row is None:
            self.games[key] = new_row(**ref)
            return "new"
        changed = []
        for f in ("game_date", "game_url", "opponent_team_id", "opponent_team_name",
                  "ssid", "scheduled_time", "game_type"):
            if ref.get(f) and ref[f] != row.get(f):
                if f in ("game_date", "game_url") and row.get(f):
                    changed.append(f"{f}: {row[f]} -> {ref[f]}")
                row[f] = ref[f]
        row["on_schedule"] = True
        if changed:
            msg = "Schedule changed (" + "; ".join(changed) + ")"
            if row["status"] == SUCCESS:
                queue_retry(row, msg + " - recheck queued", now)
            else:
                row["retry_exhausted"], row["cycle_attempts"], row["next_retry_after"] = False, 0, ""
                _push_log(row, now_str(now), msg + " - retry budget reset")
            return "updated"
        return "unchanged"

    # ---- Excel / CSV tracker ----
    def tracker_path(self):
        return os.path.splitext(self.path)[0] + "_tracker.xlsx"

    def export_tracker(self, rows=None):
        order = {FAILED: 0, RETRY: 1, NO_DATA: 2, PENDING: 3, SUCCESS: 4}
        rows = sorted(rows if rows is not None else self.games.values(),
                      key=lambda r: (order.get(r["status"], 9), r["team_name"], r["game_date"]))
        try:
            import openpyxl
            from openpyxl.styles import PatternFill, Font, Alignment
            from openpyxl.utils import get_column_letter
        except ImportError:
            import csv
            p = os.path.splitext(self.path)[0] + "_tracker.csv"
            with open(p, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow([c[0] for c in EXCEL_COLUMNS])
                for r in rows:
                    w.writerow([_cell(r, k) for _, k in EXCEL_COLUMNS])
            return p

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Games"
        ws.append([c[0] for c in EXCEL_COLUMNS])
        fills = {SUCCESS: "C6EFCE", NO_DATA: "FFEB9C", FAILED: "FFC7CE", RETRY: "FCE4D6", PENDING: "DDEBF7"}
        sc = [k for _, k in EXCEL_COLUMNS].index("status") + 1
        for r in rows:
            ws.append([_cell(r, k) for _, k in EXCEL_COLUMNS])
            ws.cell(ws.max_row, sc).fill = PatternFill("solid", fgColor=fills.get(r["status"], "FFFFFF"))
        for i, (label, key) in enumerate(EXCEL_COLUMNS, 1):
            ws.cell(1, i).font = Font(bold=True, color="FFFFFF")
            ws.cell(1, i).fill = PatternFill("solid", fgColor="305496")
            width = {"game_url": 60, "log_message": 60, "team_name": 28, "opponent_team_name": 28,
                     "team_id": 38, "opponent_team_id": 38, "contest_id": 38}.get(key, 18)
            ws.column_dimensions[get_column_letter(i)].width = width
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Team-level view: which teams still need attention.
        ts = wb.create_sheet("By Team")
        ts.append(["Team", "Games", "SUCCESS", "PENDING", "NO_DATA", "FAILED", "RETRY",
                   "Needs Attention", "Schedule Fetch", "Schedule Error"])
        agg = {}
        for r in rows:
            a = agg.setdefault(r["team_url"], {s: 0 for s in STATUSES})
            a[r["status"]] += 1
        for turl, t in sorted(self.teams.items(), key=lambda kv: kv[1]["teamName"]):
            a = agg.get(turl, {s: 0 for s in STATUSES})
            ts.append([t["teamName"], sum(a.values()), a[SUCCESS], a[PENDING], a[NO_DATA],
                       a[FAILED], a[RETRY], a[NO_DATA] + a[FAILED] + a[RETRY],
                       t.get("schedule_status", ""), t.get("schedule_error", "")])
        for c in ts[1]:
            c.font = Font(bold=True)
        ts.freeze_panes = "A2"
        ts.auto_filter.ref = ts.dimensions
        ts.column_dimensions["A"].width = 34
        ts.column_dimensions["J"].width = 50

        tmp = self.tracker_path() + ".tmp"
        wb.save(tmp)
        os.replace(tmp, self.tracker_path())
        return self.tracker_path()


def _cell(r, key):
    v = r.get(key, "")
    if isinstance(v, bool):
        return "Y" if v else "N"
    return "" if v is None else v


def _json_lines(items):
    """JSON array with one compact object per line (readable, diff-friendly,
    and ~3x smaller than indent=2 for 50k-row game lists)."""
    if not items:
        return "[]"
    return "[\n" + ",\n".join(json.dumps(i, ensure_ascii=False, separators=(",", ":")) for i in items) + "\n]"
