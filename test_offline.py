"""
Offline tests for the date-driven Gap Finder workflow (no network needed).

    python -m unittest test_offline -v        (from this folder)
"""
import os
import sys
import json
import shutil
import tempfile
import unittest
import datetime as dt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import daily_gapfinder as dg
from wf_core import SUCCESS, NO_DATA, FAILED, PENDING, RETRY, resolve_window, date_from_url, date_from_value
from gap_store import GapFile, Policy, Outcome, new_row, select_due, record_attempt, due_reason, apply_expiry, mark_duplicates
from dataset_store import BoxScoreDataset

TODAY = dt.date(2026, 10, 12)
NOW = dt.datetime(2026, 10, 12, 6, 0, 0)
YESTERDAY = (TODAY - dt.timedelta(days=1)).isoformat()     # 2026-10-11
WINDOW = resolve_window(as_of=TODAY)
TEAM = "tx/austin/austin-maroons/basketball"


def ref(cid, date=YESTERDAY, opp="tx/dallas/dallas-bears/basketball", **kw):
    url = f"https://www.maxpreps.com/tx/basketball/game/austin-vs-dallas/{date[5:7]}-{date[8:]}-{date[:4]}/?c={cid}"
    r = dict(team_id=TEAM, team_name="Austin Maroons", team_url=f"https://www.maxpreps.com/{TEAM}/",
             state="TX", sport="boys", season="2026-2027", opponent_team_id=opp,
             opponent_team_name="Dallas Bears", game_date=date, game_url=url, contest_id=cid, ssid="ssid-1")
    r.update(kw)
    return new_row(**r)


def record(cid, pts=10, team_id=TEAM):
    p = lambda n: {"team": {"players": [{"player_name": "A", "points": n}]}, "opponent": {"players": []}}
    return {"contest_id": cid, "game_url": f"https://x/{cid}", "game_date": "10-11-2026", "is_deleted": False,
            "team": {"team_id": team_id, "team_name": "Austin Maroons"},
            "opponent": {"team_id": "tx/dallas/dallas-bears/basketball", "team_name": "Dallas Bears"},
            "shooting": p(pts), "detailed_shooting": p(pts), "totals": p(pts), "misc": p(pts)}


class Dates(unittest.TestCase):
    def test_default_is_yesterday(self):
        self.assertEqual(WINDOW, (dt.date(2026, 10, 11), dt.date(2026, 10, 11)))

    def test_explicit(self):
        self.assertEqual(resolve_window(date="2026-09-01"), (dt.date(2026, 9, 1),) * 2)
        self.assertEqual(resolve_window(date_from="2026-10-01", date_to="2026-10-03")[1], dt.date(2026, 10, 3))
        with self.assertRaises(ValueError):
            resolve_window(date_from="2026-10-05", date_to="2026-10-01")

    def test_parsing(self):
        self.assertEqual(date_from_url("https://x/tx/basketball/game/a-vs-b/11-15-2025/?c=1"), "2025-11-15")
        self.assertEqual(date_from_url("https://x/nodate/"), "")
        self.assertEqual(date_from_value("2025-11-15T14:00:00"), "2025-11-15")
        self.assertEqual(date_from_value(None), "")


class StateMachine(unittest.TestCase):
    def setUp(self):
        self.p = Policy()

    def test_new_game_in_window_is_due_future_is_not(self):
        self.assertEqual(due_reason(ref("a"), NOW, TODAY, WINDOW, self.p), "new")
        self.assertIsNone(due_reason(ref("b", date="2026-10-20"), NOW, TODAY, WINDOW, self.p))

    def test_success_not_rescraped(self):
        r = ref("a")
        record_attempt(r, Outcome(SUCCESS, data_class="full"), self.p, NOW)
        self.assertEqual((r["status"], r["retry_count"], r["data_retrieved"]), (SUCCESS, 1, True))
        self.assertIsNone(due_reason(r, NOW, TODAY, WINDOW, self.p))
        self.assertEqual(due_reason(r, NOW, TODAY, WINDOW, Policy(recheck_success=True)), "recheck-success")
        self.assertEqual(due_reason(r, NOW, TODAY, WINDOW, Policy(recheck_contests={"a"})), "recheck-contest")

    def test_no_data_backs_off_then_retries(self):
        r = ref("a")
        record_attempt(r, Outcome(NO_DATA, "Statistics not available yet"), self.p, NOW)
        self.assertEqual((r["status"], r["retry_count"]), (NO_DATA, 1))
        self.assertIsNone(due_reason(r, NOW + dt.timedelta(hours=1), TODAY, WINDOW, self.p))     # inside back-off
        self.assertEqual(due_reason(r, NOW + dt.timedelta(hours=13), TODAY, WINDOW, self.p), "retry")

    def test_data_arrives_on_retry(self):
        r = ref("a")
        record_attempt(r, Outcome(NO_DATA, "x"), self.p, NOW)
        record_attempt(r, Outcome(SUCCESS, data_class="team_only"), self.p, NOW + dt.timedelta(days=1))
        self.assertEqual((r["status"], r["retry_count"], r["retry_exhausted"], r["next_retry_after"]),
                         (SUCCESS, 2, False, ""))

    def test_gives_up_after_max_attempts(self):
        p = Policy(max_attempts=3)
        r = ref("a")
        t = NOW
        for _ in range(3):
            record_attempt(r, Outcome(NO_DATA, "x"), p, t)
            t += dt.timedelta(days=5)
        self.assertTrue(r["retry_exhausted"])
        self.assertIn("gave up", r["log_message"])
        self.assertIsNone(due_reason(r, t, TODAY, WINDOW, p))
        self.assertEqual(due_reason(r, t, TODAY, WINDOW, Policy(max_attempts=3, force_exhausted=True)), "retry")

    def test_infra_failure_does_not_burn_budget(self):
        r = ref("a")
        record_attempt(r, Outcome(FAILED, "geo-block", infra=True), self.p, NOW)
        self.assertEqual((r["status"], r["retry_count"], r["cycle_attempts"]), (FAILED, 0, 0))

    def test_failed_vs_no_data_backoff(self):
        a, b = ref("a"), ref("b")
        record_attempt(a, Outcome(FAILED, "HTTP 500"), self.p, NOW)
        record_attempt(b, Outcome(NO_DATA, "none"), self.p, NOW)
        later = NOW + dt.timedelta(hours=2)
        self.assertEqual(due_reason(a, later, TODAY, WINDOW, self.p), "retry")   # 1h back-off elapsed
        self.assertIsNone(due_reason(b, later, TODAY, WINDOW, self.p))           # 12h back-off not yet

    def test_recheck_failure_keeps_good_data(self):
        r = ref("a")
        record_attempt(r, Outcome(SUCCESS, data_class="full"), self.p, NOW)
        record_attempt(r, Outcome(FAILED, "HTTP 500"), self.p, NOW + dt.timedelta(days=1))
        self.assertEqual((r["status"], r["data_retrieved"]), (SUCCESS, True))
        self.assertTrue(r["log_message"].startswith("Recheck"))

    def test_retry_window_expiry(self):
        r = ref("old", date="2026-09-01")
        r["status"] = NO_DATA
        self.assertIsNone(due_reason(r, NOW, TODAY, WINDOW, self.p))
        self.assertEqual(apply_expiry([r], TODAY, self.p, NOW), 1)
        self.assertTrue(r["retry_exhausted"])

    def test_backfill_window_not_expired(self):
        r = ref("old", date="2026-09-01")
        w = (dt.date(2026, 9, 1), dt.date(2026, 9, 1))
        self.assertEqual(apply_expiry([r], TODAY, self.p, NOW, w), 0)
        self.assertEqual(due_reason(r, NOW, TODAY, w, self.p), "new")

    def test_explicit_date_bypasses_backoff(self):
        r = ref("a")
        record_attempt(r, Outcome(NO_DATA, "x"), self.p, NOW)
        p = Policy(ignore_backoff=True)
        self.assertEqual(due_reason(r, NOW, TODAY, WINDOW, p), "retry")

    def test_schedule_change_requeues_success(self):
        gf = GapFile(os.path.join(tempfile.gettempdir(), "x.json"))
        r0 = ref("a")
        gf.upsert_discovered(r0, NOW)
        row = gf.games[r0["game_key"]]
        record_attempt(row, Outcome(SUCCESS, data_class="full"), self.p, NOW)
        moved = ref("a", date="2026-10-13")
        self.assertEqual(gf.upsert_discovered(moved, NOW), "updated")
        self.assertEqual((row["status"], row["game_date"], row["data_retrieved"]), (RETRY, "2026-10-13", True))
        self.assertEqual(gf.upsert_discovered(moved, NOW), "unchanged")

    def test_duplicate_contest_placeholder(self):
        good, stale = ref("good"), ref("stale")
        record_attempt(good, Outcome(SUCCESS, data_class="full"), self.p, NOW)
        record_attempt(stale, Outcome(NO_DATA, "x"), self.p, NOW)
        self.assertEqual(mark_duplicates([good, stale]), 1)
        self.assertEqual(stale["duplicate_of"], "good")
        self.assertIsNone(due_reason(stale, NOW + dt.timedelta(days=3), TODAY, WINDOW, Policy(ignore_backoff=True)))


class Dataset(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.path = os.path.join(self.d, "box.json")

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_add_unchanged_update_never_duplicates(self):
        ds = BoxScoreDataset(self.path)
        self.assertEqual(ds.upsert(record("c1")), "added")
        self.assertEqual(ds.upsert(record("c1")), "unchanged")
        self.assertEqual(ds.upsert(record("c1", pts=99)), "updated")
        self.assertEqual(ds.upsert(record("c1", team_id="tx/other/team/basketball")), "added")  # other team's view
        self.assertEqual(len(ds), 2)
        ds.save(processed_teams={TEAM})
        again = BoxScoreDataset(self.path)
        self.assertEqual(len(again), 2)
        self.assertEqual(again.upsert(record("c1", pts=99)), "unchanged")
        self.assertEqual(again.get(TEAM, "c1")["shooting"]["team"]["players"][0]["points"], 99)

    def test_preexisting_duplicates_collapsed(self):
        with open(self.path, "w") as f:
            json.dump({"meta": {}, "games": [record("c1"), record("c1"), record("c2")]}, f)
        ds = BoxScoreDataset(self.path)
        self.assertEqual((len(ds), ds.dupes_dropped), (2, 1))


class GapFileIO(unittest.TestCase):
    def test_roundtrip_legacy_buckets_and_corrupt_recovery(self):
        d = tempfile.mkdtemp()
        try:
            path = os.path.join(d, "tx_data_gaps_boys_2026_2027.json")
            gf = GapFile(path, "TX", "Texas", "boys", "2026-2027")
            gf.teams[f"https://www.maxpreps.com/{TEAM}/"] = {
                "teamName": "Austin Maroons", "teamUrl": f"https://www.maxpreps.com/{TEAM}/", "region": "R",
                "schedule_status": "OK", "schedule_last_attempt": "", "schedule_error": ""}
            a, b = ref("a"), ref("b")
            gf.games[a["game_key"]], gf.games[b["game_key"]] = a, b
            record_attempt(a, Outcome(SUCCESS, data_class="full"), Policy(), NOW)
            gf.save(export_excel=True)
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(len(data["games"]), 2)
            self.assertEqual(data["meta"]["statusCounts"]["SUCCESS"], 1)
            self.assertEqual(len(data["teamsFullBoxScores"]), 1)          # legacy shape still produced
            self.assertTrue(os.path.exists(gf.tracker_path()) or os.path.exists(path.replace(".json", "_tracker.csv")))
            self.assertEqual(len(GapFile.load(path).games), 2)
            with open(path, "w") as f:                                   # interrupted/garbled file
                f.write("{not json")
            self.assertEqual(len(GapFile.load(path).games), 0)
            self.assertTrue(any(".corrupt-" in n for n in os.listdir(d)))  # set aside, not overwritten
        finally:
            shutil.rmtree(d, ignore_errors=True)


class FullFlow(unittest.TestCase):
    """run_one() end to end with an injected schedule + injected game pages."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.master = os.path.join(self.d, "master.json")
        team = {"teamName": "Austin", "teamUrl": f"https://www.maxpreps.com/{TEAM}/"}
        with open(self.master, "w") as f:
            json.dump({"byState": {"TX": {"regions": {"R": {"teams": [team]}}}}}, f)
        self.pages = {}                                   # cid -> Outcome
        self.calls = []

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def args(self, *extra):
        return dg.build_parser().parse_args(
            ["--state", "TX", "--sport", "boys", "--master", self.master, "--output-dir", self.d,
             "--no-excel", "--delay", "0", "--no-stats-tab", "--no-accumulate", *extra])

    def sched(self, refs):
        def f(team):
            return {"ok": True, "refs": [dict(r) for r in refs], "record": None, "error": ""}
        return f

    def checker(self):
        def f(row):
            self.calls.append(row["contest_id"])
            return self.pages[row["contest_id"]](row) if callable(self.pages[row["contest_id"]]) else self.pages[row["contest_id"]]
        return f

    def run_(self, refs, *extra, now=NOW):
        return dg.run_one(self.args(*extra), "TX", "boys", schedule_fetcher=self.sched(refs),
                          checker=self.checker(), now=now, as_of=TODAY)

    def ok(self, cid):
        return Outcome(SUCCESS, "ok", data_class="full", record=record(cid), record_hash="h", http_status=200)

    def test_daily_cycle(self):
        refs = [ref("g1"), ref("g2", opp="tx/waco/waco-lions/basketball"), ref("old", date="2026-09-01"), ref("future", date="2026-10-20")]
        self.pages = {"g1": self.ok("g1"), "g2": Outcome(NO_DATA, "No player stats on the game page yet", http_status=200)}

        s1 = self.run_(refs)
        # only yesterday's games are discovered/attempted; old + future are ignored
        self.assertEqual((s1["new_games"], s1["attempted"], s1["SUCCESS"], s1["NO_DATA"]), (2, 2, 1, 1))
        gf = GapFile.load(os.path.join(self.d, "tx_data_gaps_boys_2026_2027.json"))
        self.assertEqual({r["contest_id"]: r["status"] for r in gf.games.values()}, {"g1": SUCCESS, "g2": NO_DATA})

        # rerun same date immediately: success untouched, no-data in back-off -> zero requests, no dupes
        self.calls.clear()
        s2 = self.run_(refs, now=NOW + dt.timedelta(minutes=5))
        self.assertEqual((s2["attempted"], self.calls), (0, []))

        # next day the stats appear -> only g2 is retried, appended once, status flips to SUCCESS
        self.pages["g2"] = self.ok("g2")
        self.calls.clear()
        s3 = self.run_(refs, "--skip-schedule", now=NOW + dt.timedelta(days=1), )
        self.assertEqual((self.calls, s3["SUCCESS"], s3["records_added"]), (["g2"], 1, 1))
        gf = GapFile.load(os.path.join(self.d, "tx_data_gaps_boys_2026_2027.json"))
        g2 = next(r for r in gf.games.values() if r["contest_id"] == "g2")
        self.assertEqual((g2["status"], g2["retry_count"], g2["data_retrieved"]), (SUCCESS, 2, True))
        ds = BoxScoreDataset(os.path.join(self.d, "tx_box_scores_boys_2026_2027.json"))
        self.assertEqual(sorted(g["contest_id"] for g in ds.games), ["g1", "g2"])

        # forced recheck of an unchanged game leaves its record alone
        self.pages["g1"] = self.ok("g1")
        s4 = self.run_(refs, "--recheck-contest", "g1", "--skip-schedule", now=NOW + dt.timedelta(days=2))
        self.assertEqual((s4["records_unchanged"], s4["records_added"], len(BoxScoreDataset(ds.path))), (1, 0, 2))

    def test_geoblock_aborts_without_burning_retries(self):
        refs = [ref(f"g{i}") for i in range(5)]
        self.pages = {r["contest_id"]: Outcome(FAILED, "HTTP 403 geo-block", http_status=403, infra=True) for r in refs}
        s = self.run_(refs, "--max-infra-failures", "3", "--game-workers", "1")
        self.assertTrue(s["aborted"])
        gf = GapFile.load(os.path.join(self.d, "tx_data_gaps_boys_2026_2027.json"))
        self.assertTrue(all(r["retry_count"] == 0 for r in gf.games.values()))

    def test_vanished_game_is_flagged_not_retried(self):
        self.pages = {"g1": Outcome(NO_DATA, "x")}
        self.run_([ref("g1")])
        s = self.run_([], now=NOW + dt.timedelta(days=1))
        self.assertEqual(s["left_schedule"], 1)
        self.assertEqual(s["attempted"], 0)

    def test_stats_tab_flow_priority_and_no_downgrade(self):
        """Stats tab is fetched for the teams that played in the window, stored, and never downgraded."""
        self.pages = {"g1": self.ok("g1")}
        calls = []

        def fetcher(url, tgc, season):
            calls.append(url)
            rec = [{"team_id": "tx/dallas/a-eagles/basketball", "team_name": "A Eagles", "record_type": "team_total", "GP": 5},
                   {"team_id": "tx/dallas/a-eagles/basketball", "team_name": "A Eagles", "record_type": "player", "Name": "X"}]
            return ("A Eagles", rec if state["has"] else [], "has_data" if state["has"] else "unparseable")
        state = {"has": True}
        a = self.args("--date", "2026-10-11")
        a.no_stats_tab, a.no_accumulate = False, True
        s = dg.run_one(a, "TX", "boys", schedule_fetcher=self.sched([ref("g1")]), checker=self.checker(),
                       now=NOW, as_of=TODAY, stats_tab_fetcher=fetcher)
        self.assertEqual(s["stats_tab"]["has_data"], len(calls))
        gf = GapFile.load(os.path.join(self.d, "tx_data_gaps_boys_2026_2027.json"))
        self.assertTrue(any(t.get("stats_tab_status") == "has_data" for t in gf.teams.values()))
        stab = os.path.join(self.d, "tx_all_stats_tab_boys_2026_2027.json")
        self.assertEqual(len(json.load(open(stab, encoding="utf-8"))), 2)
        # later the page empties out: the team keeps its stats-tab status and records
        state["has"] = False
        dg.run_one(a, "TX", "boys", schedule_fetcher=self.sched([ref("g1")]), checker=self.checker(),
                   now=NOW, as_of=TODAY, stats_tab_fetcher=fetcher)
        gf = GapFile.load(os.path.join(self.d, "tx_data_gaps_boys_2026_2027.json"))
        self.assertTrue(any(t.get("stats_tab_status") == "has_data" for t in gf.teams.values()))
        self.assertEqual(len(json.load(open(stab, encoding="utf-8"))), 2)


if __name__ == "__main__":
    unittest.main()
