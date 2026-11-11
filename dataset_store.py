"""
dataset_store.py — append / update box-score records without duplicates.

The output file keeps the shape the existing pipeline already writes
({state}_box_scores_{sport}[_jv]_{season}.json -> {"meta": ..., "games": [...]}),
so Accumulation_data.py and the rest of stage 4 can read it unchanged.

Identity of a record:   (team.team_id, contest_id)
                        (team.team_id, normalised game_url) when contest_id is empty

upsert() semantics — this is what makes re-running a date safe:
    key not present            -> append                      ("added")
    key present, same content  -> leave the record untouched  ("unchanged")
    key present, new content   -> replace it IN PLACE         ("updated")
Any pre-existing duplicate of the same key in the file is dropped on the way.
"""

import os
import json
import hashlib

from wf_core import now_str, log
from gap_store import norm_url


def record_key(rec):
    team_id = (rec.get("team") or {}).get("team_id", "")
    cid = (rec.get("contest_id") or "").lower()
    return (team_id, cid or "url:" + norm_url(rec.get("game_url", "")))


def content_hash(rec):
    """Hash of what a record SAYS about the game (stats, sides, date) — not of
    incidental fields — so an unchanged page never triggers a rewrite."""
    payload = {
        "date": rec.get("game_date"),
        "team": (rec.get("team") or {}).get("team_id"),
        "opp": (rec.get("opponent") or {}).get("team_id"),
        "stats": {c: rec.get(c) for c in ("shooting", "detailed_shooting", "totals", "misc")},
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False)
                          .encode("utf-8")).hexdigest()


class BoxScoreDataset:
    def __init__(self, path):
        self.path = path
        self.games = []          # list of records, in file order
        self.meta = {}
        self._index = {}         # key -> position in self.games
        self.dirty = False
        self.dupes_dropped = 0
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            bad = self.path + ".corrupt-" + now_str().replace(":", "").replace(" ", "-")
            os.replace(self.path, bad)
            log(f"[WARN] {os.path.basename(self.path)} unreadable ({e}); moved to {bad}")
            return
        self.meta = data.get("meta", {})
        for rec in data.get("games", []):
            k = record_key(rec)
            if k in self._index:                  # duplicate already in the file
                self.dupes_dropped += 1
                self.dirty = True
                continue
            self._index[k] = len(self.games)
            self.games.append(rec)
        if self.dupes_dropped:
            log(f"[WARN] dropped {self.dupes_dropped} duplicate record(s) while loading "
                f"{os.path.basename(self.path)}")

    def __len__(self):
        return len(self.games)

    def get(self, team_id, contest_id):
        i = self._index.get((team_id, (contest_id or "").lower()))
        return self.games[i] if i is not None else None

    def upsert(self, rec):
        """-> 'added' | 'updated' | 'unchanged'."""
        k = record_key(rec)
        i = self._index.get(k)
        if i is None:
            self._index[k] = len(self.games)
            self.games.append(rec)
            self.dirty = True
            return "added"
        if content_hash(self.games[i]) == content_hash(rec):
            return "unchanged"
        self.games[i] = rec
        self.dirty = True
        return "updated"

    def save(self, processed_teams=None, errors=None):
        meta = dict(self.meta)
        known = set(meta.get("processedTeams", []))
        known.update(processed_teams or [])
        meta.update({
            "totalGames": len(self.games),
            "totalErrors": len(errors) if errors is not None else meta.get("totalErrors", 0),
            "processedTeamsCount": len(known),
            "processedTeams": sorted(known),
            "last_updated": now_str(),
        })
        if errors is not None:
            meta["errors"] = errors
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write('{\n"meta": ' + json.dumps(meta, indent=2, ensure_ascii=False))
            f.write(',\n"games": [\n')
            f.write(",\n".join(json.dumps(g, ensure_ascii=False, separators=(",", ":"))
                               for g in self.games))
            f.write("\n]\n}\n")
        os.replace(tmp, self.path)
        self.meta = meta
        self.dirty = False
