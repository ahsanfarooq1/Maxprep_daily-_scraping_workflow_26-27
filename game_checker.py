"""
game_checker.py — step 6 of the daily workflow: attempt one game.

ONE page fetch yields both the gap-finder classification and the box-score
record, using the production fetch chain and parser (scrape_box_scores), so
results match what the existing pipeline would have produced. What this adds
over scrape_game()/check_game_worker() is an honest verdict:

    SUCCESS  page parsed, at least one side has player rows
    NO_DATA  page reachable (200/404) but no stats published — not played yet,
             or the coaches never entered any. Worth re-checking later.
    FAILED   a technical problem: HTTP 5xx/429/other, timeout, parse crash.
    infra    (flag on a FAILED outcome) geo-block / network down — says nothing
             about the game, so the retry budget is not charged for it.

Returns gap_store.Outcome; never raises.
"""

import re
import time

import requests
from bs4 import BeautifulSoup

from wf_core import SUCCESS, NO_DATA, FAILED, CLS_FULL, CLS_TEAM, CLS_OPP, legacy, date_from_url, iso_to_mdy
from gap_store import Outcome, contest_id_from_url
from dataset_store import content_hash

CATS = ("shooting", "detailed_shooting", "totals", "misc")


def _fetch(bx, url, delay, timeout=25):
    time.sleep(delay)
    return bx._http_get_page(url, timeout=timeout, allow_redirects=True)


def check_game(row, opp_index=None, delay=0.3, http=None):
    """Attempt one game row. `http` (url -> (status, html, final_url)) lets
    tests inject pages without the network."""
    try:
        bx, app = legacy()
        get = http or (lambda u: _fetch(bx, u, delay))

        guid, ssid = row.get("contest_id") or None, row.get("ssid") or None
        url = (f"https://www.maxpreps.com/local/stats/boxscore.aspx?contestid={guid}&ssid={ssid}"
               if guid and ssid else row["game_url"])
        if not url or not url.startswith("http"):
            return Outcome(FAILED, "Invalid or missing game URL")

        status, html, final_url = get(url)
        final_url = final_url or url
        if status == 404:
            return Outcome(NO_DATA, "Game page returned 404 (not published, moved or removed)",
                           http_status=404)
        if status == 403 or (html and bx._is_geo_block(None, html[:2000])):
            return Outcome(FAILED, "HTTP 403 geo-block: connect a system-wide VPN",
                           http_status=403, infra=True)
        if status in (429,) or (status or 0) >= 500:
            return Outcome(FAILED, f"HTTP {status} from MaxPreps; retry required", http_status=status)
        if status != 200 or not html:
            return Outcome(FAILED, f"Request failed (HTTP {status}); retry required", http_status=status)

        # 2026 redesign: player stats only render under ?tab=stats.
        stats_url = bx._with_stats_tab(final_url)
        if stats_url != final_url:
            st2, html2, final2 = get(stats_url)
            if st2 == 200 and html2:
                html, final_url = html2, (final2 or stats_url)

        soup = BeautifulSoup(html, "html.parser")
        try:
            page = bx.parse_game_page(soup, final_url, row["team_name"], row["team_id"], opp_index)
        except Exception as e:
            return Outcome(FAILED, f"Parse error: {type(e).__name__}: {e}", http_status=200)

        if not page:
            return Outcome(NO_DATA, "No player stats on the game page yet", http_status=200,
                           final_url=final_url)

        has_team = any(page[c]["team"]["players"] for c in CATS)
        has_opp = any(page[c]["opponent"]["players"] for c in CATS)
        if not (has_team or has_opp):
            return Outcome(NO_DATA, "Game page found but no player rows entered by either team",
                           http_status=200, final_url=final_url,
                           opponent_team_id=page["opp_id"], opponent_team_name=page["opp_name"])

        cls = CLS_FULL if (has_team and has_opp) else (CLS_TEAM if has_team else CLS_OPP)
        msg = {CLS_FULL: "Data scraped successfully (both teams)",
               CLS_TEAM: "Data scraped; only this team entered stats",
               CLS_OPP: "Data scraped; only the opponent entered stats"}[cls]

        cid = guid or contest_id_from_url(final_url) or None
        record = {
            "contest_id": cid,
            "game_url": final_url,
            "game_date": (re.search(r"/(\d{1,2}-\d{1,2}-\d{4})/", final_url or "") or [None, ""])[1]
                         or iso_to_mdy(row.get("game_date", "")),
            "is_deleted": False,
            "team": {"team_id": row["team_id"], "team_name": row["team_name"]},
            "opponent": {"team_id": page["opp_id"], "team_name": page["opp_name"]},
            "shooting": page["shooting"], "detailed_shooting": page["detailed_shooting"],
            "totals": page["totals"], "misc": page["misc"],
        }
        return Outcome(SUCCESS, msg, data_class=cls, record=record,
                       record_hash=content_hash(record),
                       fingerprint=app._stats_fingerprint(page),
                       http_status=200, final_url=final_url,
                       opponent_team_id=page["opp_id"], opponent_team_name=page["opp_name"])

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        return Outcome(FAILED, f"Network error: {type(e).__name__}; retry required", infra=True)
    except Exception as e:
        return Outcome(FAILED, f"Unexpected error: {type(e).__name__}: {e}")
