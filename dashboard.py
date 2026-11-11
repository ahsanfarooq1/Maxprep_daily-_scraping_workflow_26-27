"""
dashboard.py — Streamlit front end for the date-driven Gap Finder.

    streamlit run dashboard.py

Read-only view of the Gap Finder files written by daily_gapfinder.py:
  * what is due to be scraped right now (and why)
  * which teams need to be scraped again
  * every game, filterable, with its status and log
  * a Run tab that starts daily_gapfinder.py in the background

It never modifies a gap file itself — only daily_gapfinder.py does.
"""
import os
import sys
import json
import glob
import datetime as dt
import subprocess

import pandas as pd
import streamlit as st

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from wf_core import SUCCESS, NO_DATA, FAILED, PENDING, RETRY, STATUSES, today, resolve_window  # noqa: E402
from gap_store import Policy, due_reason  # noqa: E402

st.set_page_config(page_title="MaxPreps Gap Finder", page_icon="🏀", layout="wide")

STATUS_COLORS = {SUCCESS: "#C6EFCE", NO_DATA: "#FFEB9C", FAILED: "#FFC7CE", RETRY: "#FCE4D6", PENDING: "#DDEBF7"}
ROW_COLS = ["state", "sport", "team_name", "opponent_team_name", "game_date", "status", "retry_count",
            "last_attempt", "next_retry_after", "log_message", "game_url"]


# ─── Loading (read-only, cached by file mtime) ───────────────────────────────
def find_gap_files(root):
    files = [f for f in glob.glob(os.path.join(root, "**", "*_data_gaps_*.json"), recursive=True)
             if not f.endswith(".tmp") and ".corrupt-" not in f]
    return sorted(files)


@st.cache_data(show_spinner=False)
def load_file(path, mtime):
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return d.get("meta", {}), d.get("games", []), d.get("teams", [])


def load_selected(paths):
    games, teams, metas, problems = [], [], {}, []
    for p in paths:
        try:
            meta, g, t = load_file(p, os.path.getmtime(p))
        except Exception as e:
            problems.append(f"{os.path.basename(p)}: {e}")
            continue
        if not g and not meta.get("schema_version"):
            problems.append(f"{os.path.basename(p)}: old-format file (no per-game rows yet) — "
                            f"run daily_gapfinder.py once to migrate it")
        metas[p] = meta
        for r in g:
            r["_file"] = os.path.basename(p)
        games += g
        for x in t:
            x["_file"] = os.path.basename(p)
            x["state"], x["sport"] = meta.get("stateCode", ""), (meta.get("sport", "") or "").split()[0].lower()
        teams += t
    return pd.DataFrame(games), pd.DataFrame(teams), metas, problems


def style_status(df):
    def color(v):
        c = STATUS_COLORS.get(v)
        return f"background-color: {c}; color: #111" if c else ""
    return df.style.map(color, subset=["status"]) if "status" in df.columns else df


def show_table(df, key, cols=None, height=420):
    cols = [c for c in (cols or ROW_COLS) if c in df.columns]
    st.dataframe(style_status(df[cols]), width="stretch", height=height, hide_index=True,
                 column_config={"game_url": st.column_config.LinkColumn("game_url", width="medium")})
    st.download_button("Download CSV", df[cols].to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"{key}.csv", mime="text/csv", key=f"dl_{key}")


# ─── Sidebar ─────────────────────────────────────────────────────────────────
st.sidebar.title("🏀 Gap Finder")
root = st.sidebar.text_input("Output folder", os.path.join(HERE, "output"))
files = find_gap_files(root)
if not files:
    st.sidebar.warning("No gap files found yet.")
labels = {f: os.path.relpath(f, root) for f in files}
chosen = st.sidebar.multiselect("Gap files", files, default=files, format_func=lambda f: labels[f])

st.sidebar.subheader("What counts as 'due'")
as_of = today()
mode = st.sidebar.radio("Game dates", ["Yesterday (default)", "One date", "Date range"])
if mode == "One date":
    d1 = st.sidebar.date_input("Date", as_of - dt.timedelta(days=1))
    window = (d1, d1)
elif mode == "Date range":
    a_, b_ = st.sidebar.date_input("Range", (as_of - dt.timedelta(days=7), as_of - dt.timedelta(days=1)))
    window = (a_, b_)
else:
    window = resolve_window(as_of=as_of)
explicit = mode != "Yesterday (default)"
max_attempts = st.sidebar.number_input("Max attempts per game", 1, 30, 6)
retry_days = st.sidebar.number_input("Retry window (days after game)", 1, 90, 14)
ignore_backoff = st.sidebar.checkbox("Ignore back-off timers (show everything retryable)", value=explicit)
if st.sidebar.button("↻ Reload files"):
    st.cache_data.clear()
    st.rerun()

policy = Policy(max_attempts=max_attempts, retry_window_days=retry_days, ignore_backoff=ignore_backoff)

df, teams_df, metas, problems = load_selected(chosen)
for p in problems:
    st.warning(p)

st.title("MaxPreps Gap Finder — what to scrape")
st.caption(f"Today (US Central): {as_of} · window: {window[0]} → {window[1]}"
           + (" · back-off timers ignored" if ignore_backoff else ""))

tab_over, tab_due, tab_teams, tab_games, tab_gaveup, tab_run = st.tabs(
    ["Overview", "To scrape now", "Teams to re-scrape", "All games", "Gave up", "Run"])

if df.empty:
    with tab_over:
        st.info("No game rows loaded. Run the Gap Finder first (Run tab), then reload.")
else:
    now = dt.datetime.now()
    df["_why"] = [due_reason(r, now, as_of, window, policy) or ""
                  for r in df.to_dict("records")]
    df["game_date_d"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    due = df[df["_why"] != ""].sort_values(["game_date", "team_name"])

    # ── Overview ─────────────────────────────────────────────────────────────
    with tab_over:
        counts = df["status"].value_counts().reindex(list(STATUSES), fill_value=0)
        cols = st.columns(len(STATUSES) + 1)
        for c, s in zip(cols, STATUSES):
            c.metric(s, f"{counts[s]:,}")
        cols[-1].metric("Due now", f"{len(due):,}")
        left, right = st.columns([3, 2])
        with left:
            st.subheader("Status by state / sport")
            piv = (df.groupby(["state", "sport", "status"]).size().unstack(fill_value=0)
                   .reindex(columns=list(STATUSES), fill_value=0))
            piv["total"] = piv.sum(axis=1)
            piv["% success"] = (piv[SUCCESS] / piv["total"] * 100).round(1)
            st.dataframe(piv, width="stretch")
        with right:
            st.subheader("Games per date")
            recent = df[df["game_date_d"].notna()].groupby(["game_date", "status"]).size().unstack(fill_value=0)
            st.bar_chart(recent.tail(30))
        st.subheader("Files")
        st.dataframe(pd.DataFrame([{
            "file": labels[p], "last updated": m.get("last_updated"), "teams": m.get("totalTeams"),
            "games": m.get("totalGames"), "teams w/ schedule error": m.get("errors_count"),
            "last run": (m.get("run_log") or [{}])[-1].get("at")} for p, m in metas.items()]),
            width="stretch", hide_index=True)

    # ── To scrape now ────────────────────────────────────────────────────────
    with tab_due:
        st.subheader(f"{len(due):,} games will be attempted on the next run")
        st.caption("Exactly the games `daily_gapfinder.py` would pick with these settings: "
                   "new games in the window plus non-successful games past their back-off. "
                   "Future games and games that gave up are excluded.")
        if due.empty:
            st.success("Nothing is due.")
        else:
            why_counts = due["_why"].value_counts()
            c = st.columns(max(len(why_counts), 1))
            for col, (k, v) in zip(c, why_counts.items()):
                col.metric({"new": "New in window", "pending": "Pending backlog", "retry": "Retry",
                            "recheck-success": "Recheck", "recheck-partial": "Recheck partial",
                            "recheck-contest": "Recheck"}.get(k, k), v)
            f1, f2 = st.columns(2)
            reasons = f1.multiselect("Reason", sorted(due["_why"].unique()), default=sorted(due["_why"].unique()))
            sts = f2.multiselect("Status", list(STATUSES), default=[s for s in STATUSES if s in set(due["status"])])
            view = due[due["_why"].isin(reasons) & due["status"].isin(sts)].rename(columns={"_why": "due_reason"})
            show_table(view, "due_games", ["due_reason"] + ROW_COLS)

        upcoming = df[(df["status"] == PENDING) & (df["game_date_d"] > as_of)]
        with st.expander(f"Upcoming games (scheduled, not played yet): {len(upcoming):,}"):
            show_table(upcoming.sort_values("game_date"), "upcoming", height=300)

    # ── Teams to re-scrape ───────────────────────────────────────────────────
    with tab_teams:
        st.caption("Teams with at least one game that is not yet SUCCESS and still retryable, "
                   "or whose schedule could not be fetched.")
        bad = df[df["status"].isin([NO_DATA, FAILED, RETRY]) & ~df["retry_exhausted"].astype(bool)
                 & df["on_schedule"].astype(bool) & (df["duplicate_of"].fillna("") == "")]
        agg = (bad.groupby(["_file", "state", "sport", "team_name", "team_url"])
               .agg(games_to_retry=("status", "size"),
                    no_data=("status", lambda s: (s == NO_DATA).sum()),
                    failed=("status", lambda s: (s == FAILED).sum()),
                    retry=("status", lambda s: (s == RETRY).sum()),
                    due_now=("_why", lambda s: (s != "").sum()),
                    oldest_game=("game_date", "min"), newest_game=("game_date", "max"),
                    next_retry_after=("next_retry_after", lambda s: min([x for x in s if x], default="")),
                    max_attempts_used=("retry_count", "max"),
                    last_log=("log_message", "last"))
               .reset_index().sort_values(["due_now", "games_to_retry"], ascending=False))
        sched_err = teams_df[teams_df.get("schedule_status", "") == "ERROR"] if not teams_df.empty else teams_df
        m1, m2, m3 = st.columns(3)
        m1.metric("Teams needing re-scrape", f"{len(agg):,}")
        m2.metric("…with games due now", f"{(agg['due_now'] > 0).sum():,}" if len(agg) else "0")
        m3.metric("Schedule fetch errors", f"{len(sched_err):,}")
        sel = st.text_input("Filter team name")
        view = agg[agg["team_name"].str.contains(sel, case=False, na=False)] if sel and len(agg) else agg
        if len(view):
            st.dataframe(view.drop(columns=["team_url", "_file"]), width="stretch", hide_index=True, height=420)
            st.download_button("Download CSV", view.to_csv(index=False).encode("utf-8-sig"),
                               "teams_to_rescrape.csv", "text/csv")
            team = st.selectbox("Inspect a team's games", ["—"] + sorted(view["team_name"].unique()))
            if team != "—":
                show_table(df[df["team_name"] == team].sort_values("game_date"), "team_games",
                           ROW_COLS + ["data_class"], height=320)
        else:
            st.success("No teams need re-scraping.")
        if len(sched_err):
            with st.expander("Teams whose schedule could not be fetched (will be retried next run)"):
                st.dataframe(sched_err[["state", "sport", "teamName", "schedule_last_attempt", "schedule_error"]],
                             width="stretch", hide_index=True)

    # ── All games ────────────────────────────────────────────────────────────
    with tab_games:
        c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
        sts = c1.multiselect("Status", list(STATUSES), default=list(STATUSES), key="g_status")
        states = c2.multiselect("State", sorted(df["state"].unique()), default=sorted(df["state"].unique()))
        sports = c3.multiselect("Sport", sorted(df["sport"].unique()), default=sorted(df["sport"].unique()))
        q = c4.text_input("Search team / opponent / log")
        v = df[df["status"].isin(sts) & df["state"].isin(states) & df["sport"].isin(sports)]
        if mode != "Yesterday (default)":
            v = v[(v["game_date_d"] >= window[0]) & (v["game_date_d"] <= window[1])]
        if q:
            mask = (v["team_name"].str.contains(q, case=False, na=False)
                    | v["opponent_team_name"].str.contains(q, case=False, na=False)
                    | v["log_message"].str.contains(q, case=False, na=False))
            v = v[mask]
        st.caption(f"{len(v):,} games" + (f" (dates {window[0]} → {window[1]})" if mode != "Yesterday (default)" else ""))
        show_table(v.sort_values(["game_date", "team_name"], ascending=[False, True]).head(5000), "games",
                   ROW_COLS + ["data_class"], height=520)
        if len(v) > 5000:
            st.caption("Showing the first 5,000 rows — narrow the filters or download the Excel tracker.")

    # ── Gave up ──────────────────────────────────────────────────────────────
    with tab_gaveup:
        gu = df[df["retry_exhausted"].astype(bool) | (df["duplicate_of"].fillna("") != "") | ~df["on_schedule"].astype(bool)]
        st.caption("Games that stopped being retried: attempts used up, retry window passed, duplicate contest id, "
                   "or removed from the schedule. Re-attempt with `--force-exhausted --date ...`.")
        st.metric("Games not being retried", f"{len(gu):,}")
        show_table(gu.sort_values("game_date", ascending=False), "gave_up", ROW_COLS + ["duplicate_of", "on_schedule"])

# ─── Run tab (works even with no files yet) ──────────────────────────────────
with tab_run:
    st.caption("Starts `daily_gapfinder.py` as a background process. Needs the VPN on. "
               "Always preview with a dry run first.")
    r1, r2, r3, r4 = st.columns(4)
    r_state = r1.text_input("State code(s)", "TX", help="Comma-separated, e.g. TX,OK")
    r_sport = r2.selectbox("Sport", ["boys", "girls", "both"])
    r_season = r3.text_input("Season", "2026-2027")
    r_limit = r4.number_input("Limit teams (0 = all)", 0, 5000, 0)
    o1, o2, o3 = st.columns(3)
    r_skip = o1.checkbox("Retry only (skip schedule fetch)")
    r_noscrape = o2.checkbox("Discovery only (scrape nothing)")
    r_dry = o3.checkbox("Dry run (write nothing)", value=True)
    cmd = [sys.executable, "-u", os.path.join(HERE, "daily_gapfinder.py"), "--state", r_state,
           "--sport", r_sport, "--season", r_season, "--output-root", root]
    if mode == "One date":
        cmd += ["--date", str(window[0])]
    elif mode == "Date range":
        cmd += ["--date-from", str(window[0]), "--date-to", str(window[1])]
    if r_limit:
        cmd += ["--limit-teams", str(r_limit)]
    cmd += (["--skip-schedule"] if r_skip else []) + (["--no-scrape"] if r_noscrape else []) + (["--dry-run"] if r_dry else [])
    st.code(" ".join(f'"{c}"' if " " in c else c for c in cmd[1:]), language="bash")

    log_dir = os.path.join(root, "_run_logs")
    if st.button("▶ Start run", type="primary"):
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"run_{dt.datetime.now():%Y%m%d_%H%M%S}.log")
        with open(log_path, "w", encoding="utf-8") as lf:
            proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, cwd=HERE,
                                    env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"})
        st.session_state["run"] = {"log": log_path, "pid": proc.pid}
        st.session_state["_proc"] = proc
    run = st.session_state.get("run")
    if run:
        proc = st.session_state.get("_proc")
        rc = proc.poll() if proc else None
        st.write(f"**Log:** `{os.path.basename(run['log'])}` — "
                 + ("running…" if rc is None else f"finished (exit code {rc})"))
        st.button("↻ Refresh log")
        try:
            with open(run["log"], encoding="utf-8", errors="replace") as lf:
                st.code("".join(lf.readlines()[-60:]) or "(no output yet)")
        except OSError:
            pass
        if rc is not None and not r_dry:
            st.info("Run finished — click **Reload files** in the sidebar to see the new results.")
