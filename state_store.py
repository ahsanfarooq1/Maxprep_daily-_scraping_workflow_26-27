"""state_store.py - keep the pipeline's working state OUT of git history.

The daily run needs three kinds of per-state files to resume (retry state, the raw box scores and the stats-tab
data):   <State>/<st>_data_gaps_<sport>_2026_2027.json
         <State>/<st>_box_scores_<sport>_2026_2027.json
         <State>/<st>_all_stats_tab_<sport>_2026_2027.json
They are ~870 MB of JSON, so committing them every day makes the repo balloon. Instead the workflow packs them into
one compressed archive (state.tar.gz, ~10x smaller) kept as a GitHub Release asset, and git only holds the small
Final files, team lists and reports.   (<st>_accumulated_stats_*.json is rebuilt from the box scores on every run,
and the .xlsx trackers are never made on GitHub, so neither is part of the state.)

    python state_store.py pack   --data-root "Scraped data 2026-27" --out state.tar.gz
    python state_store.py unpack --archive state.tar.gz --data-root "Scraped data 2026-27"
    python state_store.py check  --data-root "Scraped data 2026-27"

check exits 4 when a Final file exists but its gap file or its box-score/stats-tab source is missing: that means the
state was lost, and running on would rebuild the Final file from (almost) nothing. The workflow refuses to run then.
"""
import argparse
import glob
import hashlib
import io
import json
import os
import sys
import tarfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PATTERNS = ("*_data_gaps_*_2026_2027.json", "*_box_scores_*_2026_2027.json", "*_all_stats_tab_*_2026_2027.json")
INFO = "STATE_INFO.json"


def state_files(data_root):
    out = []
    for pat in PATTERNS:
        out += glob.glob(os.path.join(data_root, "*", pat))
    return sorted(out)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def cmd_pack(a):
    files = state_files(a.data_root)
    if not files:
        sys.exit("pack: no state files found under " + a.data_root)
    info = {"created": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), "files": {}}
    tmp = a.out + ".tmp"
    with tarfile.open(tmp, "w:gz", compresslevel=6) as tar:
        for fp in files:
            arc = os.path.relpath(fp, a.data_root).replace("\\", "/")
            info["files"][arc] = {"bytes": os.path.getsize(fp), "sha256": sha256_file(fp)}
            tar.add(fp, arcname=arc)
        blob = json.dumps(info, indent=1).encode("utf-8")
        ti = tarfile.TarInfo(INFO)
        ti.size = len(blob)
        ti.mtime = time.time()
        tar.addfile(ti, io.BytesIO(blob))
    os.replace(tmp, a.out)
    raw = sum(v["bytes"] for v in info["files"].values())
    print(f"pack: {len(files)} files, {raw / 1048576:.0f} MB -> {a.out} ({os.path.getsize(a.out) / 1048576:.0f} MB)")


def cmd_unpack(a):
    root = os.path.abspath(a.data_root)
    with tarfile.open(a.archive, "r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
        if INFO not in members:
            sys.exit(f"unpack: {INFO} missing - not a state archive")
        info = json.load(tar.extractfile(members[INFO]))
        n = 0
        for arc, meta in info["files"].items():
            dest = os.path.abspath(os.path.join(root, arc))
            if not dest.startswith(root + os.sep) or arc not in members:
                sys.exit(f"unpack: unsafe or missing entry {arc}")
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            tmp = dest + ".unpack.tmp"
            h = hashlib.sha256()
            with tar.extractfile(members[arc]) as src, open(tmp, "wb") as dst:
                for chunk in iter(lambda: src.read(1 << 20), b""):
                    h.update(chunk)
                    dst.write(chunk)
            if h.hexdigest() != meta["sha256"]:
                os.remove(tmp)
                sys.exit(f"unpack: checksum mismatch for {arc} - archive is corrupt, nothing further extracted")
            os.replace(tmp, dest)
            n += 1
    print(f"unpack: {n} files restored (archive created {info.get('created')})")


def cmd_check(a):
    problems = []
    for fp in sorted(glob.glob(os.path.join(a.data_root, "*", "Final_*_accumulated_*_26_27.json"))):
        folder = os.path.dirname(fp)
        parts = os.path.basename(fp).split("_")                 # Final_<st>_accumulated_<sport>_26_27.json
        st, sport = parts[1], parts[3]
        gaps = os.path.join(folder, f"{st}_data_gaps_{sport}_2026_2027.json")
        box = os.path.join(folder, f"{st}_box_scores_{sport}_2026_2027.json")
        stab = os.path.join(folder, f"{st}_all_stats_tab_{sport}_2026_2027.json")
        tag = f"{st.upper()} {sport}"
        if not os.path.exists(gaps):
            problems.append(f"{tag}: gap file missing")
        if not (os.path.exists(box) or os.path.exists(stab)):
            problems.append(f"{tag}: neither box-score nor stats-tab file present")
    if problems:
        print("STATE CHECK FAILED - the working state is incomplete; refusing to continue so the Final files "
              "are not rebuilt from nothing:")
        for p in problems:
            print("  -", p)
        return 4
    print("state check ok: every Final file has its gap file and a box-score / stats-tab source")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("pack")
    p.add_argument("--data-root", default=os.path.join(HERE, "Scraped data 2026-27"))
    p.add_argument("--out", default="state.tar.gz")
    p.set_defaults(fn=cmd_pack)
    p = sub.add_parser("unpack")
    p.add_argument("--archive", default="state.tar.gz")
    p.add_argument("--data-root", default=os.path.join(HERE, "Scraped data 2026-27"))
    p.set_defaults(fn=cmd_unpack)
    p = sub.add_parser("check")
    p.add_argument("--data-root", default=os.path.join(HERE, "Scraped data 2026-27"))
    p.set_defaults(fn=cmd_check)
    a = ap.parse_args()
    return a.fn(a) or 0


if __name__ == "__main__":
    sys.exit(main())
