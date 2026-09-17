#!/usr/bin/env python3
"""
Base64-encodes team and conference logo PNGs in ui/team_logos/ and
ui/conference_logos/ into a single JSON manifest the dashboard can
embed directly (published pages can't load external images -- see
ui/dashboard_shell.html -- so everything has to be inlined as data
URIs).

Filenames in those two folders are expected to be the safe-encoded
team/conference name (spaces and punctuation replaced with
underscores) plus .png -- e.g. "Miami_FL_.png" for "Miami (FL)".
A team/conference with no matching file simply won't appear in the
manifest, and the dashboard falls back to a generated monogram badge
for it.

Usage:
    python src/build_logo_assets.py --db db/league.db --out ui/logo_assets.json
"""
from __future__ import annotations

import argparse
import base64
import re
import sqlite3
from pathlib import Path
import json

TEAM_LOGO_DIR = Path("ui/team_logos")
CONF_LOGO_DIR = Path("ui/conference_logos")


def safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")


def encode_dir(names: list[str], directory: Path) -> dict[str, str]:
    out = {}
    for name in names:
        path = directory / f"{safe(name)}.png"
        if not path.exists():
            continue
        b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        out[name] = f"data:image/png;base64,{b64}"
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="ui/logo_assets.json")
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    teams = [r[0] for r in conn.execute("SELECT team_name FROM teams")]
    confs = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT conference_real FROM team_membership_by_season WHERE conference_real != 'FBS Independents'"
        )
    ]
    conn.close()

    manifest = {
        "teams": encode_dir(teams, TEAM_LOGO_DIR),
        "conferences": encode_dir(confs, CONF_LOGO_DIR),
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(manifest))
    n_teams, n_confs = len(manifest["teams"]), len(manifest["conferences"])
    print(f"Encoded {n_teams}/{len(teams)} team logos, {n_confs}/{len(confs)} conference logos")
    print(f"Wrote {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
