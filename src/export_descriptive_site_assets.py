#!/usr/bin/env python3
"""Build isolated descriptive scouting JS assets from archived, verified CFBD inputs.

Never changes the supplied production league DB: SQLite backup is taken into
memory before loading research splits. Every asset is versioned by its content.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from src.load_game_advanced import load_game_advanced
from src.export_matchup_descriptive import build_descriptive_contract
from src.validate_matchup_descriptive_contract import verify_contract

SOURCE_ROOT = Path("data/research/matchup_cfbd")
SEASONS = (2025, 2026)

def export(db_path: str, output: str = "ui/data/scouting", as_of: str | None = None) -> dict:
    paths = [SOURCE_ROOT / f"game_advanced_{year}.csv" for year in SEASONS]
    if not all(p.is_file() and p.stat().st_size for p in paths):
        raise FileNotFoundError("Verified CFBD research snapshots missing; fail closed")
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)
    # Read-only original; all mutable research data stays in the scratch database.
    source = sqlite3.connect(f"file:{Path(db_path).resolve().as_posix()}?mode=ro", uri=True)
    scratch = sqlite3.connect(":memory:")
    try:
        source.backup(scratch)
        for path in paths:
            result = load_game_advanced(scratch, str(path))
            if result["unmatched"]:
                raise ValueError(f"Unknown CFBD team identities in {path}: {result['unmatched']}")
            if result["loaded"] <= 0:
                raise ValueError(f"No advanced rows matched canonical games: {path}")
        versions = {}
        for year in SEASONS:
            # Source is season-scoped; never label a future game as observed.
            cutoff = as_of if year == max(SEASONS) and as_of else f"{year + 1}-01-01"
            date.fromisoformat(cutoff)
            data = build_descriptive_contract(scratch, season=year, as_of=cutoff)
            validation = verify_contract(data)
            if not validation["passed"] or validation["rated_team_metric_pairs"] < 1:
                raise ValueError(f"{year}: scouting contract failed: {validation}")
            # No fabricated values and no script injection; json.dumps is safe
            # for JS literals because the payload is JSON built from numeric IDs.
            raw = json.dumps(data, separators=(",", ":"), allow_nan=False)
            text = f'window.__ELOCAST_SCOUTING__ = window.__ELOCAST_SCOUTING__ || {{}};\nwindow.__ELOCAST_SCOUTING__[{year}] = {raw};\n'
            path = out / f"{year}.js"
            path.write_text(text, encoding="utf-8")
            versions[str(year)] = hashlib.sha256(text.encode()).hexdigest()[:12]
        return versions
    finally:
        scratch.close()
        source.close()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default="ui/data/scouting")
    p.add_argument("--as-of", default=None)
    a=p.parse_args()
    print(json.dumps(export(a.db,a.out,a.as_of), sort_keys=True))

if __name__ == "__main__":
    main()
