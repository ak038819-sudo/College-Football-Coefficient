#!/usr/bin/env python3
"""Build isolated descriptive scouting JS assets from archived, verified CFBD inputs.

Never changes the supplied production league DB: SQLite backup is taken into
memory before loading research splits. Every asset is versioned by its content.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
try:
    from src.load_game_advanced import load_game_advanced
    from src.export_matchup_descriptive import build_descriptive_contract
    from src.validate_matchup_descriptive_contract import verify_contract
    from src.check_matchup_data_readiness import readiness_report
except ModuleNotFoundError:
    from load_game_advanced import load_game_advanced
    from export_matchup_descriptive import build_descriptive_contract
    from validate_matchup_descriptive_contract import verify_contract
    from check_matchup_data_readiness import readiness_report

SOURCE_ROOT = Path("data/research/matchup_cfbd")
SEASONS = (2025, 2026)

SPLIT_COLUMNS = {
    "off_rush_success_rate", "off_rush_ppa", "off_rush_explosiveness",
    "off_pass_success_rate", "off_pass_ppa", "off_pass_explosiveness",
}

def has_split_observations(path: Path) -> bool:
    if not path.is_file() or not path.stat().st_size:
        return False
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if not SPLIT_COLUMNS.issubset(set(reader.fieldnames or ())):
            return False
        return any(all(str(row.get(k) or "").strip() for k in SPLIT_COLUMNS) for row in reader)

def latest_covered_game(conn: sqlite3.Connection, season: int) -> str | None:
    query = ("SELECT MAX(SUBSTR(g.game_date,1,10)) FROM games g "
             "JOIN game_team_advanced a ON a.game_id=g.game_id "
             "JOIN team_membership_by_season mh ON mh.team_id=g.home_team_id AND mh.season_year=g.season_year "
             "JOIN team_membership_by_season ma ON ma.team_id=g.away_team_id AND ma.season_year=g.season_year "
             "WHERE g.season_year=? AND mh.is_fbs=1 AND ma.is_fbs=1 "
             "AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL "
             "AND a.off_rush_success_rate IS NOT NULL")
    return conn.execute(query, (season,)).fetchone()[0]


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
        input_sources = {}
        for year, archive in zip(SEASONS, paths):
            fresh = Path(f"data/raw/game_advanced_{year}.csv")
            candidates = [fresh, archive] if has_split_observations(fresh) else [archive]
            used = False
            for path in candidates:
                result = load_game_advanced(scratch, str(path))
                if result["unmatched"] or result["loaded"] <= 0:
                    if path == archive:
                        raise ValueError(f"Invalid archived CFBD research data: {path}: {result}")
                    continue
                if path == fresh:
                    evidence = readiness_report(
                        scratch, (year,), min_paired_fraction=1.0, min_team_fraction=1.0)
                    if not evidence["coverage_gate_passed"]:
                        continue
                if not latest_covered_game(scratch, year):
                    continue
                input_sources[str(year)] = str(path)
                used = True
                break
            if not used:
                raise ValueError(f"No usable advanced source for {year}")
        versions = {}
        for year in SEASONS:
            # Source is season-scoped; never label a future game as observed.
            captured = latest_covered_game(scratch, year)
            if not captured:
                raise ValueError(f"{year}: no completed source-covered FBS games")
            # Strict earlier-date filtering: cutoff is the day AFTER the last
            # source-covered completed matchup. Never imply future coverage.
            source_cutoff = (date.fromisoformat(captured) + timedelta(days=1)).isoformat()
            if as_of and year == max(SEASONS):
                requested = date.fromisoformat(as_of)
                if requested > date.fromisoformat(source_cutoff):
                    raise ValueError("Requested scouting cutoff exceeds archived coverage")
                cutoff = as_of
            else:
                cutoff = source_cutoff
            data = build_descriptive_contract(scratch, season=year, as_of=cutoff)
            data['advanced_source_path'] = input_sources[str(year)]
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
