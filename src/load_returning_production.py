#!/usr/bin/env python3
"""Load the committed returning-production archive into team_returning_production.

WHY THIS EXISTS. `data/raw/returning_production_*.csv` has held thirteen
seasons -- 1,691 team-seasons, 2014-2026 -- since the Elo preseason experiment
fetched it. Nothing read those files afterwards except
`src/calibrate_returning_retention.py`, the script that measured the feed and
led to REJECTING it as a rating input, and there was no table for it at all.
The decision was to keep it as its own measurement; this is the loader that
lets a reader see it.

NOTHING THIS WRITES IS A RATING INPUT. `offseason_retention` stays the flat
0.8 the Elo configuration already carries. See sql/returning_tables.sql.

AN UNRESOLVED SCHOOL IS A DEFECT HERE, not a normal answer. This endpoint
covers FBS only, so a name the identity map cannot place means the map is
wrong, and the loader refuses the file rather than writing a row with no team
or quietly dropping one. That is the opposite of src/load_recruiting.py's
rule, and the difference is deliberate -- that endpoint covers all of college
football, where an unplaceable school is simply a school this database does
not carry.

Idempotent: the key is (season, team), so re-running the same archive updates
the same rows.

Usage:
    python src/load_returning_production.py --db db/league.db
    python src/load_returning_production.py --db db/league.db --raw-dir data/raw
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import team_identity

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "returning_tables.sql"
DEFAULT_RAW = Path("data/raw")
DEFAULT_DB = Path("db/league.db")
FILE_PREFIX = "returning_production_"

# Every metric column, in the order the table declares them. Named once so the
# INSERT, the CSV read and the test all agree without three copies to drift.
METRICS = ("total_ppa", "total_passing_ppa", "total_receiving_ppa", "total_rushing_ppa",
           "percent_ppa", "percent_passing_ppa", "percent_receiving_ppa",
           "percent_rushing_ppa", "usage", "passing_usage", "receiving_usage",
           "rushing_usage")


def _float(value):
    """A number, or None for a blank. Blank is 'not reported'; 0 is a team that
    returned nothing, and the two must not become the same row."""
    text = (value or "").strip()
    return None if text == "" else float(text)


def season_of(path: Path) -> int:
    stem = path.stem
    if not stem.startswith(FILE_PREFIX):
        raise ValueError(f"{path} is not a {FILE_PREFIX}* file")
    season = stem[len(FILE_PREFIX):]
    if not season.isdigit():
        raise ValueError(f"{path} has no season in its name")
    return int(season)


def team_resolver(cur):
    """Resolve a provider school name to a team_id, memoised. Raises on a name
    this database cannot place -- see the module docstring for why."""
    cache: dict[str, int] = {}
    by_name = {name: team_id for team_id, name
               in cur.execute("SELECT team_id, team_name FROM teams")}

    def resolve(raw: str, where: str) -> int:
        name = (raw or "").strip()
        if not name:
            raise ValueError(f"{where}: a returning-production row has no team name")
        if name not in cache:
            resolved = team_identity.resolve_database_name(cur, name)
            team_id = by_name.get(resolved) if resolved else None
            if team_id is None:
                raise ValueError(
                    f"{where}: '{name}' does not resolve to a team in this database. "
                    "This feed covers FBS only, so an unresolved school is a gap in "
                    "data/identity, not a school we do not carry. Add the alias "
                    "rather than letting the row be dropped.")
            cache[name] = team_id
        return cache[name]

    return resolve


def load_file(conn: sqlite3.Connection, path: Path) -> int:
    """One season's file. Returns the number of rows written."""
    season = season_of(path)
    cur = conn.cursor()
    resolve = team_resolver(cur)
    written = 0
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if int(row["season_year"]) != season:
                raise ValueError(
                    f"{path}: a row says season {row['season_year']}. The file name "
                    "decides which season production returns into; they must agree.")
            team_id = resolve(row["team"], str(path))
            values = [_float(row.get(metric)) for metric in METRICS]
            cur.execute(
                "INSERT INTO team_returning_production "
                "(season_year, team_id, source_team, conference, " + ", ".join(METRICS) + ") "
                "VALUES (?, ?, ?, ?, " + ", ".join("?" * len(METRICS)) + ") "
                "ON CONFLICT(season_year, team_id) DO UPDATE SET source_team=excluded.source_team, "
                "conference=excluded.conference, "
                + ", ".join(f"{m}=excluded.{m}" for m in METRICS),
                [season, team_id, row["team"], (row.get("conference") or "").strip() or None,
                 *values])
            written += 1
    conn.commit()
    return written


def load_archive(conn: sqlite3.Connection, raw_dir: Path) -> tuple[int, int]:
    """Every committed season. Returns (files, rows)."""
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    files = sorted(Path(raw_dir).glob(f"{FILE_PREFIX}*.csv"))
    return len(files), sum(load_file(conn, path) for path in files)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW))
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    try:
        files, rows = load_archive(conn, Path(args.raw_dir))
    finally:
        conn.close()
    print(f"returning production: {rows:,} team-seasons from {files} files")


if __name__ == "__main__":
    main()
