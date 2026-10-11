#!/usr/bin/env python3
"""Load the committed recruiting archive into recruiting_classes / recruits.

WHAT THIS IS FOR. Everything the site says about a coach measures what he did
with what he had, and nothing measures what he had. These two tables are the
other half: the class rank and point total his program signed each season, and
every recruit in it with a hometown that can be put on a map.

THE SCHOOL NAME IS THE ONLY JOIN CFBD OFFERS, and a quarter of recruiting
classes belong to schools this database does not carry -- Harvard, Eastern
Washington, Northern Iowa. So resolution is deliberately two-tiered:

  - A name the reviewed registry or the database knows resolves to a team_id.
  - A name neither knows is NOT an error and NOT a dropped row. It is stored
    with team_id NULL and the provider's own string kept beside it, and the
    run prints how many there were.

That second rule is the opposite of the one the returning-production fetcher
uses, and the difference is the point: that endpoint covers FBS only, so an
unresolved name there IS a defect. This one covers all of college football, so
failing on an FCS school would refuse the data for being complete. What both
rules share is that nothing is skipped silently.

Idempotent: both tables are keyed on identifiers the provider assigns, so
re-running the same archive updates the same rows.

Usage:
    python src/load_recruiting.py --db db/league.db
    python src/load_recruiting.py --db db/league.db --raw-dir data/raw
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import team_identity

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "recruiting_tables.sql"
DEFAULT_RAW = Path("data/raw")
DEFAULT_DB = Path("db/league.db")


def _int(value):
    """An integer, or None for a blank. Blank is 'not reported', never 0."""
    text = (value or "").strip()
    if text == "":
        return None
    return int(float(text))


def _float(value):
    text = (value or "").strip()
    return None if text == "" else float(text)


def _text(value):
    text = (value or "").strip()
    return None if text == "" else text


def team_resolver(cur):
    """Resolve a provider school name to a team_id, memoised.

    Returns None for a school this database does not carry, which is a normal
    answer here rather than a failure.
    """
    cache: dict[str, int | None] = {}
    by_name = {name: team_id for team_id, name
               in cur.execute("SELECT team_id, team_name FROM teams")}

    def resolve(raw):
        name = (raw or "").strip()
        if not name:
            return None
        if name not in cache:
            resolved = team_identity.resolve_database_name(cur, name)
            cache[name] = by_name.get(resolved) if resolved else None
        return cache[name]

    return resolve


def season_of(path, prefix):
    stem = path.stem
    if not stem.startswith(prefix):
        raise ValueError(f"{path} is not a {prefix}* file")
    season = stem[len(prefix):]
    if not season.isdigit():
        raise ValueError(f"{path} has no season in its name")
    return int(season)


def load_classes(conn, raw_dir):
    """Returns (rows written, rows whose school this database does not carry)."""
    cur = conn.cursor()
    resolve = team_resolver(cur)
    written = unplaced = 0
    for path in sorted(Path(raw_dir).glob("recruiting_teams_*.csv")):
        season = season_of(path, "recruiting_teams_")
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if int(row["season_year"]) != season:
                    raise ValueError(
                        f"{path}: row says season {row['season_year']}. The file name "
                        "decides which season a class belongs to; they must agree.")
                team_id = resolve(row["team"])
                unplaced += team_id is None
                cur.execute(
                    "INSERT INTO recruiting_classes "
                    "(source_team, season_year, team_id, class_rank, points) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(source_team, season_year) DO UPDATE SET "
                    "team_id=excluded.team_id, class_rank=excluded.class_rank, "
                    "points=excluded.points",
                    (row["team"], season, team_id,
                     _int(row["rank"]), _float(row["points"])))
                written += 1
    conn.commit()
    return written, unplaced


def load_recruits(conn, raw_dir):
    """Returns (rows written, rows with no resolvable committed team)."""
    cur = conn.cursor()
    resolve = team_resolver(cur)
    written = unplaced = 0
    for path in sorted(Path(raw_dir).glob("recruiting_players_*.csv")):
        season = season_of(path, "recruiting_players_")
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if int(row["season_year"]) != season:
                    raise ValueError(
                        f"{path}: row says season {row['season_year']}. The file name "
                        "decides which season a recruit belongs to; they must agree.")
                if not _text(row["recruit_id"]):
                    raise ValueError(f"{path}: a recruit has no id")
                committed = resolve(row["committed_to"])
                unplaced += committed is None
                cur.execute(
                    "INSERT INTO recruits (recruit_id, season_year, athlete_id, name, "
                    "position, recruit_type, height, weight, stars, rating, "
                    "national_rank, committed_team_id, committed_to, high_school, "
                    "city, state_province, country, hometown_latitude, "
                    "hometown_longitude, hometown_county_fips) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(recruit_id) DO UPDATE SET "
                    "season_year=excluded.season_year, athlete_id=excluded.athlete_id, "
                    "name=excluded.name, position=excluded.position, "
                    "recruit_type=excluded.recruit_type, height=excluded.height, "
                    "weight=excluded.weight, stars=excluded.stars, "
                    "rating=excluded.rating, national_rank=excluded.national_rank, "
                    "committed_team_id=excluded.committed_team_id, "
                    "committed_to=excluded.committed_to, "
                    "high_school=excluded.high_school, city=excluded.city, "
                    "state_province=excluded.state_province, country=excluded.country, "
                    "hometown_latitude=excluded.hometown_latitude, "
                    "hometown_longitude=excluded.hometown_longitude, "
                    "hometown_county_fips=excluded.hometown_county_fips",
                    (row["recruit_id"], season, _int(row["athlete_id"]),
                     row["name"], _text(row["position"]), _text(row["recruit_type"]),
                     _int(row["height"]), _int(row["weight"]), _int(row["stars"]),
                     _float(row["rating"]), _int(row["national_rank"]),
                     committed, _text(row["committed_to"]), _text(row["school"]),
                     _text(row["city"]), _text(row["state_province"]),
                     _text(row["country"]), _float(row["hometown_latitude"]),
                     _float(row["hometown_longitude"]),
                     _text(row["hometown_county_fips"])))
                written += 1
    conn.commit()
    return written, unplaced


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW))
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    classes, class_unplaced = load_classes(conn, args.raw_dir)
    recruits, recruit_unplaced = load_recruits(conn, args.raw_dir)
    located = conn.execute(
        "SELECT COUNT(*) FROM recruits WHERE hometown_latitude IS NOT NULL").fetchone()[0]
    conn.close()
    print(f"Loaded {classes} recruiting class(es), {class_unplaced} for schools "
          f"this database does not carry.")
    print(f"Loaded {recruits} recruit(s), {recruit_unplaced} with no resolvable "
          f"commitment, {located} with hometown coordinates.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
