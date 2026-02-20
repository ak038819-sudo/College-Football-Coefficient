#!/usr/bin/env python3
from __future__ import annotations

import csv
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

# Local import without "src." headaches when running as a script
# (works because build_playoff_field.py sits next to load_team_conference_map.py)
from load_team_conference_map import load_team_conference_map


DB_PATH = "db/league.db"
RATINGS_CSV = "data/processed/team_ratings_by_season.csv"

TOTAL_PLAYOFF_TEAMS = 12
AUTO_BIDS = 5          # top 5 conference champions
BYE_SEEDS = 4          # seeds 1-4 get byes


@dataclass(frozen=True)
class TeamRow:
    team: str
    conf: str
    rating: float


def load_ratings_for_year(csv_path: str, season_year: int) -> Dict[str, float]:
    """
    Reads data/processed/team_ratings_by_season.csv
    Expected columns: season_year, team_name, rating (or similar).
    Defensive with column names.
    """
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"Ratings CSV not found: {csv_path}")

    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("Ratings CSV has no header row.")

        def col(*names: str) -> str:
            lower = {c.lower(): c for c in reader.fieldnames or []}
            for n in names:
                if n.lower() in lower:
                    return lower[n.lower()]
            raise RuntimeError(f"Ratings CSV missing required column. Have: {reader.fieldnames}")

        c_year = col("season_year", "year", "season")
        c_team = col("team_name", "team", "school")
        c_rating = col("rating", "score", "value", "team_rating")

        out: Dict[str, float] = {}
        for row in reader:
            try:
                y = int(row[c_year])
            except Exception:
                continue
            if y != season_year:
                continue

            team = (row.get(c_team) or "").strip()
            if not team:
                continue

            try:
                r = float(row[c_rating])
            except Exception:
                continue

            out[team] = r

    if not out:
        raise RuntimeError(f"No ratings found in {csv_path} for season_year={season_year}")
    return out


def ensure_playoff_table(conn: sqlite3.Connection) -> None:
    """
    Normalized playoff output table (matches your migrated schema).
    """
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS playoff_field_by_year (
          season_year      INTEGER NOT NULL,
          seed             INTEGER NOT NULL,
          team_id          INTEGER NOT NULL,
          conference_id    INTEGER,
          conference_name  TEXT,
          bid_type         TEXT NOT NULL CHECK (bid_type IN ('auto','at_large')),
          rating           REAL NOT NULL,

          PRIMARY KEY (season_year, seed),
          UNIQUE (season_year, team_id),

          FOREIGN KEY (team_id) REFERENCES teams(team_id),
          FOREIGN KEY (conference_id) REFERENCES conferences_new(conference_id)
        );
        """
    )
    conn.commit()


def pick_conference_champs(rows: List[TeamRow]) -> Dict[str, TeamRow]:
    """
    TEMP champ rule (foundation-first):
    champ = highest-rated team in that conference
    """
    champs: Dict[str, TeamRow] = {}
    for tr in rows:
        if not tr.conf:
            continue
        cur = champs.get(tr.conf)
        if cur is None or tr.rating > cur.rating:
            champs[tr.conf] = tr
    return champs


def build_field(rows: List[TeamRow]) -> Tuple[List[Tuple[int, TeamRow, str]], List[TeamRow]]:
    """
    Returns:
      seeded: list of (seed, TeamRow, bid_type)
      byes: list of top-4 champs (TeamRow) in seed order
    """
    champs = pick_conference_champs(rows)
    champ_list = sorted(champs.values(), key=lambda x: x.rating, reverse=True)

    auto = champ_list[:AUTO_BIDS]
    auto_set = {t.team for t in auto}

    remaining = [t for t in sorted(rows, key=lambda x: x.rating, reverse=True) if t.team not in auto_set]
    at_large_needed = TOTAL_PLAYOFF_TEAMS - len(auto)
    at_large = remaining[:at_large_needed]

    field = auto + at_large

    top4_champs = auto[:BYE_SEEDS]
    top4_set = {t.team for t in top4_champs}

    others = [t for t in sorted(field, key=lambda x: x.rating, reverse=True) if t.team not in top4_set]

    seeds: List[Tuple[int, TeamRow, str]] = []
    for i, t in enumerate(top4_champs, start=1):
        seeds.append((i, t, "auto"))

    seed_num = 5
    for t in others:
        bid_type = "auto" if t.team in auto_set else "at_large"
        seeds.append((seed_num, t, bid_type))
        seed_num += 1
        if seed_num > TOTAL_PLAYOFF_TEAMS:
            break

    if len(seeds) != TOTAL_PLAYOFF_TEAMS:
        raise RuntimeError(f"Built {len(seeds)} seeds, expected {TOTAL_PLAYOFF_TEAMS}.")
    return seeds, top4_champs


def print_bracket(seeded: List[Tuple[int, TeamRow, str]]) -> None:
    byes = [(s, t) for s, t, _ in seeded if s <= 4]
    first = {s: t for s, t, _ in seeded if s >= 5}

    print("\nBYES (Round of 16):")
    for s, t in byes:
        print(f"  Seed {s}: {t.team} ({t.rating:.3f}) [{t.conf}]")

    print("\nFIRST ROUND MATCHUPS:")
    pairs = [(5, 12), (6, 11), (7, 10), (8, 9)]
    for hi, lo in pairs:
        a = first.get(hi)
        b = first.get(lo)
        if not a or not b:
            print(f"  Seed {hi} vs Seed {lo}: (missing team data)")
            continue
        print(f"  ({hi}) {a.team} [{a.conf}]  vs  ({lo}) {b.team} [{b.conf}]")


# --------------------------------------------------------
# ID / Name Resolvers
# --------------------------------------------------------

def resolve_team_id(conn: sqlite3.Connection, team_name: str) -> int:
    team_name = (team_name or "").strip()
    if not team_name:
        raise ValueError("resolve_team_id: empty team_name")

    row = conn.execute(
        "SELECT team_id FROM teams WHERE TRIM(team_name) = TRIM(?) LIMIT 1;",
        (team_name,),
    ).fetchone()

    if not row:
        raise KeyError(f"Team not found in teams table: {team_name!r}")

    return int(row[0])


def resolve_conference_id(conn: sqlite3.Connection, conference_name: str | None) -> int | None:
    if not conference_name or not str(conference_name).strip():
        return None

    conf = str(conference_name).strip()
    row = conn.execute(
        "SELECT conference_id FROM conferences_new WHERE TRIM(conference_name) = TRIM(?) LIMIT 1;",
        (conf,),
    ).fetchone()

    return int(row[0]) if row else None


def canonical_team_name(conn: sqlite3.Connection, name: str) -> str:
    """
    If name appears in team_aliases.alias, return canonical team_aliases.team_name.
    Otherwise return name unchanged.
    """
    name = (name or "").strip()
    if not name:
        return name

    row = conn.execute(
        "SELECT team_name FROM team_aliases WHERE TRIM(alias) = TRIM(?) LIMIT 1;",
        (name,),
    ).fetchone()

    return row[0] if row else name


def write_field(conn: sqlite3.Connection, season_year: int, seeded: List[Tuple[int, TeamRow, str]]) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_playoff_table(conn)

    rows = []
    for seed, team, bid_type in seeded:
        team_name = team.team
        rating = team.rating
        conference_name = team.conf

        team_id = resolve_team_id(conn, team_name)
        conference_id = resolve_conference_id(conn, conference_name)

        rows.append(
            (
                season_year,
                int(seed),
                int(team_id),
                conference_id,
                conference_name,
                bid_type,
                float(rating),
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO playoff_field_by_year
          (season_year, seed, team_id, conference_id, conference_name, bid_type, rating)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python src/build_playoff_field.py <season_year> [db_path] [ratings_csv]")
        return 2

    season_year = int(argv[1])
    db_path = argv[2] if len(argv) >= 3 else DB_PATH
    ratings_csv = argv[3] if len(argv) >= 4 else RATINGS_CSV

    ratings = load_ratings_for_year(ratings_csv, season_year)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        team_to_conf = load_team_conference_map(conn, season_year)
    finally:
        conn.close()

    # Build rows with alias->canonical normalization BEFORE conference lookup
    missing = []
    rows: List[TeamRow] = []

    conn = sqlite3.connect(db_path)
    missing = []
    rows: List[TeamRow] = []

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        for raw_team, rating in ratings.items():
            canon = canonical_team_name(conn, raw_team)

            # Conference lookup: canonical first, then raw fallback
            conf = team_to_conf.get(canon, "")
            if not conf:
                conf = team_to_conf.get(raw_team, "")

            if not conf:
                missing.append(raw_team)

            # Keep canonical for downstream consistency / ID resolution
            rows.append(TeamRow(team=canon, conf=conf, rating=rating))
    finally:
        conn.close()

    rows.sort(key=lambda x: x.rating, reverse=True)

    print(f"Teams missing conference mapping (still included): {len(missing)}")
    if missing:
        print("Missing conference mapping teams:")
        for t in missing[:50]:
            print(f"  - {t}")
        if len(missing) > 50:
            print(f"  ... and {len(missing)-50} more")

    if missing:
        raise RuntimeError(
            f"{len(missing)} teams missing conference mapping; refusing to build playoff field."
        )

    seeded, _top4 = build_field(rows)

    print("\nSEEDS:")
    for seed, tr, bid in seeded:
        print(f"{seed:>2}. {tr.team:<25} {tr.rating:>8.3f}   {tr.conf:<16} ({bid})")

    print_bracket(seeded)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        write_field(conn, season_year, seeded)
    finally:
        conn.close()

    print(f"\nWrote playoff_field_by_year for {season_year} into {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))