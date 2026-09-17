#!/usr/bin/env python3
"""
Bootstraps a fresh db/league.db from:
  1) sql/schema.sql (core entity tables)
  2) teams + team_aliases + team_membership_by_season, seeded from the
     existing db/league_backup_before_playoff_migration.db (which already
     has 138 teams and 2014-2025 conference membership from a prior CFBD pull)
  3) A small set of manually-triaged additions/aliases needed to resolve
     every team name that appears in data/raw/games_2010.csv .. games_2025.csv
     but wasn't in that backup's 138 teams -- found by diffing CSV team
     names against the backup's teams+aliases. See NEW_TEAMS / NEW_ALIASES
     below for exactly what and why.

Does NOT populate team_membership_by_season for 2010-2013 -- the backup
only has 2014-2025 (from a live CFBD API pull this project doesn't have
credentials/network to redo here). This means build_coefficients.py's
per-team ratings work fine back to 2010, but the conference-level rollup
(which depends on team_membership_by_season) will simply have no data
for 2010-2013 -- teams without a membership row that year are silently
excluded from any conference sum for that year, not an error.

Usage:
    python src/bootstrap_league_db.py \
        --backup db/league_backup_before_playoff_migration.db \
        --out db/league.db
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

# Genuinely-missing FBS teams (verified against the backup's 138 teams --
# not resolvable via alias to anything that already exists).
# Idaho: FBS through 2017, dropped to FCS in 2018 -- absent from a
#   2014+-only CFBD teams pull is plausible if that pull was scoped
#   to post-transition years, but Idaho appears as FBS in earlier data.
# Massachusetts (UMass): FBS independent, should exist regardless of year.
NEW_TEAMS = ["Idaho", "Massachusetts"]

# alias -> canonical team_name already in the backup's teams table
NEW_ALIASES = {
    "App State": "Appalachian State",
    "Florida Atlantic": "FAU",
    "Florida International": "FIU",
    "UL Monroe": "ULM",
}


def build_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    conn.executescript(schema_path.read_text())


def copy_table(src: sqlite3.Connection, dst: sqlite3.Connection, table: str, columns: str) -> int:
    rows = src.execute(f"SELECT {columns} FROM {table}").fetchall()
    if not rows:
        return 0
    placeholders = ",".join("?" for _ in columns.split(","))
    dst.executemany(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows)
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--backup", default="db/league_backup_before_playoff_migration.db")
    p.add_argument("--out", default="db/league.db")
    p.add_argument("--schema", default="sql/schema.sql")
    args = p.parse_args()

    out_path = Path(args.out)
    if out_path.exists():
        raise SystemExit(f"{out_path} already exists -- delete it first if you want a clean rebuild.")

    src = sqlite3.connect(args.backup)
    dst = sqlite3.connect(str(out_path))

    build_schema(dst, Path(args.schema))

    n_teams = copy_table(src, dst, "teams", "team_id, team_name, short_name, state")
    n_aliases = copy_table(src, dst, "team_aliases", "alias, team_name")
    n_membership = copy_table(
        src, dst, "team_membership_by_season", "team_id, season_year, conference_real, is_fbs"
    )
    dst.commit()

    print(f"Copied from backup: {n_teams} teams, {n_aliases} aliases, {n_membership} membership rows")

    # Add genuinely-missing teams
    cur = dst.cursor()
    added_teams = 0
    for name in NEW_TEAMS:
        cur.execute("SELECT 1 FROM teams WHERE team_name = ?", (name,))
        if not cur.fetchone():
            cur.execute("INSERT INTO teams (team_name) VALUES (?)", (name,))
            added_teams += 1
    dst.commit()
    print(f"Added {added_teams} missing teams: {NEW_TEAMS}")

    # Add new aliases (only if the canonical target actually exists)
    added_aliases = 0
    for alias, canonical in NEW_ALIASES.items():
        cur.execute("SELECT 1 FROM teams WHERE team_name = ?", (canonical,))
        if not cur.fetchone():
            print(f"  WARNING: alias target '{canonical}' not found in teams -- skipping alias '{alias}'")
            continue
        cur.execute("SELECT 1 FROM team_aliases WHERE alias = ?", (alias,))
        if not cur.fetchone():
            cur.execute("INSERT INTO team_aliases (alias, team_name) VALUES (?, ?)", (alias, canonical))
            added_aliases += 1
    dst.commit()
    print(f"Added {added_aliases} new aliases: {list(NEW_ALIASES.keys())}")

    src.close()
    dst.close()
    print(f"\nBootstrapped {out_path}")


if __name__ == "__main__":
    main()
