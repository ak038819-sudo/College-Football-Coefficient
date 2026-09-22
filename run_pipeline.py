#!/usr/bin/env python3
"""
Runs the ENTIRE College Football Coefficient pipeline in one command,
from a clean bootstrap through a full playoff bracket draw for one
season. This replaces ~30 manual commands with one script.

What it does, in order:
  1. Bootstraps db/league.db from the backup DB + schema (skipped if
     db/league.db already exists, unless --force)
  2. Applies the standings schema + v_games_enriched view
  3. Loads all games CSVs (data/raw/games_2010.csv .. games_2025.csv)
  4. Patches known conference-membership gaps (Miami (FL), FAU, etc.)
  5. Runs the iterative rating model (team + conference ratings, all
     years) -- writes data/processed/*.csv
  6. Computes conference team records and derives conference standings
     for every season 2014-2025 (the years with membership data)
  7. Builds and prints the playoff field + Round-of-24 bracket draw
     for one season (--year, default 2025)

Usage:
    python run_pipeline.py                      # full run, playoff field for 2025
    python run_pipeline.py --year 2014           # full run, playoff field for 2014
    python run_pipeline.py --force               # wipe db/league.db and rebuild from scratch
    python run_pipeline.py --skip-load           # if db/league.db is already fully loaded,
                                                  # skip straight to ratings + playoff field
    python run_pipeline.py --draw-seed 7         # use a specific bracket draw seed
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import datetime

DB_PATH = Path("db/league.db")
BACKUP_PATH = Path("db/league_backup_before_playoff_migration.db")
CURRENT_YEAR = datetime.date.today().year
# Team RATINGS only need game results, not conference membership -- so this
# can go back much further than MEMBERSHIP_SEASONS below, which is bounded
# by when real conference-membership data actually exists (2014-). Extended
# to 1980 once tie-handling was solved (see build_elo.py's run_elo() and
# build_hybrid_coefficients.py's tie_game_coe() -- NCAA football had no
# overtime rule until 1996, so pre-1996 games can genuinely end in a tie,
# which the Elo/hybrid layers now handle correctly instead of just
# assuming ties can't happen). fetch_cfbd_games.py will just return fewer
# rows for thinner years rather than erroring, so this is safe to try
# further back if you're curious.
EARLIEST_GAME_YEAR = 1980
SEASONS = list(range(EARLIEST_GAME_YEAR, CURRENT_YEAR + 1))
MEMBERSHIP_SEASONS = list(range(2014, CURRENT_YEAR + 1))  # only years with conference membership


def run(cmd: list[str], label: str) -> None:
    print(f"\n>>> {label}")
    print(f"    $ {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\nFAILED at step: {label}")
        sys.exit(result.returncode)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--year", type=int, default=2025, help="Season to build the playoff field/bracket for")
    p.add_argument("--draw-seed", type=int, default=1, help="Bracket draw seed (reproducible)")
    p.add_argument("--force", action="store_true", help="Delete db/league.db and rebuild from scratch")
    p.add_argument("--skip-load", action="store_true", help="Skip bootstrap/schema/game-loading steps entirely")
    args = p.parse_args()

    python = sys.executable

    if args.force and DB_PATH.exists():
        print(f">>> --force: removing {DB_PATH}")
        DB_PATH.unlink()

    if not args.skip_load:
        if not DB_PATH.exists():
            if not BACKUP_PATH.exists():
                sys.exit(f"ERROR: {BACKUP_PATH} not found -- can't bootstrap without it.")
            run(
                [python, "src/bootstrap_league_db.py", "--backup", str(BACKUP_PATH), "--out", str(DB_PATH)],
                "Bootstrap db/league.db (teams, aliases, membership)",
            )
        else:
            print(f"\n>>> {DB_PATH} already exists, skipping bootstrap (use --force to rebuild)")

        run(
            [
                python,
                "-c",
                "import sqlite3; c=sqlite3.connect('db/league.db'); "
                "c.executescript(open('sql/standings_tables.sql').read()); "
                "c.executescript(open('sql/views/v_games_enriched.sql').read()); "
                "c.commit(); print('Applied standings schema + view')",
            ],
            "Apply standings schema + v_games_enriched view",
        )

        for year in SEASONS:
            csv_path = Path(f"data/raw/games_{year}.csv")
            if csv_path.exists():
                run([python, "src/load_games.py", str(csv_path)], f"Load games: {year}")

        run(
            [python, "src/patch_known_membership_gaps.py", "--db", str(DB_PATH)],
            "Patch known conference-membership gaps",
        )

        for snapshot_path in sorted(Path("data/raw").glob("membership_*.csv")):
            year_str = snapshot_path.stem.replace("membership_", "")
            if year_str.isdigit():
                run(
                    [python, "src/load_membership_snapshot.py", str(snapshot_path), "--db", str(DB_PATH), "--year", year_str],
                    f"Load committed membership snapshot: {year_str}",
                )
    else:
        print(">>> --skip-load: assuming db/league.db is already bootstrapped and loaded")

    run([python, "src/build_coefficients.py"], "Compute iterative team + conference ratings (all years)")

    for year in MEMBERSHIP_SEASONS:
        run(
            [python, "src/coefficients/compute_conference_team_records.py", "--year", str(year), "--no-validation"],
            f"Compute conference team records: {year}",
        )
    for year in MEMBERSHIP_SEASONS:
        run(
            [python, "src/coefficients/derive_conference_standings_local.py", "--year", str(year)],
            f"Derive conference standings: {year}",
        )

    print(f"\n{'='*70}")
    print(f"PLAYOFF FIELD + BRACKET DRAW FOR {args.year}")
    print(f"{'='*70}")
    subprocess.run([python, "src/coefficients/select_playoff_field_v2.py", "--year", str(args.year)])
    subprocess.run(
        [python, "src/coefficients/draw_playoff_bracket_v2.py", "--year", str(args.year), "--draw-seed", str(args.draw_seed)]
    )

    print(f"\n{'='*70}")
    print("PIPELINE COMPLETE")
    print(f"{'='*70}")
    print("Outputs written to data/processed/:")
    print("  team_ratings_by_season.csv, team_coeff_5yr.csv,")
    print("  conference_ratings_by_season.csv, conference_coeff_5yr.csv")


if __name__ == "__main__":
    main()
