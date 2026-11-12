#!/usr/bin/env python3
"""Read-only historical coverage audit. Never creates or modifies the source DB."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
YEARS = range(1980, 2027)


def columns(db, table):
    return {r[1] for r in db.execute(f"PRAGMA table_info({table})")}


def exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def audit(db):
    if not exists(db, "games"):
        raise RuntimeError("Missing games table")
    cols = columns(db, "games")
    required = {"season_year", "game_id", "home_team_id", "away_team_id", "home_score", "away_score"}
    if not required <= cols:
        raise RuntimeError(f"Missing games columns: {sorted(required-cols)}")
    rows = []
    for year in YEARS:
        base = db.execute("""
            SELECT COUNT(*), COUNT(DISTINCT game_id),\n                   SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END),
                   SUM(CASE WHEN home_score IS NULL OR away_score IS NULL THEN 1 ELSE 0 END),
                   SUM(CASE WHEN home_team_id IS NULL OR away_team_id IS NULL THEN 1 ELSE 0 END),
                   SUM(CASE WHEN home_team_id = away_team_id THEN 1 ELSE 0 END),
                   SUM(CASE WHEN home_score = away_score AND home_score IS NOT NULL THEN 1 ELSE 0 END)
            FROM games WHERE season_year=?
        """, (year,)).fetchone()
        total, unique, missing_ids, missing_scores, missing_teams, same_team, ties = [int(v or 0) for v in base]
        membership_missing = None
        if exists(db, "team_membership_by_season"):
            mcols = columns(db, "team_membership_by_season")
            if {"season_year", "team_id"} <= mcols:
                membership_missing = db.execute("""
                    SELECT COUNT(*) FROM (
                      SELECT home_team_id AS team_id FROM games WHERE season_year=?
                      UNION SELECT away_team_id FROM games WHERE season_year=?
                    ) t WHERE NOT EXISTS (
                      SELECT 1 FROM team_membership_by_season m
                      WHERE m.season_year=? AND m.team_id=t.team_id
                    )
                """, (year, year, year)).fetchone()[0]
        rows.append(dict(season=year, games=total, distinct_game_ids=unique,
                         duplicate_game_ids=total-unique-missing_ids, missing_game_ids=missing_ids,\n                         missing_scores=missing_scores,
                         missing_team_ids=missing_teams, self_matchups=same_team,
                         ties=ties, teams_without_membership=membership_missing))
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=ROOT / "db/league.db")
    p.add_argument("--out", type=Path, default=ROOT / "data/processed/historical_audit")
    args = p.parse_args()
    if not args.db.is_file():
        p.error(f"Database not found: {args.db}")
    uri = args.db.resolve().as_uri() + "?mode=ro"
    db = sqlite3.connect(uri, uri=True)
    try:
        db.execute("PRAGMA query_only=ON")
        results = audit(db)
    finally:
        db.close()
    args.out.mkdir(parents=True, exist_ok=True)
    csv_path = args.out / "season_coverage.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    manifest = {
        "source_database": str(args.db),
        "source_sha256": hashlib.sha256(args.db.read_bytes()).hexdigest(),
        "coverage": results,
        "notes": [
            "Counts reflect database contents, not completeness against CFBD.",
            "Duplicate game IDs exclude missing IDs, which are counted separately.",
            "Missing team aliases discarded by the existing loader cannot be recovered from the games table.",
            "2020 is included in the audit; do not assume it is missing.",
        ],
    }
    (args.out / "season_coverage.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Audited {len(results)} seasons; results: {args.out}")


if __name__ == "__main__":
    main()
