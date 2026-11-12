#!/usr/bin/env python3
"""Report integrity and provenance of a historical staging import; no writes."""
import argparse
import csv
import json
import sqlite3
from pathlib import Path


def report(db, years):
    seasons = []
    errors = []
    for year in years:
        for kind in ("regular", "postseason"):
            checkpoint = db.execute(
                "SELECT status,record_count,error FROM archive_fetches WHERE season=? AND season_type=?",
                (year, kind)).fetchone()
            count, distinct_ids, missing_names, missing_scores, ties, missing_class = db.execute(
                """SELECT COUNT(*), COUNT(DISTINCT source_game_id),
                          COALESCE(SUM(CASE WHEN home_name_raw IS NULL OR away_name_raw IS NULL THEN 1 ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN home_score IS NULL OR away_score IS NULL THEN 1 ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN home_score=away_score AND home_score IS NOT NULL THEN 1 ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN home_classification IS NULL OR away_classification IS NULL THEN 1 ELSE 0 END),0)
                   FROM archive_games WHERE season=? AND season_type=?""",
                (year, kind)).fetchone()
            status, expected, err = checkpoint if checkpoint else ("missing", None, None)
            row = dict(season=year, season_type=kind, status=status,
                       expected=expected, actual=count, unique_ids=distinct_ids,
                       missing_names=missing_names, missing_scores=missing_scores,
                       ties=ties, unknown_classification=missing_class, error=err)
            seasons.append(row)
            if status != "complete" or count != expected or count != distinct_ids:
                errors.append(f"{year} {kind}: incomplete checkpoint or count mismatch")
            if count == 0:
                errors.append(f"{year} {kind}: empty response requires independent coverage review")
            if missing_names:
                errors.append(f"{year} {kind}: {missing_names} games missing team names")
    bad_hashes = 0
    import hashlib
    for payload, digest in db.execute("SELECT source_payload_json,payload_sha256 FROM archive_games"):
        if hashlib.sha256(payload.encode()).hexdigest() != digest:
            bad_hashes += 1
    if bad_hashes:
        errors.append(f"{bad_hashes} source payload hash mismatches")
    return dict(seasons=seasons, errors=errors, passed=not errors,
                warning="A complete empty API response is not proof of complete historical coverage.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    if a.end < a.start:
        p.error("--end must be >= --start")
    a.out.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{a.db.resolve()}?mode=ro", uri=True) as db:
        result = report(db, range(a.start, a.end + 1))
    (a.out / "pilot_report.json").write_text(json.dumps(result, indent=2) + "\n")
    with (a.out / "pilot_seasons.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=result["seasons"][0].keys())
        writer.writeheader()
        writer.writerows(result["seasons"])
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
