#!/usr/bin/env python3
"""Read-only 1946–1979 historical backfill readiness report.

Consumes the isolated archive_games/archive_fetches schema from Phase 2.
Never modifies production league.db or generates Elo/Merits.
"""
import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

FIRST, LAST = 1946, 1979


def audit(db, first=FIRST, last=LAST):
    if first < FIRST or last > LAST or last < first:
        raise ValueError("Phase 3 range must be within 1946–1979")
    results = []
    for year in range(first, last + 1):
        for kind in ("regular", "postseason"):
            checkpoint = db.execute(
                "SELECT status, record_count FROM archive_fetches WHERE season=? AND season_type=?",
                (year, kind)).fetchone()
            rows = db.execute(
                """SELECT source_game_id, home_name_raw, away_name_raw,
                          home_score, away_score, home_classification,
                          away_classification, start_date
                   FROM archive_games WHERE season=? AND season_type=?""",
                (year, kind)).fetchall()
            seen = set()
            duplicates = 0
            missing_id = missing_teams = missing_scores = missing_date = ties = unknown_class = 0
            for gid, home, away, hs, aws, hc, ac, date in rows:
                if not gid:
                    # A blank id is already counted as missing; never let two of
                    # them read as a duplicate pair as well.
                    missing_id += 1
                else:
                    if gid in seen:
                        duplicates += 1
                    seen.add(gid)
                missing_teams += int(not home or not away or home == away)
                missing_scores += int(hs is None or aws is None)
                missing_date += int(not date)
                ties += int(hs is not None and aws is not None and hs == aws)
                unknown_class += int(not hc or not ac)
            status, expected = checkpoint if checkpoint else ("missing", None)
            reasons = []
            if status != "complete":
                reasons.append("checkpoint_incomplete")
            if expected != len(rows):
                reasons.append("source_count_mismatch")
            if duplicates or missing_id:
                reasons.append("invalid_source_ids")
            if missing_teams:
                reasons.append("missing_or_invalid_teams")
            if kind == "regular" and not rows:
                reasons.append("empty_regular_season_requires_source_review")
            results.append(dict(
                season=year, season_type=kind, checkpoint=status,
                fetched_count=expected, staged_count=len(rows),
                duplicate_ids=duplicates, missing_ids=missing_id,
                missing_or_invalid_teams=missing_teams,
                missing_scores=missing_scores, missing_dates=missing_date,
                ties=ties, unknown_classification=unknown_class,
                ready_for_source_reconciliation=not reasons,
                blockers=";".join(reasons),
            ))
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--start", type=int, default=FIRST)
    p.add_argument("--end", type=int, default=LAST)
    args = p.parse_args()
    if args.db.resolve().name == "league.db":
        p.error("Refusing production database")
    args.out.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{args.db.resolve()}?mode=ro", uri=True) as db:
        rows = audit(db, args.start, args.end)
    with (args.out / "backfill_readiness.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "years": [args.start, args.end],
        "total_staged_games": sum(r["staged_count"] for r in rows),
        "blocked_partitions": sum(bool(r["blockers"]) for r in rows),
        "missing_scores": sum(r["missing_scores"] for r in rows),
        "missing_dates": sum(r["missing_dates"] for r in rows),
        "ties": sum(r["ties"] for r in rows),
        "unknown_classification": sum(r["unknown_classification"] for r in rows),
        "production_promotion_allowed": False,
        "note": "Structural checks are not proof of complete historical coverage or major-college eligibility.",
    }
    (args.out / "backfill_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
