#!/usr/bin/env python3
"""Conservative evidence gate for research scouting data.

A passing coverage check is *necessary but never sufficient*: it cannot
establish historical point-in-time correctness or prediction accuracy.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

try:
    from src.audit_matchup_coverage import audit_coverage
except ModuleNotFoundError:
    from audit_matchup_coverage import audit_coverage

SPLIT_METRICS = (
    "rush_success_rate", "rush_ppa", "rush_explosiveness",
    "pass_success_rate", "pass_ppa", "pass_explosiveness",
)


def readiness_report(conn: sqlite3.Connection, seasons: tuple[int, ...],
                     min_paired_fraction: float = 0.90,
                     min_team_fraction: float = 0.95) -> dict:
    if not seasons or any(s < 2001 for s in seasons):
        raise ValueError("Select seasons covered by the advanced-game endpoint")
    if not (0 < min_paired_fraction <= 1 and 0 < min_team_fraction <= 1):
        raise ValueError("Coverage thresholds must be in (0,1]")
    reports = {}
    blockers = []
    for year in seasons:
        audit = audit_coverage(conn, year)
        reports[str(year)] = audit
        total = audit["completed_fbs_games"]
        if total == 0:
            blockers.append(f"{year}: no completed eligible FBS games")
            continue
        if audit["missing_membership_games"]:
            blockers.append(
                f"{year}: {audit['missing_membership_games']} completed games lack membership")
        for metric in SPLIT_METRICS:
            m = audit["metrics"][metric]
            paired = m["paired_games"] / total
            observed = m["team_games"] / (2 * total)
            if m["status"] != "available":
                blockers.append(f"{year}/{metric}: schema column missing")
            elif paired < min_paired_fraction or observed < min_team_fraction:
                blockers.append(
                    f"{year}/{metric}: paired={paired:.1%}, team-sides={observed:.1%}, "
                    f"required={min_paired_fraction:.1%}/{min_team_fraction:.1%}")
    return {
        "coverage_gate_passed": not blockers,
        "production_ready": False,
        "note": "Coverage alone cannot establish model skill, no leakage or reliability.",
        "thresholds": {
            "min_paired_games_fraction": min_paired_fraction,
            "min_team_game_sides_fraction": min_team_fraction,
        },
        "blockers": blockers,
        "seasons": reports,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, action="append", required=True)
    p.add_argument("--min-paired", type=float, default=0.90)
    p.add_argument("--min-sides", type=float, default=0.95)
    args = p.parse_args()
    path = Path(args.db).resolve()
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as conn:
        result = readiness_report(conn, tuple(args.season),
                                  args.min_paired, args.min_sides)
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["coverage_gate_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
