#!/usr/bin/env python3
"""Audit real matchup data availability without modifying the league database.

Reports completed FBS games, overall and per-metric observed team-games,
paired game coverage and missing FBS membership. The report is not evidence
that projections are calibrated.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

try:
    from src.build_matchup_intelligence import METRICS
except ModuleNotFoundError:
    from build_matchup_intelligence import METRICS


def audit_coverage(conn: sqlite3.Connection, season: int) -> dict:
    required = {"games", "team_membership_by_season", "game_team_advanced"}
    present = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = required - present
    if missing:
        raise ValueError(f"Missing tables: {sorted(missing)}")
    cols = {row[1] for row in conn.execute("PRAGMA table_info(game_team_advanced)")}
    games = conn.execute("""
        SELECT g.game_id, g.home_team_id, g.away_team_id,
               mh.is_fbs, ma.is_fbs
        FROM games g
        LEFT JOIN team_membership_by_season mh
          ON mh.team_id=g.home_team_id AND mh.season_year=g.season_year
        LEFT JOIN team_membership_by_season ma
          ON ma.team_id=g.away_team_id AND ma.season_year=g.season_year
        WHERE g.season_year=? AND g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
    """, (season,)).fetchall()
    missing_membership = sum(1 for _, _, _, h, a in games if h is None or a is None)
    eligible = {g: (h, a) for g, h, a, hf, af in games if hf == 1 and af == 1}
    raw = conn.execute("""
        SELECT game_id, team_id, season_year FROM game_team_advanced
        WHERE season_year=?
    """, (season,)).fetchall()
    recorded = defaultdict(set)
    foreign_or_unmatched = 0
    for gid, tid, row_season in raw:
        sides = eligible.get(gid)
        if sides is None or tid not in sides:
            foreign_or_unmatched += 1
        else:
            recorded[gid].add(tid)
    metric_report = {}
    for name, column in METRICS.items():
        if column not in cols:
            metric_report[name] = {"status": "schema_missing", "team_games": 0,
                                   "paired_games": 0}
            continue
        vals = conn.execute(
            f"SELECT game_id, team_id FROM game_team_advanced "
            f"WHERE season_year=? AND {column} IS NOT NULL", (season,)
        ).fetchall()
        by_game = defaultdict(set)
        for gid, tid in vals:
            if gid in eligible and tid in eligible[gid]:
                by_game[gid].add(tid)
        metric_report[name] = {
            "status": "available",
            "team_games": sum(len(v) for v in by_game.values()),
            "paired_games": sum(
                eligible[g][0] in v and eligible[g][1] in v
                for g, v in by_game.items()),
            "eligible_team_game_slots": len(eligible) * 2,
            "eligible_games": len(eligible),
        }
    return {
        "season": season,
        "completed_games": len(games),
        "missing_membership_games": missing_membership,
        "completed_fbs_games": len(eligible),
        "advanced_team_games_recorded": sum(len(v) for v in recorded.values()),
        "advanced_games_with_both_sides": sum(len(v)==2 for v in recorded.values()),
        "advanced_rows_unmatched_to_fbs_game_side": foreign_or_unmatched,
        "metrics": metric_report,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--output", help="optional JSON file; default stdout only")
    args = p.parse_args()
    db = Path(args.db).resolve()
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as conn:
        report = audit_coverage(conn, args.season)
    result = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(result + "\n", encoding="utf-8")
    print(result)


if __name__ == "__main__":
    main()
