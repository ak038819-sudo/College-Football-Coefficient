#!/usr/bin/env python3
"""Stage CFBD rushing/passing game data without overwriting committed raw files.

This is a *manual research* command. Requires CFBD_API_KEY. Downloaded rows
are saved to a caller-selected staging directory, never to data/raw, and a
machine-readable coverage report accompanies each staged season. Missing
metrics are reported, not replaced by zero. Review before promoting any data.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from pathlib import Path

try:
    from src.fetch_cfbd_game_advanced import CSV_FIELDS, FIELDS, fetch_game_advanced
except ModuleNotFoundError:
    from fetch_cfbd_game_advanced import CSV_FIELDS, FIELDS, fetch_game_advanced


SPLITS = [c for c, _, _ in FIELDS if c.startswith(
    ("off_rush_", "off_pass_", "def_rush_", "def_pass_"))]


def summarize(rows: list[dict], season: int) -> dict:
    per_metric = {}
    game_sides = defaultdict(set)
    for row in rows:
        gid = row.get("game_id")
        team = row.get("team")
        if gid is not None and team:
            game_sides[gid].add(team)
    for metric in SPLITS:
        valid_games = defaultdict(set)
        for row in rows:
            if row.get(metric) not in (None, "") and row.get("game_id") is not None and row.get("team"):
                valid_games[row["game_id"]].add(row["team"])
        per_metric[metric] = {
            "team_games_reported": sum(len(v) for v in valid_games.values()),
            "games_with_at_least_two_sides": sum(len(v) >= 2 for v in valid_games.values()),
        }
    return {
        "season": season,
        "source": "cfbd_stats_game_advanced",
        "team_game_rows": len(rows),
        "distinct_games": len(game_sides),
        "games_with_at_least_two_sides": sum(len(v) >= 2 for v in game_sides.values()),
        "split_metrics": per_metric,
        "note": "Source-only counts. Canonical FBS membership and dates are audited after staging into a scratch SQLite database.",
    }


def stage_seasons(years: list[int], out_dir: Path, token: str) -> list[dict]:
    if not years or any(y < 2001 for y in years):
        raise ValueError("Stage at least one season from 2001 onward")
    if not token:
        raise ValueError("CFBD_API_KEY required")
    if out_dir.resolve() == (Path("data/raw").resolve()):
        raise ValueError("Refusing to overwrite committed source-data directory")
    out_dir.mkdir(parents=True, exist_ok=True)
    reports = []
    headers = {"Authorization": f"Bearer {token}"}
    for year in years:
        rows = fetch_game_advanced(year, headers)
        if rows is None:
            reports.append({"season": year, "status": "fetch_failed_or_unavailable"})
            continue
        report = summarize(rows, year)
        if not any(v["team_games_reported"] for v in report["split_metrics"].values()):
            report["status"] = "no_split_metrics"
        else:
            report["status"] = "staged_for_review"
        csv_path = out_dir / f"game_advanced_{year}.csv"
        # No partial committed-source overwrite. Stage a temporary file first.
        temp_path = out_dir / f".game_advanced_{year}.csv.tmp"
        with temp_path.open("w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        temp_path.replace(csv_path)
        reports.append(report)
    (out_dir / "split_coverage_report.json").write_text(
        json.dumps(reports, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return reports


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("years", type=int, nargs="+")
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()
    reports = stage_seasons(args.years, Path(args.out_dir),
                            os.environ.get("CFBD_API_KEY", ""))
    print(json.dumps(reports, indent=2))
    if any(r.get("status") != "staged_for_review" for r in reports):
        raise SystemExit("One or more seasons lack verified split metrics; inspect staging report")


if __name__ == "__main__":
    main()
