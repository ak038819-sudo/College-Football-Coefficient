#!/usr/bin/env python3
"""
Compares CoE v1 (build_coefficients.py, the live system) against CoE 2.0
/ Game CoE 2.0 (build_hybrid_coefficients.py) at the team-season level.

Per the design doc's own philosophy (section 32): a disagreement between
the two models is NOT automatically a bug to fix. Elo-strong-but-CoE-light
can mean "dominant team that hasn't racked up signature wins yet";
CoE-strong-but-Elo-light can mean "not overwhelming, but an outstanding
resume." This script surfaces disagreements for inspection -- it does
not judge which model is "right" for a given team.

Scope (deliberately v1 of this comparison): team-level Season CoE only.
Conference-level CoE 2.0 (external-games-only, per spec section 16) is a
natural follow-up, not built here yet -- keeping this first pass focused
and tractable rather than doing everything at once.

Filters out any team-season with fewer than CONFIDENCE_GAMES games played
that season (same threshold already established for CoE v1's own
early-season confidence blending -- reused here for consistency, not
re-derived). Without this, comparing a raw cumulative Season CoE 2.0 sum
(which has NO early-season adjustment yet, unlike v1's already-blended
values) against a team with only 1-2 games played produces wild, entirely
spurious "disagreements" that look like interesting signal but are really
just an artifact of comparing a full season against a fraction of one --
confirmed on real data: Indiana and Massachusetts both showed 70+ rank
swings in the 2026 season, and both turned out to have played exactly 1
game. This also correctly handles 2020's uneven COVID-shortened schedules
(some teams played as few as 3 games that year), not just the current
in-progress season -- filtering by actual games played generalizes better
than excluding a hardcoded "current year".

Usage:
    python src/compare_models.py [--db db/league.db] [--team-ratings-csv data/processed/team_ratings_by_season.csv] [--out data/processed/model_comparison.csv]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict

CONFIDENCE_GAMES = 8  # matches build_coefficients.py's own threshold


def load_v1_ratings(csv_path: str) -> dict:
    ratings = {}
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            ratings[(int(row["season_year"]), row["team_name"])] = float(row["rating"])
    return ratings


def load_games_played(conn: sqlite3.Connection) -> dict:
    """Returns {(season_year, team_name): games_played}, counting only games with a recorded score."""
    rows = conn.execute(
        """
        SELECT g.season_year, t.team_name, COUNT(*) as n
        FROM games g
        JOIN teams t ON t.team_id IN (g.home_team_id, g.away_team_id)
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        GROUP BY g.season_year, t.team_name
        """
    ).fetchall()
    return {(r["season_year"], r["team_name"]): r["n"] for r in rows}


def load_v2_season_coe(conn: sqlite3.Connection) -> dict:
    """
    Season CoE 2.0 = sum of Game CoE 2.0 across a team's games that
    season (spec section 15, ignoring the not-yet-modeled bonus term B).
    NULL game_coe (ties -- structurally absent from 2000-2026 anyway) is
    excluded, not treated as zero, since a tie isn't "worth zero
    achievement" in the spec's own framing, it's simply unscored yet.
    """
    rows = conn.execute(
        """
        SELECT hgr.team_id, g.season_year, SUM(hgr.game_coe) as season_coe, t.team_name
        FROM hybrid_game_ratings hgr
        JOIN games g ON g.game_id = hgr.game_id
        JOIN teams t ON t.team_id = hgr.team_id
        WHERE hgr.game_coe IS NOT NULL
        GROUP BY hgr.team_id, g.season_year
        """
    ).fetchall()
    return {(r["season_year"], r["team_name"]): r["season_coe"] for r in rows}


def rank_within_year(ratings: dict, year: int) -> dict:
    """Returns {team_name: rank} for a given year, 1 = highest rated."""
    year_ratings = [(team, val) for (y, team), val in ratings.items() if y == year]
    year_ratings.sort(key=lambda x: -x[1])
    return {team: i + 1 for i, (team, val) in enumerate(year_ratings)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--team-ratings-csv", default="data/processed/team_ratings_by_season.csv")
    p.add_argument("--out", default="data/processed/model_comparison.csv")
    p.add_argument("--top-disagreements", type=int, default=15)
    p.add_argument("--confidence-games", type=int, default=CONFIDENCE_GAMES)
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    v1 = load_v1_ratings(args.team_ratings_csv)
    v2 = load_v2_season_coe(conn)
    games_played = load_games_played(conn)
    conn.close()

    # Filter OUT of the ranking population entirely -- not just the
    # output rows -- any team-season with too few games played, so a
    # thin-schedule team can't distort other teams' ranks by occupying
    # a rank slot it shouldn't meaningfully hold.
    v1 = {k: v for k, v in v1.items() if games_played.get(k, 0) >= args.confidence_games}
    v2 = {k: v for k, v in v2.items() if games_played.get(k, 0) >= args.confidence_games}

    years_v2 = sorted(set(y for y, _ in v2))
    print(f"Comparing {len(years_v2)} seasons with CoE 2.0 data: {years_v2[0]}-{years_v2[-1]} "
          f"(team-seasons with fewer than {args.confidence_games} games played excluded)")

    all_rows = []
    for year in years_v2:
        v1_ranks = rank_within_year(v1, year)
        v2_ranks = rank_within_year(v2, year)
        teams_this_year = set(t for (y, t) in v2 if y == year)

        for team in teams_this_year:
            r1 = v1_ranks.get(team)
            r2 = v2_ranks.get(team)
            if r1 is None or r2 is None:
                continue
            all_rows.append({
                "season_year": year,
                "team_name": team,
                "v1_rank": r1,
                "v2_rank": r2,
                "rank_diff": r1 - r2,
                "v1_rating": v1.get((year, team)),
                "v2_season_coe": v2.get((year, team)),
            })

    all_rows.sort(key=lambda r: (r["season_year"], r["v1_rank"]))
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["season_year", "team_name", "v1_rank", "v2_rank", "rank_diff", "v1_rating", "v2_season_coe"])
        w.writeheader()
        w.writerows(all_rows)
    print(f"Wrote {len(all_rows)} team-season comparison rows to {args.out}")

    biggest = sorted(all_rows, key=lambda r: -abs(r["rank_diff"]))[:args.top_disagreements]
    print(f"\nTop {args.top_disagreements} team-seasons where v1 and v2 disagree most on ranking "
          "(not necessarily errors -- see script docstring):")
    print(f"{'Year':<6}{'Team':<20}{'v1 rank':>8}{'v2 rank':>8}{'diff':>6}")
    for r in biggest:
        print(f"{r['season_year']:<6}{r['team_name']:<20}{r['v1_rank']:>8}{r['v2_rank']:>8}{r['rank_diff']:>+6}")


if __name__ == "__main__":
    main()
