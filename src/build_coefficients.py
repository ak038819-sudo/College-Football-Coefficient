#!/usr/bin/env python3
"""
Per-season iterative ratings + rolling 5-year coefficients.

Outputs:
  1) data/processed/team_ratings_by_season.csv
     columns: season_year, team_name, rating

  2) data/processed/team_coeff_5yr.csv
     columns: end_year, window_start, window_end, team_name, coeff_5yr

Model (per-season):
  - Start all teams at 1.0 for that season
  - Iterate:
        winner += opponent_rating * phase_weight
  - Normalize each iteration to keep scale stable

Rolling 5-year:
  - For each end_year, sum weighted season ratings for [end_year-4 .. end_year]
  - Optionally apply within-window decay weights (tunable)
"""

import sqlite3
from pathlib import Path
from collections import defaultdict
import csv
import math
from typing import Dict, List, Tuple, Iterable

DB_PATH = Path("db/league.db")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -------- Tunables --------
ITERATIONS = 15

PHASE_WEIGHTS = {
    "regular": 1.0,
    "bowl": 2.0,
    "cfp": 3.0,
}

# Rolling window settings
ROLLING_YEARS = 5

# If True: apply within-window decay so recent years count more in the 5-year sum
USE_WITHIN_WINDOW_DECAY = True
WITHIN_WINDOW_DECAY_BASE = 0.92  # 1.0 means no decay


def phase_weight(phase: str) -> float:
    p = (phase or "regular").strip().lower()
    return PHASE_WEIGHTS.get(p, 1.0)


def ensure_view(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE VIEW IF NOT EXISTS v_model_games AS
        SELECT
            g.season_year,
            g.game_phase,
            g.home_score,
            g.away_score,
            ht.team_name AS home_team,
            at.team_name AS away_team
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id;
        """
    )
    conn.commit()


def fetch_years(conn: sqlite3.Connection) -> List[int]:
    rows = conn.execute("SELECT DISTINCT season_year FROM games ORDER BY season_year").fetchall()
    return [int(r[0]) for r in rows]


def fetch_games_for_year(conn: sqlite3.Connection, year: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM v_model_games
        WHERE season_year = ?
        """,
        (year,),
    ).fetchall()


def per_season_iterative_ratings(games: List[sqlite3.Row], iterations: int = ITERATIONS) -> Dict[str, float]:
    # Collect teams participating that season
    teams = set()
    for g in games:
        teams.add(g["home_team"])
        teams.add(g["away_team"])
    if not teams:
        return {}

    # Initialize ratings
    ratings = {t: 1.0 for t in teams}

    # Iterate
    for _ in range(iterations):
        new_scores = defaultdict(float)

        for g in games:
            home = g["home_team"]
            away = g["away_team"]
            hs = g["home_score"]
            ays = g["away_score"]

            if home is None or away is None or hs is None or ays is None:
                continue
            if hs == ays:
                continue

            w = phase_weight(g["game_phase"])

            LOSS_PENALTY = 0.15

            if hs > ays:
                new_scores[home] += ratings[away] * w
                new_scores[away] += ratings[home] * w * LOSS_PENALTY
            else:
                new_scores[away] += ratings[home] * w
                new_scores[home] += ratings[away] * w * LOSS_PENALTY


        # Normalize (avoid runaway / keep comparable scale)
        total = sum(new_scores.values())
        if total <= 0:
            break

        scale = len(teams) / total
        for t in teams:
            ratings[t] = new_scores[t] * scale

    return ratings


def within_window_weight(end_year: int, year: int) -> float:
    """Weight for a year within the rolling window ending at end_year."""
    if not USE_WITHIN_WINDOW_DECAY:
        return 1.0
    age = end_year - year  # 0 for end_year, 1 for end_year-1, ...
    return WITHIN_WINDOW_DECAY_BASE ** age


def write_team_ratings_by_season(all_ratings: Dict[Tuple[int, str], float]) -> Path:
    out_path = OUT_DIR / "team_ratings_by_season.csv"
    rows = [
        {"season_year": yr, "team_name": team, "rating": round(val, 6)}
        for (yr, team), val in all_ratings.items()
    ]
    rows.sort(key=lambda r: (r["season_year"], -r["rating"], r["team_name"]))
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["season_year", "team_name", "rating"])
        w.writeheader()
        w.writerows(rows)
    return out_path


def write_rolling_5yr(all_ratings: Dict[Tuple[int, str], float], years: List[int]) -> Path:
    out_path = OUT_DIR / "team_coeff_5yr.csv"
    years = sorted(years)
    rows_out = []

    for end_year in years:
        start_year = end_year - (ROLLING_YEARS - 1)
        window_years = [y for y in years if start_year <= y <= end_year]
        if len(window_years) < ROLLING_YEARS:
            # skip partial windows (keeps interpretation clean)
            continue

        # Collect teams that have ratings in the window
        teams = set(team for (yr, team) in all_ratings.keys() if yr in window_years)

        for team in teams:
            coeff = 0.0
            for y in window_years:
                coeff += all_ratings.get((y, team), 0.0) * within_window_weight(end_year, y)

            rows_out.append(
                {
                    "end_year": end_year,
                    "window_start": start_year,
                    "window_end": end_year,
                    "team_name": team,
                    "coeff_5yr": round(coeff, 6),
                }
            )

    rows_out.sort(key=lambda r: (r["end_year"], -r["coeff_5yr"], r["team_name"]))
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["end_year", "window_start", "window_end", "team_name", "coeff_5yr"],
        )
        w.writeheader()
        w.writerows(rows_out)

    return out_path


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_view(conn)

    years = fetch_years(conn)

    all_ratings: Dict[Tuple[int, str], float] = {}

    for y in years:
        games = fetch_games_for_year(conn, y)
        ratings = per_season_iterative_ratings(games, iterations=ITERATIONS)
        for team, val in ratings.items():
            all_ratings[(y, team)] = float(val)

        # quick console peek (top 5 each season)
        top5 = sorted(ratings.items(), key=lambda x: -x[1])[:5]
        if top5:
            print(f"{y} top 5: " + ", ".join([f"{t} {v:.3f}" for t, v in top5]))

    p1 = write_team_ratings_by_season(all_ratings)
    p2 = write_rolling_5yr(all_ratings, years)

    print(f"\nWrote: {p1}")
    print(f"Wrote: {p2}")

    # Show latest season + latest 5-year window leaders
    latest = max(years)
    latest_season = sorted(
        [(team, all_ratings[(latest, team)]) for team in {t for (y, t) in all_ratings.keys() if y == latest}],
        key=lambda x: -x[1],
    )[:20]
    print(f"\nLatest season ({latest}) Top 20:")
    for i, (t, v) in enumerate(latest_season, 1):
        print(f"{i:>2}. {t:30} {v:.4f}")

    latest_end = max(y for y in years if y >= min(years) + (ROLLING_YEARS - 1))
    # Read back latest 5-year top 20 from computed dict quickly
    # (recompute in-memory for the latest window)
    start = latest_end - (ROLLING_YEARS - 1)
    window_years = [y for y in years if start <= y <= latest_end]
    teams = set(team for (yr, team) in all_ratings.keys() if yr in window_years)
    rolling = []
    for team in teams:
        coeff = sum(all_ratings.get((y, team), 0.0) * within_window_weight(latest_end, y) for y in window_years)
        rolling.append((team, coeff))
    rolling.sort(key=lambda x: -x[1])

    print(f"\nLatest rolling {ROLLING_YEARS}-year window ({start}-{latest_end}) Top 20:")
    for i, (t, v) in enumerate(rolling[:20], 1):
        print(f"{i:>2}. {t:30} {v:.4f}")

    conn.close()


if __name__ == "__main__":
    main()
