"""Chronological, no-future-data validation of matchup metric point estimates.

Intentionally standalone and read-only. Each evaluated date uses only games
from earlier dates, with the same matchup model used for scouting. This
estimates metric error, NOT game win probability or Elo predictive quality.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path

try:
    from src.build_matchup_intelligence import METRICS, load_observations
    from src.matchup_intelligence import fit_profiles, project_matchup
except ModuleNotFoundError:
    from build_matchup_intelligence import METRICS, load_observations
    from matchup_intelligence import fit_profiles, project_matchup


def _scores(errors: list[float]) -> dict | None:
    if not errors:
        return None
    return {
        "n": len(errors),
        "mae": sum(abs(e) for e in errors) / len(errors),
        "rmse": math.sqrt(sum(e * e for e in errors) / len(errors)),
    }


def evaluate_walk_forward(
    conn: sqlite3.Connection,
    *,
    season: int,
    metric: str,
    min_training_games: int = 4,
    shrinkage: float = 8.0,
) -> dict:
    """Score only games strictly later than the data used for fitting.

    Evaluates every offense team-game row independently. Two baselines:
    national game-average and the same team's shrinkage-adjusted prior
    offensive average. Never fits on the test date or later observations.
    Teams lacking both offense and defensive profile coverage are excluded
    from model error, but counted against forecast coverage.
    """
    if min_training_games < 1:
        raise ValueError("min_training_games must be positive")
    if metric not in METRICS:
        raise ValueError(f"Unknown metric {metric}")
    # First collect all eligible played games with a far-future cutoff.
    # The adapter handles FBS identity, score completeness and missing values.
    # We also get their recorded dates here for chronological grouping.
    _rows, coverage = load_observations(
        conn, season=season, as_of=f"{season + 2}-01-01", metric=metric)
    column = METRICS[metric]
    # Use the exact same eligibility conditions; retrieve dates by game/team
    # via identity. Duplicate team-game values are skipped by the adapter.
    dated = conn.execute(f"""
        SELECT g.game_date, g.home_team_id, g.away_team_id,
               a.team_id, a.{column}, g.game_id
        FROM games g
        JOIN game_team_advanced a ON a.game_id = g.game_id
        JOIN team_membership_by_season mh
          ON mh.team_id = g.home_team_id AND mh.season_year = g.season_year
        JOIN team_membership_by_season ma
          ON ma.team_id = g.away_team_id AND ma.season_year = g.season_year
        WHERE g.season_year = ? AND mh.is_fbs = 1 AND ma.is_fbs = 1
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
          AND a.season_year = g.season_year AND a.{column} IS NOT NULL
          AND a.team_id IN (g.home_team_id, g.away_team_id)
        ORDER BY g.game_date, g.game_id, a.team_id
    """, (season,)).fetchall()
    # Match the adapter's chronological set, never using test game values
    # as training input or allowing duplicates or invalid dates.
    from datetime import date
    by_day: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
    seen = set()
    for day, home, away, team, value, game_id in dated:
        if not day:
            continue
        try:
            date_key = date.fromisoformat(str(day)[:10]).isoformat()
        except ValueError:
            continue
        if date_key >= f"{season + 2}-01-01":
            continue
        key = (game_id, team)
        if key in seen:
            continue
        seen.add(key)
        by_day[date_key].append((
            str(team), str(away if team == home else home), float(value)))
    model_errors, national_errors, team_errors = [], [], []
    matched_national_errors, matched_team_errors = [], []
    scored_dates = 0
    considered = 0
    # Refit once per calendar date; all games of the same day remain unseen.
    for day in sorted(by_day):
        training, _ = load_observations(
            conn, season=season, as_of=day, metric=metric)
        if len({(r.team, r.opponent) for r in training}) < min_training_games:
            continue
        grades, baseline = fit_profiles(training, shrinkage=shrinkage)
        # Simple prior offense-only shrinkage baseline (not opponent-adjusted).
        prior = defaultdict(list)
        for r in training:
            prior[r.team].append(r.value)
        for offense, defense, observed in by_day[day]:
            considered += 1
            national_errors.append(observed - baseline)
            prior_values = prior.get(offense, [])
            offensive_guess = ((sum(prior_values) + shrinkage * baseline)
                               / (len(prior_values) + shrinkage))
            team_errors.append(observed - offensive_guess)
            if offense not in grades or defense not in grades:
                continue
            prediction = project_matchup(grades, baseline, offense, defense).projected_value
            model_errors.append(observed - prediction)
            matched_national_errors.append(observed - baseline)
            matched_team_errors.append(observed - offensive_guess)
        scored_dates += 1
    return {
        "season": season, "metric": metric, "model": "opponent-adjusted-ridge-v1",
        "method": "strict-prior-calendar-date",
        "evaluation_dates": scored_dates, "considered_team_games": considered,
        "model_coverage": (len(model_errors) / considered) if considered else 0,
        "opponent_adjusted": _scores(model_errors),
        "national_mean_baseline": _scores(national_errors),
        "prior_team_offense_baseline": _scores(team_errors),
        "matched_national_baseline": _scores(matched_national_errors),
        "matched_team_offense_baseline": _scores(matched_team_errors),
        "note": "Compare the model to matched baselines on identical observed team-games.",
        "source_coverage": coverage,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--metric", choices=sorted(METRICS), required=True)
    p.add_argument("--min-training-games", type=int, default=4)
    args = p.parse_args()
    db = Path(args.db).resolve()
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as conn:
        report = evaluate_walk_forward(
            conn, season=args.season, metric=args.metric,
            min_training_games=args.min_training_games)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
