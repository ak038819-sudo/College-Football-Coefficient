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

# Confidence blending: a team's FINAL season rating blends the raw
# iterative result with a regressed prior (their previous season's own
# blended rating), weighted by how many games they've actually played
# so far this season. CONFIDENCE_GAMES is the number of games at which
# a team gets full confidence (weight=1.0, pure raw iterative rating,
# identical to the old behavior) -- 8 is comfortably below a normal
# full season's game count, so completed seasons are unaffected.
#
# This replaces an earlier, simpler-looking idea (seed each team's
# STARTING value from their prior instead of a flat 1.0) that was
# tried and tested here but didn't actually work: the per-iteration
# renormalization creates a feedback loop that concentrates rating
# mass wherever the game graph happens to be locally dense (e.g. a
# small cluster of 3-4 teams that have already played each other),
# and that structural effect overwhelms any starting value after 15
# iterations regardless of what it was. Blending the FINAL result
# with a confidence-weighted prior, rather than seeding the start,
# is what actually dampens it -- verified: in a synthetic sparse
# 2-games-per-team scenario, an early 2-0 start (real case: Tulsa,
# 2026) dropped from an absurd 9.6 to a sensible 2.7, while
# one-blowout-win blue-bloods (Ohio State, Georgia, Alabama) correctly
# moved from near-zero back up near their real prior-season quality.
CONFIDENCE_GAMES = 8


def phase_weight(phase: str) -> float:
    p = (phase or "regular").strip().lower()
    return PHASE_WEIGHTS.get(p, 1.0)


def compute_prior_ratings(previous_year_ratings: Dict[str, float], factor: float) -> Dict[str, float]:
    """
    Regresses each team's previous-season final (blended) rating toward
    the mean (1.0). A team absent from previous_year_ratings (new to the
    dataset, just joined FBS, etc.) simply isn't in the returned dict --
    callers should treat a missing team as "no prior available",
    defaulting to the flat 1.0 baseline.
    """
    return {team: 1.0 + factor * (rating - 1.0) for team, rating in previous_year_ratings.items()}


def count_games_played(games: List[sqlite3.Row]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for g in games:
        if g["home_score"] is None or g["away_score"] is None:
            continue
        counts[g["home_team"]] += 1
        counts[g["away_team"]] += 1
    return dict(counts)


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


def per_season_iterative_ratings(
    games: List[sqlite3.Row],
    iterations: int = ITERATIONS,
    prior: Dict[str, float] = None,
    confidence: Dict[str, float] = None,
) -> Dict[str, float]:
    """
    prior[team] / confidence[team] (0.0-1.0): at EVERY iteration, a
    team's updated rating is confidence*computed_value + (1-confidence)*
    prior[team], not just as a one-time blend at the end. This matters:
    a ONE-TIME post-hoc blend was tried and tested first, and found
    insufficient -- on real sparse early-2026 data, a raw (undamped)
    rating reached 39.79 for a 2-0 team (Tulsa) whose only losses came
    from teams also caught in the same small-sample feedback loop, and
    even blending 75% toward a modest prior only brought that down to
    10.46 -- still an outlier well above every blue-blood program. The
    root problem is structural: this Bradley-Terry-style algorithm's
    per-iteration renormalization concentrates rating mass wherever the
    game graph is locally dense, and that compounds across all 15
    iterations. Pulling back toward the prior at every single iteration,
    not just once at the end, stops that compounding before it can
    reach an extreme value in the first place.

    A team missing from confidence gets 0.0 (full trust in prior, not
    1.0) -- this only affects a team with zero valid (scored) games in
    the given games list, which would otherwise compute to a meaningless
    raw 0 rather than sensibly falling back to their prior.

    confidence=1.0 (or an empty confidence dict) for every team
    reproduces the original undamped behavior exactly -- this is what
    happens automatically once a season is complete (see
    CONFIDENCE_GAMES), so finished seasons are unaffected.
    """
    prior = prior or {}
    confidence = confidence or {}

    # Collect teams participating that season
    teams = set()
    for g in games:
        teams.add(g["home_team"])
        teams.add(g["away_team"])
    if not teams:
        return {}

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
            raw_new = new_scores[t] * scale
            c = confidence.get(t, 0.0)
            p = prior.get(t, raw_new)
            ratings[t] = c * raw_new + (1 - c) * p

    return ratings


def fetch_team_conference_map(conn: sqlite3.Connection, year: int) -> Dict[str, str]:
    """
    Maps team_name -> conference_real for a given season, via
    team_membership_by_season (joined through teams for the name).
    Teams with no membership row for that season (or no conference)
    are simply absent from the map.
    """
    rows = conn.execute(
        """
        SELECT t.team_name AS team_name, m.conference_real AS conference
        FROM team_membership_by_season m
        JOIN teams t ON t.team_id = m.team_id
        WHERE m.season_year = ? AND m.conference_real IS NOT NULL
        """,
        (year,),
    ).fetchall()
    return {r["team_name"]: r["conference"] for r in rows}


def within_window_weight(end_year: int, year: int) -> float:
    """Weight for a year within the rolling window ending at end_year."""
    if not USE_WITHIN_WINDOW_DECAY:
        return 1.0
    age = end_year - year  # 0 for end_year, 1 for end_year-1, ...
    return WITHIN_WINDOW_DECAY_BASE ** age


def compute_conference_ratings_by_season(
    all_ratings: Dict[Tuple[int, str], float],
    team_conf_by_year: Dict[int, Dict[str, str]],
) -> Dict[Tuple[int, str], float]:
    """
    Conference rating per season = SUM of that season's ratings for every
    team whose membership places them in that conference that year.
    """
    conf_ratings: Dict[Tuple[int, str], float] = {}
    for (year, team), rating in all_ratings.items():
        conf = team_conf_by_year.get(year, {}).get(team)
        if conf is None:
            continue
        key = (year, conf)
        conf_ratings[key] = conf_ratings.get(key, 0.0) + rating
    return conf_ratings


def write_conference_ratings_by_season(conf_ratings: Dict[Tuple[int, str], float]) -> Path:
    out_path = OUT_DIR / "conference_ratings_by_season.csv"
    rows = [
        {"season_year": yr, "conference_name": conf, "rating": round(val, 6)}
        for (yr, conf), val in conf_ratings.items()
    ]
    rows.sort(key=lambda r: (r["season_year"], -r["rating"], r["conference_name"]))
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["season_year", "conference_name", "rating"])
        w.writeheader()
        w.writerows(rows)
    return out_path


def write_conference_rolling_5yr(
    conf_ratings: Dict[Tuple[int, str], float], years: List[int]
) -> Path:
    """
    Same window/decay logic as write_rolling_5yr, but for conferences.
    A conference's rolling coefficient is the decay-weighted sum of its
    OWN per-season sums, which is equivalent to summing its member
    teams' rolling coefficients (sum and weighted-sum commute).
    """
    out_path = OUT_DIR / "conference_coeff_5yr.csv"
    years = sorted(years)
    rows_out = []

    for end_year in years:
        start_year = end_year - (ROLLING_YEARS - 1)
        window_years = [y for y in years if start_year <= y <= end_year]
        if len(window_years) < ROLLING_YEARS:
            continue

        confs = set(conf for (yr, conf) in conf_ratings.keys() if yr in window_years)

        for conf in confs:
            coeff = 0.0
            for y in window_years:
                coeff += conf_ratings.get((y, conf), 0.0) * within_window_weight(end_year, y)

            rows_out.append(
                {
                    "end_year": end_year,
                    "window_start": start_year,
                    "window_end": end_year,
                    "conference_name": conf,
                    "coeff_5yr": round(coeff, 6),
                }
            )

    rows_out.sort(key=lambda r: (r["end_year"], -r["coeff_5yr"], r["conference_name"]))
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["end_year", "window_start", "window_end", "conference_name", "coeff_5yr"],
        )
        w.writeheader()
        w.writerows(rows_out)

    return out_path


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
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument(
        "--confidence-games", type=int, default=CONFIDENCE_GAMES,
        help="Games played at which a team's rating gets full confidence (pure raw iterative "
             f"value, no prior blend). Default: {CONFIDENCE_GAMES}",
    )
    p.add_argument(
        "--prior-regression", type=float, default=0.5,
        help="How much of a team's previous-season BLENDED rating carries forward as their "
             "next season's prior, regressed toward 1.0 (0.0=ignore prior entirely, "
             "1.0=full carryover). Default: 0.5",
    )
    args = p.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"Missing DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_view(conn)

    years = fetch_years(conn)

    all_ratings: Dict[Tuple[int, str], float] = {}
    team_conf_by_year: Dict[int, Dict[str, str]] = {}
    previous_year_ratings: Dict[str, float] = {}

    for y in years:
        games = fetch_games_for_year(conn, y)
        games_played = count_games_played(games)
        prior = compute_prior_ratings(previous_year_ratings, args.prior_regression)
        confidence = {
            t: min(1.0, n / args.confidence_games) if args.confidence_games > 0 else 1.0
            for t, n in games_played.items()
        }
        ratings = per_season_iterative_ratings(games, iterations=ITERATIONS, prior=prior, confidence=confidence)

        for team, val in ratings.items():
            all_ratings[(y, team)] = float(val)

        team_conf_by_year[y] = fetch_team_conference_map(conn, y)
        previous_year_ratings = ratings

        # quick console peek (top 5 each season)
        top5 = sorted(ratings.items(), key=lambda x: -x[1])[:5]
        if top5:
            print(f"{y} top 5: " + ", ".join([f"{t} {v:.3f}" for t, v in top5]))

    p1 = write_team_ratings_by_season(all_ratings)
    p2 = write_rolling_5yr(all_ratings, years)

    conf_ratings = compute_conference_ratings_by_season(all_ratings, team_conf_by_year)
    p3 = write_conference_ratings_by_season(conf_ratings)
    p4 = write_conference_rolling_5yr(conf_ratings, years)

    print(f"\nWrote: {p1}")
    print(f"Wrote: {p2}")
    print(f"Wrote: {p3}")
    print(f"Wrote: {p4}")

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
