#!/usr/bin/env python3
"""
Elo predictions for not-yet-final games (Milestone 3).

Uses the Elo ENGINE's own code -- build_elo.run_elo, effective_rating and
expected_result, with config/model_config.json -- rather than a lookalike
formula, so a prediction can never drift from how the engine itself would
score the same matchup:

    P(home) = expected_result(effective_rating(R_home, home, neutral, HFA),
                              effective_rating(R_away, away, neutral, HFA), scale)

Current ratings come from replaying every COMPLETED game through run_elo
(the table elo_game_history is not trusted to be fresh; if someone loaded
new games without rebuilding Elo, predictions are still correct). If the
upcoming season hasn't started in the completed data, the same one-time
offseason regression run_elo applies at a new season's first game is
applied here -- exactly once, however many calendar years have passed,
because that's what run_elo does.

Scheduled games are read from scheduled_games only and never written
anywhere a rating engine reads.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from build_elo import (effective_rating, expected_result, fetch_games_chronological,
                       load_config, load_game_success_rates, run_elo)
from srdiff import XsrModel, build_layer, load_performance_config

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "model_config.json"
PHASE_CODE = {"regular": 0, "bowl": 1, "cfp": 2}


def elo_config(path: Path = CONFIG_PATH) -> dict:
    return load_config(str(path))["elo"]


def performance_layer(conn: sqlite3.Connection, cfg: dict, path: Path = CONFIG_PATH):
    """
    The performance layer build_elo.py would use, plus the Success Rate it needs.

    This file used to call run_elo with no layer at all, which silently meant the
    margin-of-victory default. That was invisible while MOV was also what the
    config asked for, and became wrong the moment it wasn't: the ratings behind
    the site's upcoming-game predictions were then produced by a different model
    than the ratings on its rankings pages, for the same teams on the same day.
    Reading the configured layer here instead of defaulting is what keeps the two
    from drifting apart again.
    """
    perf = load_performance_config(load_config(str(path)).get("performance"))
    model = XsrModel.load(perf["model_path"]) if perf["model_path"] else None
    return build_layer(perf, cfg, model), load_game_success_rates(conn)


def game_expectation(r_home: float, r_away: float, neutral: bool, cfg: dict) -> float:
    """P(home team wins), computed exactly as build_elo.run_elo computes a game's pregame expectation."""
    eff_home = effective_rating(r_home, True, bool(neutral), cfg["home_field"])
    eff_away = effective_rating(r_away, False, bool(neutral), cfg["home_field"])
    return expected_result(eff_home, eff_away, cfg["scale"])


def current_ratings(conn: sqlite3.Connection, cfg: dict, for_season: int,
                    layer=None, success_rates: dict | None = None):
    """
    Ratings every team would carry into its next game of `for_season`, as
    run_elo would have them. Returns (ratings, last_completed_season,
    last_completed_date). Teams absent from `ratings` have never played a
    completed game; run_elo would start them at initial_rating.

    `layer`/`success_rates` default to the CONFIGURED performance layer rather
        than to run_elo's own MOV default -- see performance_layer() above for why
        that distinction is the whole point.
    """
    prev = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        games = fetch_games_chronological(conn)
    finally:
        conn.row_factory = prev
    if not games:
        return {}, None, None
    if layer is None:
        layer, configured_sr = performance_layer(conn, cfg)
        if success_rates is None:
            success_rates = configured_sr
    _, ratings, _ = run_elo(games, cfg, layer, success_rates)
    last_season, last_date = games[-1]["season_year"], games[-1]["game_date"]
    if for_season > last_season:
        # run_elo regresses every known team once when it meets a new season's first game.
        init, keep = cfg["initial_rating"], cfg["offseason_retention"]
        ratings = {t: init + keep * (r - init) for t, r in ratings.items()}
    return ratings, last_season, last_date


def build_upcoming(conn: sqlite3.Connection, cfg: dict) -> dict:
    """
    {"season", "ratings_as_of", "games": [...], "current_elo": [[team_id, elo, rank], ...]}

    games rows: [game_id, season, week, kickoff_utc, start_time_tbd, home_id, away_id,
                 neutral, phase_code, home_elo, away_elo, p_home, home_provisional, away_provisional]
    A *_provisional flag marks a team with no completed game yet (rating = the
    engine's starting rating), so the UI can say so instead of implying history.
    """
    empty = {"season": None, "ratings_as_of": None, "games": [], "current_elo": []}
    has_sched = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'").fetchone()
    sched = conn.execute(
        """
        SELECT game_id, season_year, week, kickoff_utc, start_time_tbd, home_team_id, away_team_id,
               neutral_site, game_phase
        FROM scheduled_games
        WHERE game_id NOT IN (SELECT game_id FROM games)
        ORDER BY kickoff_utc IS NULL, kickoff_utc, week, game_id
        """).fetchall() if has_sched else []

    last_completed = conn.execute(
        "SELECT MAX(season_year) FROM games WHERE home_score IS NOT NULL AND away_score IS NOT NULL").fetchone()[0]
    if last_completed is None and not sched:
        return empty
    season = max([last_completed or 0] + [r[1] for r in sched])

    cache: dict = {}
    # Built once: the config read, the curve file and the Success Rate table are
    # the same for every season asked about, and only the Elo walk differs.
    layer, success_rates = performance_layer(conn, cfg)

    def ratings_for(s: int):
        if s not in cache:
            cache[s] = current_ratings(conn, cfg, s, layer, success_rates)
        return cache[s]

    games_out = []
    for gid, s, week, kickoff, tbd, home, away, neutral, phase in sched:
        ratings = ratings_for(s)[0]
        r_home = ratings.get(home, cfg["initial_rating"])
        r_away = ratings.get(away, cfg["initial_rating"])
        p_home = game_expectation(r_home, r_away, neutral, cfg)
        games_out.append([gid, s, week, kickoff, int(tbd or 0), home, away, int(neutral or 0),
                          PHASE_CODE.get(phase, 0), round(r_home, 1), round(r_away, 1), round(p_home, 4),
                          int(home not in ratings), int(away not in ratings)])

    ratings, _, as_of = ratings_for(season)
    # Leaderboard population: teams active in `season` (played or scheduled);
    # before a season has any data at all, fall back to last season's teams.
    active = {t for (t,) in conn.execute(
        """SELECT home_team_id FROM games WHERE season_year = ? AND home_score IS NOT NULL
           UNION SELECT away_team_id FROM games WHERE season_year = ? AND home_score IS NOT NULL""",
        (season, season))}
    active |= {r[5] for r in sched if r[1] == season} | {r[6] for r in sched if r[1] == season}
    if not active and last_completed is not None:
        active = {t for (t,) in conn.execute(
            "SELECT home_team_id FROM games WHERE season_year = ? UNION SELECT away_team_id FROM games WHERE season_year = ?",
            (last_completed, last_completed))}
    board = sorted(((t, ratings.get(t, cfg["initial_rating"])) for t in active), key=lambda x: (-x[1], x[0]))
    current_elo = [[t, round(r, 1), i + 1] for i, (t, r) in enumerate(board)]

    return {"season": season, "ratings_as_of": as_of, "games": games_out, "current_elo": current_elo}
