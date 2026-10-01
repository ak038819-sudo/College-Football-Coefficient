"""Pregame team HFA provider derived from an independent flat-Elo reference.

The closure is called by run_elo BEFORE each game. Games on the same calendar
date are excluded because the archive has no reliable kickoff ordering.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from build_elo import expected_result
from hfa import (HomeGame, calculate_raw_hfa, calculate_team_hfa,
                 convert_hfa_to_elo_points, weighted_sums)


def home_games_from_flat(games, flat_rows, scale):
    if len(flat_rows) != 2 * len(games):
        raise ValueError('Flat reference must have two Elo rows per game')
    result = []
    for g, home, away in zip(games, flat_rows[::2], flat_rows[1::2]):
        if home[0] != g['game_id'] or away[0] != g['game_id'] or home[1] != g['home_team_id']:
            raise ValueError('Flat reference game order differs from source games')
        if g['neutral_site']:
            continue
        hs, aws = g['home_score'], g['away_score']
        result.append(HomeGame(g['game_id'], g['season_year'],
                               dt.date.fromisoformat(str(g['game_date'])[:10]),
                               g['home_team_id'], g['away_team_id'], home[2], away[2],
                               expected_result(home[2], away[2], scale),
                               1.0 if hs > aws else 0.0 if hs < aws else 0.5))
    return result


def pregame_bonus_provider(reference_games, hfa_cfg, scale, flat_bonus):
    """Return a callback for run_elo; never reads its dynamic ratings/results."""
    by_team = defaultdict(list)
    for game in reference_games:
        by_team[game.team_id].append(game)
    day_cache, team_cache = {}, {}
    half_life = hfa_cfg['half_life_years']
    bounds = tuple(hfa_cfg['point_bounds'])

    def bonus(row):
        date = dt.date.fromisoformat(str(row['game_date'])[:10])
        team = row['home_team_id']
        key = date, team
        if key in team_cache:
            return team_cache[key]
        if date not in day_cache:
            sums = weighted_sums(reference_games, date, half_life)
            baseline = calculate_raw_hfa(sums)
            points = None
            if baseline is not None:
                points, at_bound = convert_hfa_to_elo_points(reference_games, date, half_life,
                                                             baseline * sums.sum_wp, scale, bounds)
                if at_bound:
                    points = None
            day_cache[date] = sums, points
        sums, national_points = day_cache[date]
        if national_points is None:
            value = flat_bonus
        else:
            est = calculate_team_hfa(by_team[team], reference_games, date, hfa_cfg, scale,
                                     national_points=national_points, national_sums=sums)
            value = est['elo_hfa_points']
            if value is None or est['elo_points_at_bound']:
                value = flat_bonus
        team_cache[key] = value
        return value

    return bonus
