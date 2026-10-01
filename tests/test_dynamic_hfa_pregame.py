import datetime as dt

from dynamic_hfa import home_games_from_flat, pregame_bonus_provider


CFG = {'half_life_years': 10, 'shrinkage_k': 30, 'fcs_prior_weight': .5,
       'effective_n_method': 'sum_weights', 'point_bounds': [-400, 400]}


def game(gid, date, home=1, away=2, hs=30, aws=20, neutral=0):
    return dict(game_id=gid, season_year=2026, game_date=date, home_team_id=home,
                away_team_id=away, home_score=hs, away_score=aws, neutral_site=neutral)


def test_pregame_hfa_excludes_same_day_and_future_results():
    games = [game(1, '2026-09-01'), game(2, '2026-09-01', hs=0, aws=20),
             game(3, '2026-09-08')]
    flat = [(g['game_id'], tid, 1500) for g in games for tid in (1, 2)]
    reference = home_games_from_flat(games, flat, 400)
    bonus = pregame_bonus_provider(reference, CFG, 400, 50)
    assert bonus(games[0]) == 50  # no past games, even though reference contains its result
    assert bonus(games[1]) == 50  # same-day result cannot be ordered safely
    assert bonus(games[2]) != 50
    later_changed = games[:2] + [game(3, '2026-09-08', hs=0, aws=100)]
    later = home_games_from_flat(later_changed, flat, 400)
    assert pregame_bonus_provider(later, CFG, 400, 50)(games[2]) == bonus(games[2])


def test_neutral_game_does_not_enter_hfa_evidence():
    games = [game(1, '2026-09-01', neutral=1), game(2, '2026-09-08')]
    flat = [(g['game_id'], tid, 1500) for g in games for tid in (1, 2)]
    reference = home_games_from_flat(games, flat, 400)
    assert [g.game_id for g in reference] == [2]
    assert pregame_bonus_provider(reference, CFG, 400, 50)(games[1]) == 50
