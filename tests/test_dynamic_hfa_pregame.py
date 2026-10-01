import datetime as dt

from dynamic_hfa import home_games_from_flat, pregame_bonus_provider
from tune_dynamic_hfa import score


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


def test_centered_blend_has_a_flat_off_switch_and_bounded_team_effect():
    games = [game(1, '2026-09-01', 1, 2, 40, 0),
             game(2, '2026-09-02', 3, 4, 0, 40),
             game(3, '2026-09-08', 1, 4)]
    flat = [(g['game_id'], tid, 1500) for g in games for tid in (g['home_team_id'], g['away_team_id'])]
    reference = home_games_from_flat(games, flat, 400)
    assert pregame_bonus_provider(reference, CFG, 400, 50, centered_alpha=0)(games[2]) == 50
    value = pregame_bonus_provider(reference, CFG, 400, 50, centered_alpha=1, max_deviation=10)(games[2])
    assert 40 <= value <= 60
    assert value != 50


def test_tuning_keeps_partial_season_out_of_complete_validation():
    games = [game(1, '2018-09-01'), game(2, '2023-09-01'), game(3, '2026-09-01')]
    for year, g in zip((2018, 2023, 2026), games):
        g['season_year'] = year
    rows = [(gid, tid, 1500, 1500, .7) for gid in (1, 2, 3) for tid in (1, 2)]
    result = score(games, rows)
    assert result['train']['games'] == 1
    assert result['validation']['games'] == 1
    assert result['current']['games'] == 1
