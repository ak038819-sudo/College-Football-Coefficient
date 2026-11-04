"""The NBA rating: the replay, the fit's own rules, and the committed model.

The test that matters most here is the NBA Cup one. Every other game of the
in-season tournament counts in the standings and the final does not, the
source sends all of them as regular-season games, and the event was renamed
mid-archive so the note that identifies the final has two spellings. Left
alone, the two finalists' records would disagree with every other source and
nothing would look wrong.
"""

import json
from pathlib import Path

import pytest

from src.nba import build_ratings
from src.nba import elo
from src.nba import fit_elo
from src.nba import games as games_module
from src.nba import identity
from src.nba import store as store_module

CONFIG = Path('config/nba_model.json')


# ------------------------------------------------------------- the Cup final

def test_the_cup_final_is_not_a_regular_season_game():
    """It does not count in the standings; every other Cup game does."""
    assert identity.season_type_of(2, 'NBA Cup Championship') == 'cup_final'
    assert identity.season_type_of(2, 'NBA Cup - Group Play') == 'regular'
    assert identity.season_type_of(2, 'NBA Cup - Quarterfinals') == 'regular'
    assert identity.season_type_of(2, 'NBA Cup - Semifinals') == 'regular'


def test_the_cup_final_is_recognised_under_its_old_name_too():
    """2023-24 calls it the In-Season Tournament; a search for "Cup" misses it.

    This one cost a measurement: the first pass found the 2025 and 2026
    finals and reported that 2024 had none, while 2024's two finalists were
    sitting on 83 games just like the others'.
    """
    assert identity.season_type_of(
        2, 'NBA In-Season Tournament Championship') == 'cup_final'


def test_the_note_is_matched_without_regard_to_case_or_spacing():
    assert identity.season_type_of(2, '  nba   CUP   Championship ') \
        == 'cup_final'


def test_a_cup_note_on_a_playoff_game_does_not_change_its_type():
    # Only a regular-season row can be the final. A postseason row carrying
    # that text is something else, and reclassifying it would move a playoff
    # game out of the playoffs.
    assert identity.season_type_of(3, 'NBA Cup Championship') == 'postseason'


def test_the_cup_final_still_counts_toward_the_rating():
    """The NBA excludes it from the standings, not from history."""
    row = _row(season_type=2, notes_headline='NBA Cup Championship')
    game = games_module.parse_row(row)
    assert game['season_type'] == 'cup_final'
    assert game['rateable'] == 1
    assert game['postseason'] == 0, \
        'it is played inside the regular season, not after it'


# ------------------------------------------------------------------- the elo

def _row(**overrides):
    row = dict(
        id=401585612, season=2024, season_type=2,
        date='2024-01-15T00:30Z', game_date='2024-01-14',
        neutral_site=False, status_type_completed=True,
        home_id=25, home_score=112, home_abbreviation='OKC',
        home_location='Oklahoma City', home_name='Thunder',
        home_display_name='Oklahoma City Thunder',
        home_logo='h.png', home_logo_dark='hd.png',
        away_id=17, away_score=108, away_abbreviation='BKN',
        away_location='Brooklyn', away_name='Nets',
        away_display_name='Brooklyn Nets',
        away_logo='a.png', away_logo_dark='ad.png',
        venue_id=2317, venue_full_name='Paycom Center',
        venue_address_city='Oklahoma City', venue_address_state='OK',
        attendance=18203, notes_headline='', time_valid=True)
    row.update(overrides)
    return row


def test_a_draw_is_refused_rather_than_scored_as_a_coin_flip():
    """The NFL engine scores a tie 0.5. An NBA game cannot be drawn."""
    assert elo.outcome(112, 108) == 1.0
    assert elo.outcome(108, 112) == 0.0
    assert elo.outcome(None, None) is None
    with pytest.raises(elo.DrawError):
        elo.outcome(100, 100)


def _game(key, season, home, away, home_points, away_points, *, date=None,
          neutral=0):
    return dict(game_key=key, season=season, game_date=date or f'{season}-01-01',
                neutral_site=neutral, home_team_key=home, away_team_key=away,
                home_points=home_points, away_points=away_points)


CONF = dict(k=20, home_advantage=80, offseason_retention=0.6,
            margin_model='mov', margin_exponent=0.75, reference_margin=11.0)


def test_the_winner_gains_exactly_what_the_loser_loses():
    rows, ratings = elo.replay([_game('g1', 2024, 'A', 'B', 110, 100)], CONF)
    assert ratings['A'] - elo.INITIAL_RATING == \
        pytest.approx(elo.INITIAL_RATING - ratings['B'])
    assert rows[0]['counted'] == 1


def test_an_unplayed_game_moves_nothing():
    rows, ratings = elo.replay([_game('g1', 2024, 'A', 'B', None, None)], CONF)
    assert ratings == {}
    assert rows[0]['counted'] == 0
    assert rows[0]['rating_change'] == 0.0


def test_the_home_team_is_favoured_and_a_neutral_game_is_not():
    at_home = elo.expected_home(1500, 1500, home_advantage=80)
    neutral = elo.expected_home(1500, 1500, home_advantage=80, neutral=True)
    assert at_home > 0.5
    assert neutral == pytest.approx(0.5)


def test_a_rating_is_pulled_toward_the_mean_between_seasons():
    before = [_game('g1', 2024, 'A', 'B', 130, 100)]
    rows, one_season = elo.replay(before, CONF)
    carried = elo.regress(one_season['A'], 0.6)
    assert elo.INITIAL_RATING < carried < one_season['A']


def test_a_bigger_win_moves_a_rating_further():
    narrow, _ = elo.replay([_game('g', 2024, 'A', 'B', 101, 100)], CONF)
    wide, _ = elo.replay([_game('g', 2024, 'A', 'B', 140, 100)], CONF)
    assert wide[0]['rating_change'] > narrow[0]['rating_change']


def test_the_home_win_rate_is_the_baseline_a_model_has_to_beat():
    rows = [dict(counted=1, home_expected=0.6, outcome=1.0),
            dict(counted=1, home_expected=0.6, outcome=0.0),
            dict(counted=1, home_expected=0.6, outcome=1.0),
            dict(counted=1, home_expected=0.6, outcome=1.0)]
    assert elo.home_win_rate(rows) == 0.75


# -------------------------------------------------------------------- the fit

def test_a_season_whose_last_playoff_game_was_played_is_complete(tmp_path):
    """2019-20 is the case this gets wrong under the obvious test.

    Nine postseason rows of 26-28 August 2020 are the games the players'
    boycott called off. They were replayed under new ids and ESPN left the
    originals marked not completed, so "no postseason game is outstanding"
    would drop a season whose Finals plainly finished.
    """
    connection = store_module.connect(tmp_path / 'nba.db')
    rows = [
        _row(id=1, season=2020, season_type=2, game_date='2020-03-01'),
        # the abandoned row, left behind by the source
        _row(id=2, season=2020, season_type=3, game_date='2020-08-26',
             status_type_completed=False, home_score=0, away_score=0),
        # the Finals, played
        _row(id=3, season=2020, season_type=3, game_date='2020-10-11'),
    ]
    parsed, _ = games_module.parse(rows)
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(rows, 2020))
    store_module.upsert_games(connection, parsed)
    assert fit_elo.complete_seasons(connection) == [2020]


def test_a_season_still_being_played_is_not_complete(tmp_path):
    connection = store_module.connect(tmp_path / 'nba.db')
    rows = [_row(id=1, season=2027, season_type=2, game_date='2026-11-01'),
            _row(id=2, season=2027, season_type=3, game_date='2027-05-01',
                 status_type_completed=False, home_score=0, away_score=0)]
    parsed, _ = games_module.parse(rows)
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(rows, 2027))
    store_module.upsert_games(connection, parsed)
    assert fit_elo.complete_seasons(connection) == []


def test_a_parameter_at_the_edge_of_its_grid_is_reported():
    """A parameter the grid stopped is the boundary, not a fitted value."""
    assert fit_elo.at_grid_edge(
        dict(k=min(fit_elo.K_GRID), home_advantage=80,
             offseason_retention=0.6)) == ['k']
    assert fit_elo.at_grid_edge(
        dict(k=20, home_advantage=max(fit_elo.HOME_GRID),
             offseason_retention=1.0)) == ['home_advantage',
                                           'offseason_retention']
    assert fit_elo.at_grid_edge(
        dict(k=20, home_advantage=80, offseason_retention=0.6)) == []


def test_the_home_grid_reaches_past_every_other_league():
    # College basketball fitted to 100. A grid that stopped there could not
    # have found a higher answer, and could not have said there wasn't one.
    assert max(fit_elo.HOME_GRID) > 100


def test_the_measured_quantities_come_from_the_fitting_seasons_only():
    played = [_game(f'g{i}', 2002, 'A', 'B', 110, 100) for i in range(3)]
    held = [_game('h', 2003, 'A', 'B', 180, 100)]
    measured = fit_elo.measure(played + held, {2002})
    assert measured['reference_margin'] == 10.0, \
        'the 80-point holdout game leaked into the reference margin'
    assert measured['fit_games'] == 3


# ------------------------------------------------------- the committed model

def test_the_committed_model_loads_and_its_validation_holds():
    payload = build_ratings.load_config(CONFIG)
    validation = build_ratings.check_validation(payload)
    assert validation['holdout']['games'] > 5000
    assert validation['holdout']['brier'] < build_ratings.MAX_HOLDOUT_BRIER


def test_the_committed_model_was_fitted_on_nba_games_not_inherited():
    elo_block = build_ratings.load_config(CONFIG)['elo']
    # Each of the other three leagues' k, and none of them is this one.
    assert elo_block['k'] not in (45, 48)
    # Nor is the home advantage any other league's.
    assert elo_block['home_advantage'] not in (50, 100)


def test_changing_a_parameter_without_refitting_fails_the_build():
    """The hash pins the held-out claim to the exact parameters."""
    payload = build_ratings.load_config(CONFIG)
    payload['elo'] = dict(payload['elo'], k=payload['elo']['k'] + 1)
    with pytest.raises(ValueError, match='hashes to'):
        build_ratings.check_validation(payload)


def test_a_holdout_that_leaked_into_the_fit_is_refused():
    payload = build_ratings.load_config(CONFIG)
    payload['validation'] = dict(
        payload['validation'],
        fit_seasons=payload['validation']['fit_seasons']
        + [payload['validation']['holdout_seasons'][0]])
    with pytest.raises(ValueError, match='holdout leaked'):
        build_ratings.check_validation(payload)


def test_a_model_no_better_than_picking_the_home_team_is_refused():
    payload = build_ratings.load_config(CONFIG)
    payload['validation'] = dict(
        payload['validation'],
        holdout=dict(payload['validation']['holdout'], brier=0.2469))
    with pytest.raises(ValueError, match='no better than picking the home'):
        build_ratings.check_validation(payload)


def test_the_brier_floor_is_the_measured_home_baseline():
    # 0.246, not a number carried over from another league. The NBA's own
    # held-out home base rate of 0.5553 puts the all-home model at 0.24694.
    assert 0.24 < build_ratings.MAX_HOLDOUT_BRIER < 0.247
    home_rate = build_ratings.load_config(CONFIG)['validation']['home_win_rate']
    all_home_brier = home_rate * (1 - home_rate) ** 2 \
        + (1 - home_rate) * home_rate ** 2
    assert build_ratings.MAX_HOLDOUT_BRIER < all_home_brier


def test_building_ratings_refuses_a_configuration_with_no_validation(tmp_path):
    connection = store_module.connect(tmp_path / 'nba.db')
    rows = [_row()]
    parsed, _ = games_module.parse(rows)
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(rows, 2024))
    store_module.upsert_games(connection, parsed)
    with pytest.raises(ValueError, match='no "validation" section'):
        build_ratings.build(connection, dict(elo=dict(CONF)))


def test_building_ratings_stores_the_chain_and_the_season_ratings(tmp_path):
    connection = store_module.connect(tmp_path / 'nba.db')
    rows = [_row(id=1), _row(id=2, home_score=120, away_score=90,
                             game_date='2024-01-20',
                             date='2024-01-21T00:30Z')]
    parsed, _ = games_module.parse(rows)
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(rows, 2024))
    store_module.upsert_games(connection, parsed)
    payload = build_ratings.load_config(CONFIG)
    summary = build_ratings.build(connection, payload)
    assert summary['counted'] == 2
    assert summary['team_seasons'] == 2
    stored = connection.execute(
        'SELECT count(*) AS n FROM rating_game').fetchone()['n']
    assert stored == 2


def test_the_module_entry_points_run(tmp_path, capsys):
    """A module the refresh invokes needs a test that invokes it."""
    connection = store_module.connect(tmp_path / 'nba.db')
    rows = [_row(id=1), _row(id=2, game_date='2024-01-20',
                             date='2024-01-21T00:30Z')]
    parsed, _ = games_module.parse(rows)
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(rows, 2024))
    store_module.upsert_games(connection, parsed)
    connection.close()
    assert build_ratings.main(['--database', str(tmp_path / 'nba.db')]) == 0
    assert '2 games replayed' in capsys.readouterr().out
    # The fit refuses rather than crashing when there is nothing to hold out.
    assert fit_elo.main(['--database', str(tmp_path / 'nba.db')]) == 1
    assert 'Cannot fit' in capsys.readouterr().err


# ------------------------------------------- the archive, as actually loaded

@pytest.mark.skipif(not Path('db/nba.db').exists(),
                    reason='needs the ingested archive')
def test_every_team_played_exactly_82_in_a_season_with_a_cup_final():
    """The invariant behind the note, in case the source renames it again.

    The four exceptions are real short seasons, not faults: the 2011-12
    lockout, the Boston game cancelled in April 2013, the 2019-20 pandemic
    and the shortened 2020-21.
    """
    import collections
    connection = store_module.connect('db/nba.db')
    short = {2012, 2013, 2020, 2021}
    finals = {row['season'] for row in connection.execute(
        "SELECT DISTINCT season FROM game WHERE season_type = 'cup_final'")}
    assert finals, 'no NBA Cup final was classified at all'
    for season in sorted(finals):
        assert season not in short
        played = collections.Counter()
        for row in connection.execute(
                "SELECT home_team_key, away_team_key FROM game"
                " WHERE season = ? AND season_type = 'regular'"
                "   AND completed = 1", (season,)):
            played[row['home_team_key']] += 1
            played[row['away_team_key']] += 1
        assert set(played.values()) == {82}, (
            f'{season}: teams played {sorted(set(played.values()))} '
            'regular-season games, so the Cup final was misclassified')
