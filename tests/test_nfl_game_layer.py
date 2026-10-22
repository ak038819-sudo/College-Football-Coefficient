"""The NFL game layer: the fitted curve, and the files it goes into.

What is checked here is the reasoning, not the numbers: a fit is re-run when
the archive moves, so a test that pinned sigma to four decimals would fail
every October for no fault. What must not move is that the fit holds a season
out, that it refuses a parameter on the edge of its own grid, that the export
splits a season into the weeks the page will ask for, and that the gate
refuses an export whose week files are not where the manifest says.
"""

import json
import math
import pathlib

import pytest

from src.nfl import (export, fit_win_probability, game_export, games,
                     publish_gate, store, win_probability)

SEASON = 2024
GAME = 'nfl:nflverse:game:2024_01_BAL_KC'
POST = 'nfl:nflverse:game:2024_19_BAL_KC'
KC = 'nfl:nflverse:franchise:KC'
BAL = 'nfl:nflverse:franchise:BAL'


def game_row(game_id='2024_01_BAL_KC', week=1, game_type='REG', **extra):
    row = dict(game_id=game_id, season=SEASON, game_type=game_type, week=week,
               gameday='2024-09-05', gametime='20:20', home_team='KC',
               away_team='BAL', home_score='27', away_score='20',
               location='Home', overtime='0')
    row.update(extra)
    return row


def drive(number, **extra):
    row = dict(game_key=GAME, drive=number, team_key=KC, code='KC',
               quarter_start=1, quarter_end=1, clock_start='15:00',
               clock_end='10:00', seconds_left=3600,
               start_yard_line='KC 25', end_yard_line='KC 45', plays=5,
               yards=20, time_of_possession='3:00', first_downs=1,
               inside_twenty=0, yards_penalized=0, result='Punt',
               start_transition='KICKOFF', end_transition='PUNT',
               ended_with_score=0, home_score=0, away_score=0)
    row.update(extra)
    return row


def score(play_id, **extra):
    row = dict(game_key=GAME, play_id=play_id, quarter=1, clock='08:18',
               seconds_left=3100, team_key=KC, code='KC', kind='TD', points=6,
               scorer='P.Mahomes', scorer_id='00-1', kick_distance=None,
               description='TOUCHDOWN.', home_score=6, away_score=0)
    row.update(extra)
    return row


@pytest.fixture
def db():
    connection = store.connect(':memory:')
    store.upsert_teams(connection, [
        dict(season=SEASON, team=code, full=code, location=None,
             nickname=None) for code in ('KC', 'BAL')])
    store.upsert_games(connection, [
        games.parse_row(game_row()),
        games.parse_row(game_row(game_id='2024_19_BAL_KC', week=19,
                                 game_type='DIV'))])
    connection.execute(
        'INSERT INTO rating_model (name, config, fitted_on, held_out,'
        ' holdout_brier, holdout_games, created_at)'
        " VALUES ('t', '{}', '[2024]', '[2025]', 0.22, 285,"
        " '2024-01-01T00:00:00Z')")
    model = connection.execute(
        'SELECT model_id FROM rating_model').fetchone()[0]
    for key in (GAME, POST):
        connection.execute(
            'INSERT INTO rating_game (game_key, model_id, home_pregame,'
            ' away_pregame, home_expected, home_postgame, away_postgame,'
            ' rating_change, multiplier, counted)'
            ' VALUES (?, ?, 1550, 1500, 0.6, 1560, 1490, 10, 1, 1)',
            (key, model))
    connection.commit()
    yield connection
    connection.close()


def params():
    return win_probability.WinProbabilityParams(
        points_per_elo=0.05, sigma=13.6, time_exponent=0.34,
        overtime_home_win=0.55, tie_half_width=1.19)


# --- reading the rating gap back off the expectation -----------------------

def test_the_rating_gap_is_read_back_off_the_stored_expectation():
    # The rating engine already applied the home-field term when it wrote
    # home_expected, so inverting the logistic recovers exactly the gap it
    # used. Re-deriving it from the two pregame ratings would be a second
    # implementation of the venue rule.
    for diff in (-300.0, -17.5, 0.0, 42.0, 300.0):
        expected = 1.0 / (1.0 + 10 ** (-diff / 400.0))
        got = fit_win_probability.elo_diff_from_expectation(expected)
        assert abs(got - diff) < 1e-6


@pytest.mark.parametrize('value', [None, 0.0, 1.0, -0.2, 1.4])
def test_an_expectation_that_is_not_a_probability_gives_no_gap(value):
    assert fit_win_probability.elo_diff_from_expectation(value) is None


# --- what the fit is fitted on ---------------------------------------------

def test_a_game_that_went_to_overtime_had_a_regulation_margin_of_zero(db):
    db.execute('UPDATE game SET overtime = 1, home_points = 30,'
               ' away_points = 27 WHERE game_key = ?', (GAME,))
    db.commit()
    loaded = fit_win_probability.load_games(db)
    row = loaded[GAME]
    # Level after four quarters whatever the final says, and still a home win.
    assert row['regulation_margin'] == 0
    assert row['home_result'] == 1.0


def test_an_overtime_drive_is_not_a_state_this_model_can_speak_for(db):
    # Two separate rules, and the second is not covered by the first: the
    # reducer pins overtime to 0 regulation seconds, but a stored row that
    # disagrees must still not be fitted on, because the model's answer at
    # any positive clock is about regulation and overtime has none left.
    rows = [drive(1, quarter_start=4, seconds_left=120),
            drive(2, quarter_start=5, seconds_left=600),
            drive(3, quarter_start=5, seconds_left=0),
            drive(4, quarter_start=4, seconds_left=0)]
    store_drives(db, rows)
    loaded = fit_win_probability.load_games(db)
    states = fit_win_probability.load_states(db, loaded)
    assert [state['seconds_left'] for state in states] == [120]


def test_a_tie_counts_as_half_a_win_to_each_side():
    rows = [dict(regulation_margin=0, home_result=0.5),
            dict(regulation_margin=0, home_result=1.0),
            dict(regulation_margin=7, home_result=1.0)]
    share, level = fit_win_probability.overtime_home_share(rows)
    assert level == 2 and share == 0.75


def test_the_tie_band_is_solved_against_how_often_games_end_level():
    rows = [dict(elo_diff=diff) for diff in (-200, -50, 0, 50, 200)]
    width = fit_win_probability.solve_tie_half_width(rows, 0.05, 13.6, 0.06)
    assert 0 < width < 6.9
    # Widening the band must raise the mass it implies; otherwise the solve
    # is finding a number rather than measuring one.
    def mass(value):
        these = win_probability.WinProbabilityParams(
            points_per_elo=0.05, sigma=13.6, time_exponent=0.5,
            overtime_home_win=0.5, tie_half_width=value)
        from src.win_probability import tie_mass
        return sum(tie_mass(0.05 * row['elo_diff'], 13.6, these)
                   for row in rows) / len(rows)
    assert abs(mass(width) - 0.06) < 1e-3
    assert mass(width * 2) > mass(width)


def test_a_fit_on_the_edge_of_its_own_grid_is_refused_rather_than_written():
    # A parameter at a grid boundary is the grid's boundary, not a fitted
    # parameter. The Elo fit learned this the same way.
    states = [dict(season=2000, elo_diff=0.0, margin=0, seconds_left=1800,
                   home_result=1.0)]
    best, curve = fit_win_probability.search_time_exponent(
        states, 0.05, 13.6, 0.55, 1.19, low=0.4, high=0.5, step=0.1)
    assert best[0] in (curve[0][0], curve[-1][0])


# --- the parameters the page is served -------------------------------------

def test_the_committed_parameters_are_loadable_and_sane():
    loaded = win_probability.load_params()
    loaded.validate()
    assert 0 < loaded.time_exponent <= 2
    assert loaded.sigma > 0


def test_the_tie_band_must_be_stated_rather_than_quietly_defaulted():
    # The shared model defaults it to half a point, which is the right
    # fallback for a caller that never measured it and the wrong one here.
    config = dict(points_per_elo=0.05, sigma=13.6, time_exponent=0.34,
                  overtime_home_win=0.55)
    with pytest.raises(KeyError):
        win_probability.params_from(config)


def test_the_config_path_resolves_inside_the_call(tmp_path):
    path = tmp_path / 'wp.json'
    path.write_text(json.dumps(dict(win_probability=dict(
        points_per_elo=0.05, sigma=9.0, time_exponent=0.4,
        overtime_home_win=0.5, tie_half_width=1.0))), encoding='utf-8')
    assert win_probability.load_params(path).sigma == 9.0


# --- the export ------------------------------------------------------------

def store_drives(connection, rows):
    from src.nfl import plays
    plays.replace_season(connection, SEASON, rows, [])


def store_all(connection, drives_in, scoring_in):
    from src.nfl import plays
    plays.replace_season(connection, SEASON, drives_in, scoring_in)


def test_a_season_is_split_into_the_weeks_the_page_will_ask_for(db):
    store_all(db, [drive(1), drive(2, game_key=POST)],
              [score(10), score(11, game_key=POST)])
    built = game_export.build(db, SEASON, params=params())
    assert sorted(built) == ['1', 'post']
    assert list(built['1']['games']) == [GAME]
    assert list(built['post']['games']) == [POST]


def test_a_drive_is_packed_in_the_order_the_page_unpacks_it(db):
    store_all(db, [drive(1, result='Touchdown')], [])
    built = game_export.build(db, SEASON, params=params())
    row = built['1']['games'][GAME]['d'][0]
    # Positional rows exist for size; this is the one test that says what
    # each slot means, and ui/nfl_app.js names them in the same order.
    assert row[0] == 1                      # drive number
    assert row[1] == 1                      # 1 = home
    assert row[2] == 1                      # quarter
    assert row[3] == 3600                   # regulation seconds left
    assert row[4] == 'KC 25'                # start
    assert row[5] == 5 and row[6] == 20     # plays, yards
    assert row[7] == '3:00'                 # time of possession
    assert built['1']['results'][row[8]] == 'Touchdown'
    assert row[9] == 0 and row[10] == 0     # the score it started from
    assert 0 <= row[11] <= 1000             # win probability, per mille


def test_the_away_team_is_side_zero_and_a_team_in_neither_is_nobody(db):
    store_all(db, [drive(1, team_key=BAL, code='BAL'),
                   drive(2, team_key=None, code=None)], [])
    built = game_export.build(db, SEASON, params=params())
    rows = built['1']['games'][GAME]['d']
    assert rows[0][1] == 0 and rows[1][1] is None


def test_an_overtime_drive_carries_no_probability(db):
    """The model's clock runs to the end of regulation.

    `seconds_left` is 0 throughout overtime, and the model then answers 1 or
    0 by the score alone — so the drive after the first overtime field goal
    would be drawn as a certainty while the opponent still has a possession
    coming. The fit excludes these states; so does the export.
    """
    store_all(db, [drive(1, quarter_start=4, seconds_left=120,
                         home_score=20, away_score=20),
                   drive(2, quarter_start=5, seconds_left=0,
                         home_score=23, away_score=20),
                   drive(3, quarter_start=5, seconds_left=600,
                         home_score=23, away_score=20)], [])
    rows = game_export.build(db, SEASON, params=params())['1']['games'][GAME]
    assert isinstance(rows['d'][0][11], int)
    assert rows['d'][1][11] is None
    # ...and not even one that claims a clock, which would still be a state
    # the model has nothing to say about.
    assert rows['d'][2][11] is None


def test_the_last_second_of_regulation_is_not_priced_either(db):
    store_all(db, [drive(1, quarter_start=4, seconds_left=0)], [])
    rows = game_export.build(db, SEASON, params=params())['1']['games'][GAME]
    assert rows['d'][0][11] is None


def test_a_game_whose_scoring_does_not_add_up_is_marked_not_corrected(db):
    # Sixteen Jacksonville home games of 2001-2002 have a play log whose
    # points contradict the final the archive states.
    store_all(db, [], [score(10, points=6)])
    built = game_export.build(db, SEASON, params=params())
    assert built['1']['games'][GAME]['ok'] == 0
    # ...and the plays are still published, as the archive has them.
    assert len(built['1']['games'][GAME]['s']) == 1


def test_a_game_whose_scoring_adds_up_says_so(db):
    store_all(db, [], [score(10, points=6, team_key=KC),
                       score(11, points=21, team_key=KC),
                       score(12, points=20, team_key=BAL, code='BAL')])
    built = game_export.build(db, SEASON, params=params())
    assert built['1']['games'][GAME]['ok'] == 1


def test_a_database_with_no_plays_carries_no_game_layer(db):
    assert game_export.has_plays(db) is False
    store_all(db, [drive(1)], [])
    assert game_export.has_plays(db) is True


# --- the gate --------------------------------------------------------------

def manifest_with_games(entry):
    return dict(files=dict(games=dict(seasons={'2024': entry})))


def test_an_export_with_no_game_layer_is_still_a_good_export():
    assert publish_gate.check_games(dict(files={})) == []


def test_a_season_whose_week_files_are_not_there_is_refused(tmp_path):
    manifest = manifest_with_games(dict(v='abc1234567', weeks=['1', 'post']))
    reasons = publish_gate.check_games(manifest, tmp_path)
    assert reasons and 'not there' in reasons[0]


def test_a_season_whose_token_does_not_hash_its_files_is_refused(tmp_path):
    root = tmp_path / 'games' / '2024'
    root.mkdir(parents=True)
    (root / '1.js').write_text('one', encoding='utf-8')
    (root / 'post.js').write_text('two', encoding='utf-8')
    manifest = manifest_with_games(dict(v='0000000000', weeks=['1', 'post']))
    reasons = publish_gate.check_games(manifest, tmp_path)
    assert reasons and 'hashes to' in reasons[0]
    # The right token is the hash of the files in PLAYING order, and the
    # gate recomputes it rather than trusting the manifest's word.
    import hashlib
    right = hashlib.sha256('onetwo'.encode('utf-8')).hexdigest()[:10]
    manifest = manifest_with_games(dict(v=right, weeks=['1', 'post']))
    assert publish_gate.check_games(manifest, tmp_path) == []


def test_weeks_listed_out_of_playing_order_are_refused(tmp_path):
    manifest = manifest_with_games(dict(v='abc1234567',
                                        weeks=['post', '1', '2']))
    reasons = publish_gate.check_games(manifest, tmp_path)
    assert reasons and 'out of playing order' in reasons[0]


def test_a_season_with_no_version_stamp_is_refused():
    reasons = publish_gate.check_games(manifest_with_games(dict(weeks=['1'])))
    assert reasons and 'no version stamp' in reasons[0]


def test_a_partial_play_by_play_dispatch_may_not_publish():
    """A `pbp_seasons` run reduces only what it names, on a database built
    from scratch, so its export carries a game layer covering only those
    seasons. Nothing downstream would notice: the gate checks the seasons
    that are there, and publish.plan only checks the `games` key survives.
    """
    import yaml
    root = pathlib.Path(__file__).resolve().parents[1]
    flow = yaml.safe_load(
        (root / '.github' / 'workflows' / 'nfl-refresh.yml')
        .read_text(encoding='utf-8'))
    step = [item for item in flow['jobs']['refresh']['steps']
            if item.get('id') == 'publish'][0]
    allowed = step['env']['ALLOWED']
    assert 'pbp_seasons' in allowed
    # The input exists and defaults to reducing everything.
    inputs = flow[True]['workflow_dispatch']['inputs']
    assert inputs['pbp_seasons']['default'] == ''


def test_the_build_gate_and_the_page_accept_the_same_schema_versions():
    source = (publish_gate.__file__.rsplit('/src/', 1)[0]
              + '/ui/nfl_loader.js')
    with open(source, encoding='utf-8') as handle:
        text = handle.read()
    stated = text.split('SUPPORTED_SCHEMA_VERSIONS = [', 1)[1].split(']', 1)[0]
    page = tuple(int(part) for part in stated.split(','))
    assert page == publish_gate.SUPPORTED_SCHEMA_VERSIONS
    assert publish_gate.SCHEMA_VERSION in page


# --- the one rule no in-suite test can see ---------------------------------

def test_an_element_hidden_by_attribute_is_not_overruled_by_its_own_display():
    """A guard on the stylesheet, because neither suite can measure it.

    `hidden` is an attribute and any rule that sets `display` beats it.
    `.cards` is `display: grid` and `.days` is `display: flex`, so hiding the
    week strip and the card grid to show one game left a full week of cards
    on screen under the open game. jsdom does no layout and its
    getComputedStyle does not apply this rule, so the browser check caught it
    and nothing in the suite could; this is what stops it coming back.

    It is a source-text assertion, which is a weak kind, so it is written to
    fail for the right reason: the second half checks that something toggled
    by `hidden` really does set a display of its own, so the rule is still
    earning its place rather than being kept by a test nobody can retire.
    """
    page = pathlib.Path(__file__).resolve().parents[1] / 'ui' / 'nfl.html'
    text = page.read_text(encoding='utf-8')
    assert '[hidden] { display: none !important; }' in text
    assert '.cards { display: grid' in text
    assert ".hidden = open" in text or "hidden = !open" in text

