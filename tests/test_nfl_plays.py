"""Reducing the NFL play-by-play to drives and scoring plays.

Every fixture here is small and made up; every RULE one of them pins was
measured against the whole archive, 1999-2026, and the measurement is written
down where the code applies it.
"""

import gzip
import io
import urllib.error

import pytest

from src.nfl import games, pbp_source, plays, store

GAME = 'nfl:nflverse:game:2024_01_BAL_KC'


def team_rows():
    return [dict(season=2024, team=code, full=code, location=None,
                 nickname=None) for code in ('KC', 'BAL', 'SF', 'ARI')]


@pytest.fixture
def db():
    connection = store.connect(':memory:')
    store.upsert_teams(connection, team_rows())
    store.upsert_games(connection, [games.parse_row(game_row())])
    yield connection
    connection.close()


def game_row(**extra):
    row = dict(game_id='2024_01_BAL_KC', season=2024, game_type='REG',
               week=1, gameday='2024-09-05', gametime='20:20',
               home_team='KC', away_team='BAL', home_score='27',
               away_score='20', location='Home', overtime='0')
    row.update(extra)
    return row


def play(**extra):
    row = {name: '' for name in plays.COLUMNS}
    row.update(game_id='2024_01_BAL_KC', play_id='1', fixed_drive='1',
               posteam='BAL', defteam='KC', home_team='KC', away_team='BAL',
               qtr='1', time='15:00', game_seconds_remaining='3600',
               total_home_score='0', total_away_score='0')
    row.update(extra)
    return row


# --- who scored ------------------------------------------------------------

def test_the_scoreboard_decides_who_scored_not_the_team_columns():
    # A safety on a punt: the punting team scores, and `defteam` names the
    # RECEIVING team. Reducing 1999-2026 on the team columns alone left 29
    # games whose points did not add up to the final; this is the case that
    # closed eleven of them.
    row = play(sp='1', safety='1', posteam='BAL', defteam='KC',
               total_home_score='0', total_away_score='2')
    assert plays.scored_by(row, 'SAFETY', (0, 0)) == 'BAL'
    # ...and the columns on their own would have said the other team.
    assert plays.named_scoring_team(row, 'SAFETY') == 'KC'


def test_a_scrimmage_safety_still_goes_to_the_defence():
    row = play(sp='1', safety='1', posteam='KC', defteam='BAL',
               total_home_score='0', total_away_score='2')
    assert plays.scored_by(row, 'SAFETY', (0, 0)) == 'BAL'


def test_a_pick_six_is_the_defence_s_touchdown():
    row = play(sp='1', touchdown='1', posteam='BAL', defteam='KC',
               td_team='KC', total_home_score='6', total_away_score='0')
    assert plays.scored_by(row, 'TD', (0, 0)) == 'KC'


def test_a_score_the_delta_cannot_name_falls_back_to_the_columns():
    # Both totals move, which names nobody. The archive's own running score
    # is wrong for sixteen Jacksonville home games of 2001-2002 in exactly
    # this way, and the fallback is what keeps those games readable at all.
    row = play(sp='1', field_goal_result='made', posteam='KC', defteam='BAL',
               total_home_score='3', total_away_score='3')
    assert plays.scored_by(row, 'FG', (0, 0)) == 'KC'


def test_the_first_row_of_a_game_has_no_delta_to_read():
    row = play(sp='1', field_goal_result='made', posteam='BAL',
               total_home_score='0', total_away_score='3')
    assert plays.scored_by(row, 'FG', None) == 'BAL'


@pytest.mark.parametrize('column,value,kind', [
    ('touchdown', '1', 'TD'),
    ('field_goal_result', 'made', 'FG'),
    ('extra_point_result', 'good', 'XP'),
    ('two_point_conv_result', 'success', '2PT'),
    ('safety', '1', 'SAFETY'),
    ('defensive_two_point_conv', '1', 'D2PT'),
])
def test_every_kind_of_score_is_classified(column, value, kind):
    assert plays.classify(play(**{column: value})) == kind


def test_a_missed_field_goal_is_not_a_score():
    assert plays.classify(play(field_goal_result='missed')) is None


# --- the clock -------------------------------------------------------------

def test_seconds_left_uses_the_source_s_own_countdown():
    assert plays.seconds_left('1234', '2') == 1234


def test_overtime_has_no_regulation_seconds_left():
    assert plays.seconds_left('', '5') == 0


def test_a_blank_countdown_falls_back_to_the_end_of_the_quarter():
    # The quarter is a FLOOR, not an estimate. With no clock, all this knows
    # is that the third quarter had not ended, so it says the least time that
    # could remain rather than inventing a position within the quarter.
    assert plays.seconds_left('', '3') == 900


def test_a_countdown_past_regulation_is_clamped_to_kickoff():
    assert plays.seconds_left('99999', '1') == 3600


def test_the_clock_is_stripped_from_a_description_it_already_states():
    assert plays.strip_clock('(7:54) 8-L.Jackson pass short left.') \
        == '8-L.Jackson pass short left.'


@pytest.mark.parametrize('text', [
    '(Shotgun) 15-P.Mahomes pass deep right for 35 yards, TOUCHDOWN.',
    '(Punt formation) 10-B.Mann right end tackled in End Zone, SAFETY.',
    '(No Huddle) 8-L.Jackson kneels.',
])
def test_a_description_that_opens_with_something_else_is_left_alone(text):
    # A first version took anything parenthesised in the first nine
    # characters, which ate the "(Shotgun) " off every description with no
    # clock in front of it.
    assert plays.strip_clock(text) == text


# --- reducing --------------------------------------------------------------

def test_a_drive_takes_its_fields_from_wherever_they_are_stated():
    # THE DRIVE FIELDS ARE NOT ON THE DRIVE'S FIRST PLAY. Reading the opening
    # row gives a drive with a result and no start, which is what the first
    # version of this module did.
    rows = [
        play(play_id='1', drive_start_yard_line='', drive_play_count='',
             fixed_drive_result=''),
        play(play_id='2', drive_start_yard_line='BAL 30',
             drive_play_count='11', fixed_drive_result='Touchdown'),
    ]
    drives, _, _ = plays.reduce_season(rows)
    assert len(drives) == 1
    assert drives[0]['start_yard_line'] == 'BAL 30'
    assert drives[0]['plays'] == 11
    assert drives[0]['result'] == 'Touchdown'


def test_a_drive_s_score_is_the_score_it_started_from():
    rows = [
        play(play_id='1', total_home_score='7', total_away_score='3'),
        play(play_id='2', total_home_score='14', total_away_score='3'),
    ]
    drives, _, _ = plays.reduce_season(rows)
    assert (drives[0]['home_score'], drives[0]['away_score']) == (7, 3)


def test_a_drive_with_no_possessing_team_is_counted_not_dropped():
    rows = [play(play_id='1', posteam='', fixed_drive_result='Punt')]
    drives, _, counts = plays.reduce_season(rows)
    assert len(drives) == 1 and drives[0]['team_key'] is None
    assert counts['no_team'] == 1


def test_a_score_this_module_cannot_classify_is_counted_not_guessed():
    rows = [play(play_id='9', sp='1')]
    _, scoring, counts = plays.reduce_season(rows)
    assert scoring == [] and counts['unclassified'] == 1


# --- storing ---------------------------------------------------------------

def rows_for(connection, table):
    return connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]


def test_a_drive_that_has_stopped_existing_stops_existing_here(db):
    first = [dict(game_key=GAME, drive=n, team_key=None, code=None,
                  quarter_start=1, quarter_end=1, clock_start=None,
                  clock_end=None, seconds_left=3600, start_yard_line=None,
                  end_yard_line=None, plays=3, yards=10,
                  time_of_possession=None, first_downs=0, inside_twenty=0,
                  yards_penalized=0, result='Punt', start_transition=None,
                  end_transition=None, ended_with_score=0, home_score=0,
                  away_score=0) for n in (1, 2)]
    plays.replace_season(db, 2024, first, [])
    assert rows_for(db, 'game_drive') == 2
    # The source revised the game down to one drive. An upsert would leave a
    # phantom drive 2 behind.
    plays.replace_season(db, 2024, first[:1], [])
    assert rows_for(db, 'game_drive') == 1


def test_a_drive_for_an_unknown_game_is_dropped_and_counted(db):
    drive = dict(game_key='nfl:nflverse:game:nope', drive=1, team_key=None,
                 code=None, quarter_start=1, quarter_end=1, clock_start=None,
                 clock_end=None, seconds_left=3600, start_yard_line=None,
                 end_yard_line=None, plays=1, yards=0,
                 time_of_possession=None, first_downs=0, inside_twenty=0,
                 yards_penalized=0, result='Punt', start_transition=None,
                 end_transition=None, ended_with_score=0, home_score=0,
                 away_score=0)
    stored = plays.replace_season(db, 2024, [drive], [])
    assert stored['drives'] == 0 and stored['dropped_drives'] == 1
    assert rows_for(db, 'game_drive') == 0


# --- the source ------------------------------------------------------------

def test_a_truncated_download_is_refused_rather_than_read_short(tmp_path):
    path = tmp_path / 'pbp.csv'
    path.write_text('game_id,play_id\n2024_01_BAL_KC\n', encoding='utf-8')
    with pytest.raises(pbp_source.NflSourceError):
        list(pbp_source.rows(path, ('game_id', 'play_id')))


def test_only_the_columns_asked_for_are_read(tmp_path):
    path = tmp_path / 'pbp.csv'
    path.write_text('game_id,play_id,desc\nA,1,hello\n', encoding='utf-8')
    got = list(pbp_source.rows(path, ('game_id', 'desc')))
    assert got == [{'game_id': 'A', 'desc': 'hello'}]


def test_a_download_larger_than_the_cap_is_refused(tmp_path):
    body = b'x' * 64

    class Response:
        status = 200

        def read(self, size):
            chunk, body_rest = body[:size], body[size:]
            return chunk if not hasattr(self, 'done') else b''

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    def opener(request, timeout=None):
        return Response()

    with pytest.raises(pbp_source.NflSourceError):
        pbp_source.download('https://example/pbp.csv', tmp_path / 'out.csv',
                            opener=opener, sleep=lambda _: None,
                            max_bytes=16)
    assert not (tmp_path / 'out.csv').exists()


# --- the week a game's detail lives in -------------------------------------

def test_the_postseason_is_one_file_however_many_rounds_it_ran_to():
    from src.nfl import game_export
    assert game_export.week_key(dict(week=19, postseason=1)) == 'post'
    assert game_export.week_key(dict(week=5, postseason=0)) == '5'
    assert game_export.week_key(dict(week=None, postseason=0)) == 'post'


def test_weeks_are_ordered_as_a_season_is_played():
    from src.nfl import game_export
    assert game_export.week_order(['post', '10', '2', '1']) \
        == ['1', '2', '10', 'post']
