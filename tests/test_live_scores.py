import datetime as dt
import pytest
from fetch_live_scores import make_snapshot, clean_player_boxscores, merge_player_boxscores


def sample(status='in_progress'):
    return {'id': 42, 'startDate': '2026-09-26T19:00:00Z', 'status': status,
            'period': 3, 'clock': '06:15', 'tv': 'ESPN', 'venue': 'LaVell Edwards Stadium',
            'homeTeam': {'id': 1, 'name': 'BYU', 'points': 21, 'classification': 'fbs', 'winProbability': .7},
            'awayTeam': {'id': 2, 'name': 'Utah', 'points': 17, 'classification': 'fbs'}}


def test_snapshot_strips_non_score_data():
    result = make_snapshot([sample()], dt.datetime(2026, 9, 26, tzinfo=dt.timezone.utc))
    assert result['games'][0]['home']['points'] == 21
    assert result['games'][0]['status'] == 'in_progress'
    assert result['games'][0]['venue'] == 'LaVell Edwards Stadium'
    assert 'winProbability' not in str(result)


def test_rejects_malformed_feed():
    with pytest.raises(ValueError):
        make_snapshot({'games': []}, dt.datetime.now(dt.timezone.utc))
    with pytest.raises(ValueError):
        make_snapshot([sample(), sample()], dt.datetime.now(dt.timezone.utc))
    with pytest.raises(ValueError):
        make_snapshot([sample('unknown')], dt.datetime.now(dt.timezone.utc))


def test_player_boxscore_keeps_current_game_and_display_lines():
    payload = [{'id': 42, 'teams': [{'team': 'BYU', 'homeAway': 'home',
        'categories': [{'name': 'passing', 'types': [{'name': 'YDS',
        'athletes': [{'id': '7', 'name': 'Quarterback', 'stat': '288'}]}]}]}]},
        {'id': 99, 'teams': []}]
    result = clean_player_boxscores(payload, {42})
    assert list(result) == ['42']
    assert result['42'][0]['categories'][0]['lines'] == [{'name': 'Quarterback', 'stat': '288'}]


def test_player_stats_survive_empty_or_failed_refresh():
    game = sample('completed')
    snapshot = make_snapshot([game], dt.datetime(2026, 9, 26, tzinfo=dt.timezone.utc))
    lines = clean_player_boxscores([{'id': 42, 'teams': [{'team': 'BYU',
        'categories': [{'name': 'passing', 'types': [{'name': 'YDS',
        'athletes': [{'name': 'Quarterback', 'stat': '288'}]}]}]}]}], {42})
    assert clean_player_boxscores([{'id': 42, 'teams': []}], {42}) == {}
    previous = merge_player_boxscores(snapshot, {}, lines, '2026-09-26T22:00:00Z')
    new_snapshot = make_snapshot([game], dt.datetime(2026, 9, 27, tzinfo=dt.timezone.utc))
    result = merge_player_boxscores(new_snapshot, previous, {}, '2026-09-27T00:00:00Z')
    assert result['player_boxscores'] == lines
    assert result['player_boxscore_times']['42'] == '2026-09-26T22:00:00Z'
    assert result['player_stats_checked_at'] == '2026-09-27T00:00:00Z'
    failed = merge_player_boxscores(make_snapshot([game], dt.datetime.now(dt.timezone.utc)),
                                    result, None, '2026-09-27T01:00:00Z')
    assert failed['player_boxscores'] == lines
    assert failed['player_stats_checked_at'] == '2026-09-27T00:00:00Z'


def test_the_live_scoreboard_carries_no_athlete_ids():
    """Every visitor downloads this file on every page load. Measured on the
    real file: 7,410 player lines, so ids would add about 96 KB to 358 KB -- for
    in-progress games whose lines the roster-name rule already links. The
    archive, which is gzipped and read only when a game page opens, asks for
    them instead."""
    payload = [{'id': 42, 'teams': [{'team': 'BYU', 'homeAway': 'home',
        'categories': [{'name': 'passing', 'types': [{'name': 'YDS',
        'athletes': [{'id': 4361182, 'name': 'Quarterback', 'stat': '288'}]}]}]}]}]
    [line] = clean_player_boxscores(payload, {42})['42'][0]['categories'][0]['lines']
    assert line == {'name': 'Quarterback', 'stat': '288'}


def test_the_archive_keeps_the_athlete_id_so_a_line_can_be_attributed():
    """The reason for the whole re-fetch. Without an id a box-score line can
    only be matched to a person by name, which is a guess the project refuses to
    present as attribution."""
    payload = [{'id': 42, 'teams': [{'team': 'BYU', 'homeAway': 'home',
        'categories': [{'name': 'passing', 'types': [{'name': 'YDS', 'athletes': [
            {'id': 4361182, 'name': 'Numbered', 'stat': '288'},
            {'name': 'Unnumbered', 'stat': '12'},
            {'id': 'team-total', 'name': 'Not A Number', 'stat': '300'},
        ]}]}]}]}]
    lines = clean_player_boxscores(payload, {42}, max_athletes=None,
                                   keep_ids=True)['42'][0]['categories'][0]['lines']
    assert lines[0] == {'name': 'Numbered', 'stat': '288', 'id': '4361182'}
    # No key at all, not a null: a reader has to be able to tell "CFBD did not
    # number this line" from "this line is numbered 0".
    assert lines[1] == {'name': 'Unnumbered', 'stat': '12'}
    assert 'id' not in lines[1]
    # An id that is not a number cannot be a CFBD athlete id, and storing a
    # name-shaped one would invite the name matching the id exists to replace.
    assert lines[2] == {'name': 'Not A Number', 'stat': '300'}


def test_a_negative_athlete_id_is_kept():
    """29,162 of CFBD's athlete ids are negative and this project uses the id
    verbatim as player_id, so dropping the sign would point a line at a
    different person or at nobody."""
    payload = [{'id': 42, 'teams': [{'team': 'BYU', 'categories': [{'name': 'passing',
        'types': [{'name': 'YDS', 'athletes': [
            {'id': -1044305, 'name': 'Placeholder', 'stat': '1'}]}]}]}]}]
    [line] = clean_player_boxscores(payload, {42}, max_athletes=None,
                                    keep_ids=True)['42'][0]['categories'][0]['lines']
    assert line['id'] == '-1044305'
