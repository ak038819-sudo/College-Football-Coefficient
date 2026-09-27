import datetime as dt
import pytest
from fetch_live_scores import make_snapshot, clean_player_boxscores, merge_player_boxscores


def sample(status='in_progress'):
    return {'id': 42, 'startDate': '2026-09-26T19:00:00Z', 'status': status,
            'period': 3, 'clock': '06:15', 'tv': 'ESPN',
            'homeTeam': {'id': 1, 'name': 'BYU', 'points': 21, 'classification': 'fbs', 'winProbability': .7},
            'awayTeam': {'id': 2, 'name': 'Utah', 'points': 17, 'classification': 'fbs'}}


def test_snapshot_strips_non_score_data():
    result = make_snapshot([sample()], dt.datetime(2026, 9, 26, tzinfo=dt.timezone.utc))
    assert result['games'][0]['home']['points'] == 21
    assert result['games'][0]['status'] == 'in_progress'
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
