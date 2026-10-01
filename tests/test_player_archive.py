from fetch_cfbd_players import merge_archive, fetch_season
from model_refresh_needed import needs_refresh
from datetime import datetime, timezone


def test_archived_box_score_survives_empty_retry():
    saved = {'42': [{'name': 'BYU', 'categories': []}]}
    assert merge_archive(saved, {}) == saved
    assert merge_archive(saved, {'42': []}) == saved


def test_recent_final_retries_missing_efficiency_and_players():
    snapshot = {'games': [{'id': 42, 'status': 'completed', 'start_date': '2026-09-26T22:00:00Z',
                           'home': {'classification': 'fbs', 'points': 30},
                           'away': {'classification': 'fbs', 'points': 20}}]}
    rows = [{'game_id': '42', 'home_score': '30', 'away_score': '20'}]
    now = datetime(2026, 9, 27, tzinfo=timezone.utc)
    assert needs_refresh(snapshot, rows, 2026, set(), {'42'}, now)
    assert needs_refresh(snapshot, rows, 2026, {'42'}, set(), now)
    assert not needs_refresh(snapshot, rows, 2026, {'42'}, {'42'}, now)
    assert not needs_refresh(snapshot, rows, 2026, set(), set(),
                             datetime(2026, 10, 1, tzinfo=timezone.utc))


def test_january_final_belongs_to_previous_football_season():
    snapshot = {'games': [{'id': 77, 'status': 'completed', 'start_date': '2027-01-15T01:00:00Z',
                           'home': {'classification': 'fbs', 'points': 24},
                           'away': {'classification': 'fbs', 'points': 21}}]}
    assert needs_refresh(snapshot, [], 2026)


def test_player_backfill_queries_only_started_weeks(monkeypatch):
    calls = []
    def get(path, params, key):
        calls.append((path, params))
        if path == '/calendar':
            return [{'week': 1, 'seasonType': 'regular', 'startDate': '2026-08-27T00:00:00Z'},
                    {'week': 2, 'seasonType': 'regular', 'startDate': '2026-10-08T00:00:00Z'}]
        return []
    monkeypatch.setattr('fetch_cfbd_players.get_json', get)
    archive = fetch_season(2026, 'test', {'42': [{'name': 'BYU'}]},
                           datetime(2026, 10, 1, tzinfo=timezone.utc))
    assert archive['42']
    assert len([path for path, _ in calls if path == '/games/players']) == 1
