"""Basketball ingestion contract tests. No network, no football database.

Synthetic responses cannot establish real provider coverage; they pin the rules
that decide whether a real response is trusted.
"""

from datetime import date
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb import games as games_module
from src.ncaambb import http as http_module
from src.ncaambb.coverage import classify_game, render, summarize_season
from src.ncaambb.games import (GAMES_RECORD_CAP, fetch_season, fetch_window,
                               game_day, halve, merge, month_windows,
                               season_window)
from src.ncaambb.http import CbbdAuthError, CbbdError, Quota, get_json
from src.ncaambb.probe_coverage import main as probe_main, probe


def game(identifier=1, day='2025-11-07T19:00:00Z', home=10, away=20,
         status='final', home_points=70, away_points=60, season_type='regular'):
    return dict(id=identifier, startDate=day, homeTeamId=home, awayTeamId=away,
                status=status, homePoints=home_points, awayPoints=away_points,
                seasonType=season_type, neutralSite=False, startTimeTbd=False)


# --- seasons ---------------------------------------------------------------

def test_november_and_march_belong_to_the_same_ending_year_season():
    first, last = season_window(2026)
    assert first <= date(2025, 11, 7) <= last
    assert first <= date(2026, 3, 20) <= last
    # The previous season's November is outside this one.
    assert not first <= date(2024, 11, 7) <= last
    with pytest.raises(ValueError):
        season_window('2026')


def test_month_windows_tile_the_season_without_gaps_or_overlap():
    first, last = season_window(2026)
    windows = month_windows(first, last)
    assert windows[0][0] == first and windows[-1][1] == last
    for (_, end), (start, _) in zip(windows, windows[1:]):
        assert (start - end).days == 1
    assert len(windows) == 12


def test_a_naive_or_missing_start_date_is_not_a_date():
    assert game_day(game()) == date(2025, 11, 7)
    assert game_day(game(day='2025-11-07T19:00:00')) is None
    assert game_day(game(day='')) is None
    assert game_day(game(day='not a date')) is None
    assert game_day({}) is None


def test_a_late_utc_tip_off_keeps_its_own_utc_date():
    # 2:30 UTC on the 8th is the evening of the 7th locally. The window is UTC,
    # so the game belongs to the 8th here and must not be reassigned.
    assert game_day(game(day='2025-11-08T02:30:00Z')) == date(2025, 11, 8)


# --- record cap and partitioning ------------------------------------------

def test_a_window_at_the_record_cap_is_split_rather_than_trusted():
    requested = []

    def getter(path, params, **kwargs):
        first = date.fromisoformat(params['startDateRange'][:10])
        last = date.fromisoformat(params['endDateRange'][:10])
        requested.append((first, last))
        if (last - first).days > 10:
            return [game(i, f'{first.isoformat()}T19:00:00Z')
                    for i in range(GAMES_RECORD_CAP)]
        return [game(1000 + len(requested), f'{first.isoformat()}T19:00:00Z')]

    rows, windows = fetch_window(2026, date(2025, 11, 1), date(2025, 11, 30),
                                 getter=getter)
    assert windows > 1
    assert len(rows) < GAMES_RECORD_CAP
    assert requested[0] == (date(2025, 11, 1), date(2025, 11, 30))


def test_a_single_day_at_the_cap_fails_instead_of_truncating():
    def getter(path, params, **kwargs):
        first = date.fromisoformat(params['startDateRange'][:10])
        return [game(i, f'{first.isoformat()}T19:00:00Z')
                for i in range(GAMES_RECORD_CAP)]

    with pytest.raises(CbbdError, match='cannot be split further'):
        fetch_window(2026, date(2025, 11, 7), date(2025, 11, 7), getter=getter)


def test_halve_refuses_a_single_day():
    assert halve(date(2025, 11, 1), date(2025, 11, 2)) == \
        ((date(2025, 11, 1), date(2025, 11, 1)), (date(2025, 11, 2), date(2025, 11, 2)))
    with pytest.raises(ValueError):
        halve(date(2025, 11, 1), date(2025, 11, 1))


def test_a_game_outside_the_requested_window_is_refused():
    def getter(path, params, **kwargs):
        return [game(1, '2026-02-01T19:00:00Z')]

    with pytest.raises(CbbdError, match='cannot be trusted'):
        fetch_window(2026, date(2025, 11, 1), date(2025, 11, 30), getter=getter)


# --- duplicates ------------------------------------------------------------

def test_an_identical_duplicate_merges_and_a_conflicting_one_is_refused():
    assert len(merge([game(1), game(1), game(2)])) == 2
    with pytest.raises(CbbdError, match='conflicting duplicates'):
        merge([game(1), game(1, home_points=71)])


def test_a_game_without_an_integer_id_is_refused():
    with pytest.raises(CbbdError, match='integer id'):
        merge([game('x')])


def test_fetch_season_reports_the_windows_it_requested():
    def getter(path, params, **kwargs):
        first = date.fromisoformat(params['startDateRange'][:10])
        if first.month != 11:
            return []
        return [game(1, '2025-11-07T19:00:00Z'), game(1, '2025-11-07T19:00:00Z')]

    collected, stats = fetch_season(2026, getter=getter)
    assert stats == dict(season=2026, games=1, rows_returned=2,
                         windows_requested=12, duplicates=1)
    assert len(collected) == 1


# --- http ------------------------------------------------------------------

class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def http_error(code):
    return HTTPError('https://example.invalid', code, 'boom', {}, None)


def test_an_access_failure_aborts_the_probe_instead_of_reporting_no_coverage():
    # Reporting a rejected key as an empty season would read as "the provider
    # has no games", which is the opposite of what happened.
    def getter(path, params, **kwargs):
        raise CbbdAuthError('HTTP 401')

    with pytest.raises(CbbdAuthError):
        probe([2025, 2026], getter=getter)


def test_a_data_failure_is_recorded_and_the_other_seasons_still_run():
    def getter(path, params, **kwargs):
        if path == '/teams':
            return [dict(id=10), dict(id=20)]
        first = date.fromisoformat(params['startDateRange'][:10])
        if first.year == 2024:
            raise CbbdError('window could not be read')
        if first.month != 11:
            return []
        return [game(1, '2025-11-07T19:00:00Z')]

    summaries, problems = probe([2025, 2026], getter=getter)
    assert [row['season'] for row in summaries] == [2026]
    assert problems == [dict(season=2025, error='window could not be read')]


def test_the_probe_cli_exits_two_on_an_access_failure(monkeypatch, capsys):
    monkeypatch.delenv('CBBD_API_KEY', raising=False)
    assert probe_main(['--seasons', '2026']) == 2
    assert 'Cannot reach the provider' in capsys.readouterr().err


def test_an_authorization_error_is_never_retried():
    calls, slept = [], []

    def opener(request, timeout=None):
        calls.append(1)
        raise http_error(401)

    with pytest.raises(CbbdAuthError, match='HTTP 401'):
        get_json('/games', key='k', sleep=slept.append, opener=opener)
    assert len(calls) == 1
    assert slept == []


def test_a_forbidden_path_is_an_access_failure_not_a_data_failure():
    def opener(request, timeout=None):
        raise http_error(403)

    with pytest.raises(CbbdAuthError, match='not entitled'):
        get_json('/lineups/game/1', key='k', sleep=lambda _: None, opener=opener)


def test_a_missing_path_is_never_retried():
    calls = []

    def opener(request, timeout=None):
        calls.append(1)
        raise http_error(404)

    with pytest.raises(CbbdError, match='HTTP 404'):
        get_json('/nope', key='k', sleep=lambda _: None, opener=opener)
    assert len(calls) == 1


def test_rate_limiting_backs_off_and_then_succeeds():
    calls, slept = [], []

    def opener(request, timeout=None):
        calls.append(1)
        if len(calls) < 3:
            raise http_error(429)
        return FakeResponse(b'[{"id": 1}]')

    quota = Quota()
    rows = get_json('/games', key='k', quota=quota, sleep=slept.append,
                    opener=opener)
    assert rows == [{'id': 1}]
    assert len(calls) == 3
    assert slept == [2.0, 4.0]
    assert quota.as_dict() == dict(requests=3, retries=2)


def test_a_connection_failure_exhausts_attempts_and_reports_the_count():
    calls, slept = [], []

    def opener(request, timeout=None):
        calls.append(1)
        raise URLError('down')

    with pytest.raises(CbbdError, match='after 3 attempts'):
        get_json('/games', key='k', attempts=3, sleep=slept.append, opener=opener)
    assert len(calls) == 3
    assert len(slept) == 2


def test_a_non_array_payload_is_refused():
    def opener(request, timeout=None):
        return FakeResponse(b'{"id": 1}')

    with pytest.raises(CbbdError, match='JSON array'):
        get_json('/games', key='k', sleep=lambda _: None, opener=opener)


def test_the_sleep_function_is_resolved_inside_the_call(monkeypatch):
    # A def-time default would bind the real time.sleep and make this pass
    # while waiting out the backoff for real.
    slept = []
    monkeypatch.setattr(http_module.time, 'sleep', slept.append)
    calls = []

    def opener(request, timeout=None):
        calls.append(1)
        if len(calls) < 2:
            raise http_error(503)
        return FakeResponse(b'[]')

    assert get_json('/games', key='k', opener=opener) == []
    assert slept == [2.0]


def test_a_missing_key_is_reported_without_reaching_the_network(monkeypatch):
    monkeypatch.delenv('CBBD_API_KEY', raising=False)
    with pytest.raises(CbbdAuthError, match='CBBD_API_KEY is not set'):
        get_json('/games', opener=lambda *a, **k: pytest.fail('no request'))


# --- coverage --------------------------------------------------------------

DIVISION_ONE = {10, 20, 30}


def test_a_clean_final_between_division_one_teams_is_rateable():
    row = classify_game(game(), DIVISION_ONE)
    assert row['rateable'] and row['division_one'] and row['exclusions'] == []


def test_a_non_division_one_opponent_is_retained_but_excluded():
    row = classify_game(game(away=999), DIVISION_ONE)
    assert not row['rateable']
    assert not row['division_one']
    assert row['exclusions'] == ['non-Division I opponent']


def test_preseason_is_counted_but_never_rateable():
    row = classify_game(game(season_type='preseason'), DIVISION_ONE)
    assert row['exclusions'] == []
    assert not row['rateable']


@pytest.mark.parametrize('change, reason', [
    (dict(status='scheduled'), None),
    (dict(home_points=None), 'final without a usable score'),
    (dict(away_points=70), 'final recorded as a tie'),
    (dict(status='rained off'), 'unknown status'),
    (dict(season_type='friendly'), 'unknown season type'),
    (dict(away=10), 'team plays itself'),
    (dict(day='2025-11-07T19:00:00'), 'missing or naive start date'),
])
def test_unusable_games_are_named_rather_than_dropped(change, reason):
    row = classify_game(game(**change), DIVISION_ONE)
    assert not row['rateable']
    if reason is not None:
        assert reason in row['exclusions']


def test_every_game_is_accounted_for_in_the_summary():
    rows = [game(1), game(2, status='scheduled', home_points=None, away_points=None),
            game(3, status='postponed', home_points=None, away_points=None),
            game(4, status='cancelled', home_points=None, away_points=None),
            game(5, away=999), game(6, status='in_progress', home_points=30,
                                    away_points=28),
            game(7, season_type='preseason')]
    summary = summarize_season(2026, rows, DIVISION_ONE)
    assert summary['games'] == 7
    assert summary['rateable'] == 1
    assert summary['finals'] == 3
    assert summary['finals_not_rateable'] == 2
    assert summary['unplayed'] == 2
    assert summary['postponed_or_cancelled'] == 2
    assert summary['teams_outside_division_one'] == 1
    assert summary['exclusions'] == {'non-Division I opponent': 1}
    assert summary['by_season_type'] == {'preseason': 1, 'regular': 6}


def test_an_empty_season_summarizes_as_empty_rather_than_failing():
    summary = summarize_season(2027, [], set())
    assert summary['games'] == 0 and summary['rateable'] == 0
    assert summary['exclusions'] == {}
    assert 'Season 2027' in '\n'.join(render([summary]))


def test_the_rendered_report_names_the_exclusions():
    summary = summarize_season(2026, [game(5, away=999)], DIVISION_ONE)
    text = '\n'.join(render([summary]))
    assert 'non-Division I opponent: 1' in text
    assert '| 2026 | 1 | 1 | 0 |' in text
