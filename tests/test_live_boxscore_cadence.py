"""The player box-score batch is on its own cadence, not the scoreboard's.

Every tick used to make three CFBD requests -- /scoreboard plus /calendar and
/games/players for the weekly player batch -- so the request cost of the live
scoreboard was three times what the live scoreboard needs. That multiplier is
what makes a faster polling cadence unaffordable.

These tests drive the REAL `refresh()` with a fake urlopen and count the
requests it makes. That matters here specifically: nothing in this suite drove
`refresh()` end to end before, and this repository has twice shipped a broken
entry point because every test exercised the functions underneath it.
"""
from __future__ import annotations

import datetime as dt
import io
import json

import pytest

import fetch_live_scores
from fetch_live_scores import BOXSCORE_INTERVAL, boxscores_due

NOW = dt.datetime(2026, 11, 8, 19, 0, tzinfo=dt.timezone.utc)


def stamp(delta_seconds):
    return (NOW - dt.timedelta(seconds=delta_seconds)).isoformat().replace('+00:00', 'Z')


# --- boxscores_due ----------------------------------------------------------

def test_a_recent_fetch_is_not_due():
    assert boxscores_due({'player_stats_checked_at': stamp(0)}, NOW) is False
    assert boxscores_due({'player_stats_checked_at': stamp(60)}, NOW) is False
    assert boxscores_due({'player_stats_checked_at': stamp(BOXSCORE_INTERVAL - 1)}, NOW) is False


def test_the_interval_is_the_boundary():
    assert boxscores_due({'player_stats_checked_at': stamp(BOXSCORE_INTERVAL)}, NOW) is True
    assert boxscores_due({'player_stats_checked_at': stamp(BOXSCORE_INTERVAL + 1)}, NOW) is True
    assert BOXSCORE_INTERVAL == 900


def test_anything_unreadable_is_due():
    # A missing, blank, unparseable or wrong-typed stamp is not evidence that a
    # fetch happened recently. Guessing the other way means never fetching
    # again, which is a silently empty player panel.
    for previous in [None, {}, {'player_stats_checked_at': None},
                     {'player_stats_checked_at': ''},
                     {'player_stats_checked_at': 'recently'},
                     {'player_stats_checked_at': 12345},
                     {'player_stats_checked_at': '2026-13-45T99:99:99Z'}]:
        assert boxscores_due(previous, NOW) is True, previous


def test_a_future_stamp_is_due_rather_than_sticking_forever():
    # A clock disagreement is not a recent fetch, and treating it as one would
    # wedge the batch off permanently.
    assert boxscores_due({'player_stats_checked_at': stamp(-5)}, NOW) is True
    assert boxscores_due({'player_stats_checked_at': stamp(-86400)}, NOW) is True


def test_a_naive_stamp_is_read_as_utc():
    # Everything this project writes is Z-suffixed, but a stamp that lost its
    # suffix must not raise or be compared against an aware datetime.
    naive = (NOW - dt.timedelta(seconds=60)).replace(tzinfo=None).isoformat()
    assert boxscores_due({'player_stats_checked_at': naive}, NOW) is False


def test_the_interval_is_overridable_without_touching_the_default():
    assert boxscores_due({'player_stats_checked_at': stamp(60)}, NOW, interval=30) is True
    assert boxscores_due({'player_stats_checked_at': stamp(60)}, NOW, interval=120) is False
    assert boxscores_due({'player_stats_checked_at': stamp(60)}, NOW) is False


def test_the_default_interval_is_resolved_inside_the_function(monkeypatch):
    # `def f(interval=BOXSCORE_INTERVAL)` would bind at DEFINITION time, so
    # patching the module constant would change nothing and any test relying on
    # the patch would pass vacuously. That has happened twice in this
    # repository, so it is pinned rather than trusted.
    previous = {'player_stats_checked_at': stamp(60)}
    assert boxscores_due(previous, NOW) is False
    monkeypatch.setattr(fetch_live_scores, 'BOXSCORE_INTERVAL', 30)
    assert boxscores_due(previous, NOW) is True, \
        'the default interval is bound at def time; patching the constant did nothing'


# --- refresh(), driven for real ---------------------------------------------

SCOREBOARD = [{
    'id': 401871071, 'startDate': '2026-11-08T19:00:00Z', 'status': 'in_progress',
    'period': 2, 'clock': '07:21', 'tv': 'CBSSN', 'venue': 'Houchens-Smith Stadium',
    'neutralSite': False,
    'homeTeam': {'id': 98, 'name': 'Western Kentucky', 'points': 21, 'classification': 'fbs'},
    'awayTeam': {'id': 48, 'name': 'Delaware', 'points': 35, 'classification': 'fbs'},
}]

def calendar():
    """A week spanning the real clock.

    `refresh()` reads the wall clock directly, and `fetch_player_boxscores`
    only accepts a week whose window contains that moment -- so a hardcoded
    window silently stops matching and the players request quietly disappears
    from the expected list, which is how a first draft of these tests
    "passed" the one-request assertion for the wrong reason.
    """
    now = dt.datetime.now(dt.timezone.utc)
    iso = lambda d: d.isoformat().replace('+00:00', 'Z')
    return [{'week': 11, 'seasonType': 'regular',
             'startDate': iso(now - dt.timedelta(days=3)),
             'endDate': iso(now + dt.timedelta(days=3))}]

PLAYERS = [{'id': 401871071, 'teams': [{
    'school': 'Western Kentucky', 'homeAway': 'home', 'conference': 'CUSA',
    'categories': [{'name': 'passing', 'types': [{'name': 'YDS', 'athletes': [
        {'id': '1', 'name': 'A Passer', 'stat': '211'}]}]}]}]}]


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


@pytest.fixture
def requests(monkeypatch):
    """Record every URL `refresh()` opens, and answer each one."""
    seen = []

    def fake_urlopen(request, timeout=None):
        url = request.full_url if hasattr(request, 'full_url') else str(request)
        seen.append(url)
        if '/scoreboard' in url:
            body = SCOREBOARD
        elif '/calendar' in url:
            body = calendar()
        elif '/games/players' in url:
            body = PLAYERS
        else:
            raise AssertionError('unexpected request ' + url)
        return FakeResponse(json.dumps(body).encode())

    monkeypatch.setattr(fetch_live_scores, 'urlopen', fake_urlopen)
    monkeypatch.setattr(fetch_live_scores.cfbd_http, 'urlopen', fake_urlopen, raising=False)
    return seen


def paths(seen):
    return [url.split('?')[0].rsplit('/', 1)[-1] for url in seen]


def test_a_first_run_fetches_all_three(tmp_path, requests):
    out = tmp_path / 'live_scores.json'
    snapshot = fetch_live_scores.refresh(out, 'k')
    assert paths(requests) == ['scoreboard', 'calendar', 'players']
    assert snapshot['player_boxscores'], 'the box scores were fetched but not published'
    assert snapshot['player_stats_checked_at']
    assert out.exists()


def test_the_next_tick_fetches_the_scoreboard_ALONE(tmp_path, requests):
    # The whole point: one request per cycle, not three. This is what makes a
    # faster cadence affordable.
    out = tmp_path / 'live_scores.json'
    fetch_live_scores.refresh(out, 'k')
    requests.clear()
    snapshot = fetch_live_scores.refresh(out, 'k')
    assert paths(requests) == ['scoreboard']
    # And the previously published lines are still there, unchanged, with the
    # time they were actually checked -- not restamped as if just fetched.
    assert snapshot['player_boxscores']
    first = json.loads(out.read_text())  # same file, just rewritten
    assert first['player_stats_checked_at'] == snapshot['player_stats_checked_at']


def test_the_batch_returns_once_the_interval_has_passed(tmp_path, requests, monkeypatch):
    out = tmp_path / 'live_scores.json'
    fetch_live_scores.refresh(out, 'k')
    # Age the published stamp past the interval, which is how the real thing
    # decides: from the file, so the five-minute cron and a polling session
    # behave identically with no state held in either.
    published = json.loads(out.read_text())
    published['player_stats_checked_at'] = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=BOXSCORE_INTERVAL + 5)
    ).isoformat().replace('+00:00', 'Z')
    out.write_text(json.dumps(published))
    requests.clear()
    fetch_live_scores.refresh(out, 'k')
    assert paths(requests) == ['scoreboard', 'calendar', 'players']


def test_a_corrupt_snapshot_still_gets_its_box_scores(tmp_path, requests):
    out = tmp_path / 'live_scores.json'
    out.write_text('{ not json')
    fetch_live_scores.refresh(out, 'k')
    assert paths(requests) == ['scoreboard', 'calendar', 'players']


def test_a_failing_batch_never_takes_the_scoreboard_down(tmp_path, monkeypatch):
    out = tmp_path / 'live_scores.json'
    seen = []

    def fake_urlopen(request, timeout=None):
        url = request.full_url
        seen.append(url)
        if '/scoreboard' in url:
            return FakeResponse(json.dumps(SCOREBOARD).encode())
        raise OSError('calendar is down')

    monkeypatch.setattr(fetch_live_scores, 'urlopen', fake_urlopen)
    snapshot = fetch_live_scores.refresh(out, 'k')
    assert snapshot['games'][0]['home']['points'] == 21
    assert snapshot['player_boxscores'] == {}
    # A failed batch leaves no checked-at stamp, so the next tick retries
    # rather than waiting out an interval it never actually used.
    assert 'player_stats_checked_at' not in snapshot
    assert boxscores_due(snapshot, dt.datetime.now(dt.timezone.utc)) is True


def test_a_slate_with_nothing_started_costs_one_request(tmp_path, requests):
    # fetch_player_boxscores returns early when no game has begun, so this was
    # already one request. Pinned so the early exit is not lost.
    out = tmp_path / 'live_scores.json'
    scheduled = [dict(SCOREBOARD[0], status='scheduled')]
    original = fetch_live_scores.make_snapshot
    fetch_live_scores.make_snapshot = lambda payload, now: original(scheduled, now)
    try:
        fetch_live_scores.refresh(out, 'k')
    finally:
        fetch_live_scores.make_snapshot = original
    assert paths(requests) == ['scoreboard']
