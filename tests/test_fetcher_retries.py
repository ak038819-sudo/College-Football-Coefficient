"""Every CFBD fetcher, driven through a connection reset.

`with_retries` existed before this and was wired into two call sites: the
player-archive fetch and the deploy's /games fetch. The other nine treated a
reset as final. Eight of them fail soft, which sounds harmless and is not: the
run keeps the committed snapshot, says so in a warning nobody reads, and a
season of fresh data is simply missing until someone dispatches the workflow
again. The two that do not fail soft are worse -- the memberships fetch ends
the run with a traceback, and the live scoreboard leaves a five-minute hole in
a feed people are watching.

So these tests drive each fetcher with a `requests` that resets once, and
assert the call was repeated and the rows came back. They patch the module in
sys.modules rather than the function, because the whole point of the helper is
that the import happens inside it.
"""
from __future__ import annotations

import sys
import types
import urllib.error

import pytest

import cfbd_http


class Reset(Exception):
    """requests' own ConnectionError, recognised by module and name."""


Reset.__module__ = 'requests.exceptions'
Reset.__name__ = 'ConnectionError'


class Answered(Exception):
    """requests' HTTPError, which carries the status the API replied with."""


Answered.__module__ = 'requests.exceptions'
Answered.__name__ = 'HTTPError'


def fake_requests(payload, *, resets=1, status=None):
    """A requests module that resets `resets` times, then answers."""
    calls = []

    class Response:
        def __init__(self):
            self.status_code = status or 200

        def raise_for_status(self):
            if status is not None:
                error = Answered(str(status))
                error.response = self
                raise error

        def json(self):
            return payload

    def get(url, headers=None, params=None, timeout=None):
        calls.append({'url': url, 'headers': headers, 'params': params, 'timeout': timeout})
        if len(calls) <= resets:
            raise Reset('Connection reset by peer')
        return Response()

    module = types.ModuleType('requests')
    module.get = get
    module.HTTPError = Answered
    module.exceptions = types.SimpleNamespace(HTTPError=Answered, ConnectionError=Reset)
    module.calls = calls
    return module


@pytest.fixture
def requests_module(monkeypatch):
    """Install a fake requests, and make the backoff instant."""
    monkeypatch.setattr(cfbd_http.time, 'sleep', lambda _: None)

    def install(payload, **kwargs):
        module = fake_requests(payload, **kwargs)
        monkeypatch.setitem(sys.modules, 'requests', module)
        return module

    return install


def test_get_json_returns_the_payload_after_a_reset(requests_module):
    module = requests_module([{'school': 'Iowa'}])
    assert cfbd_http.get_json('https://x/teams', headers={'Authorization': 'Bearer k'},
                              params={'year': 2024}, timeout=60) == [{'school': 'Iowa'}]
    assert len(module.calls) == 2, 'the reset was not retried'
    # The retry must repeat the same request, not a bare one.
    assert module.calls[1] == {'url': 'https://x/teams', 'headers': {'Authorization': 'Bearer k'},
                               'params': {'year': 2024}, 'timeout': 60}


def test_get_json_does_not_retry_a_status_the_api_replied_with(requests_module):
    """raise_for_status runs inside the retried call, so a 401 has to stop it."""
    module = requests_module([], resets=0, status=401)
    with pytest.raises(Exception) as caught:
        cfbd_http.get_json('https://x/teams', headers={}, timeout=60)
    assert caught.value.response.status_code == 401
    assert len(module.calls) == 1


def test_get_json_still_retries_a_busy_service(requests_module):
    module = requests_module([{'ok': True}], resets=0, status=503)
    with pytest.raises(Exception):
        cfbd_http.get_json('https://x/teams', headers={}, timeout=60, attempts=3)
    assert len(module.calls) == 3, '503 means not now, so it is retried'


def test_the_helper_does_not_need_requests_to_be_importable(monkeypatch):
    """A loader reads a CSV from these modules and must not require the HTTP
    library: CI's rebuild step failed exactly that way once. The import lives
    inside get_json, so importing cfbd_http cannot pull requests in."""
    monkeypatch.setitem(sys.modules, 'requests', None)
    import importlib
    importlib.reload(cfbd_http)
    assert cfbd_http.is_transient(ConnectionResetError(104, 'reset'))


# (module, function, a payload it accepts, the arguments it takes)
FETCHERS = [
    ('fetch_cfbd_advanced', 'fetch_advanced', [], (2024, {})),
    ('fetch_cfbd_game_advanced', 'fetch_game_advanced', [], (2024, {})),
    ('fetch_cfbd_coaches', 'fetch_coaches', [], (2024, 2024, {})),
    ('fetch_cfbd_player_season_stats', 'fetch_season', [], (2024, {})),
    ('fetch_cfbd_rosters', 'fetch_season', [], (2024, {})),
    ('fetch_kickoffs', 'fetch_kickoffs', [], (2024, {})),
]


@pytest.mark.parametrize('module_name,function_name,payload,args', FETCHERS)
def test_a_reset_is_retried_rather_than_kept_as_a_stale_snapshot(
        module_name, function_name, payload, args, requests_module):
    module = requests_module(payload)
    fetcher = __import__(module_name)
    getattr(fetcher, function_name)(*args)
    assert len(module.calls) >= 2, f'{module_name}.{function_name} did not retry'
    assert module.calls[0]['url'] == module.calls[1]['url']


def test_the_memberships_fetch_is_retried(requests_module, monkeypatch):
    """The one fetcher with no fail-soft path at all: a reset on any season
    ended the run with a traceback and the table half written."""
    monkeypatch.setenv('CFBD_API_KEY', 'k')
    module = requests_module([{'school': 'Iowa', 'conference': 'Big Ten',
                               'classification': 'fbs'}])
    import fetch_cfbd_team_memberships as fetcher
    assert fetcher.fetch_year(2024)[0]['school'] == 'Iowa'
    assert len(module.calls) == 2
    assert module.calls[1]['params'] == {'year': 2024}


def test_patching_the_module_sleep_actually_stops_the_wait(monkeypatch):
    """A regression on the helper itself.

    `sleep` was a default argument -- `sleep: Callable = time.sleep` -- which
    binds at def time, so a test patching cfbd_http.time.sleep changed nothing.
    Those tests passed while waiting the full backoff, which is exactly how a
    vacuous patch hides: the assertion is about the retry, not the clock. Now
    the default is resolved inside the call.
    """
    waits = []
    monkeypatch.setattr(cfbd_http.time, 'sleep', waits.append)
    with pytest.raises(ConnectionResetError):
        cfbd_http.with_retries(
            lambda: (_ for _ in ()).throw(ConnectionResetError(104, 'reset')),
            attempts=3, base_delay=1.0)
    assert waits == [1.0, 2.0], 'the patched sleep was not the one called'


def test_the_live_scoreboard_rides_out_a_reset(monkeypatch, tmp_path):
    """The fetcher with the most visible failure: it runs every five minutes in
    season, and a reset left a hole in a scoreboard people were watching."""
    import fetch_live_scores as live

    attempts = []
    game = {'id': 1, 'status': 'completed', 'startDate': '2026-09-05T23:00:00.000Z',
            'homeTeam': {'name': 'Iowa', 'points': 20},
            'awayTeam': {'name': 'Iowa State', 'points': 13}}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'[' + __import__('json').dumps(game).encode() + b']'

    def urlopen(request, timeout=None):
        attempts.append(request.full_url)
        if len(attempts) < 2:
            raise urllib.error.URLError(ConnectionResetError(104, 'Connection reset by peer'))
        return Response()

    monkeypatch.setattr(live, 'urlopen', urlopen)
    monkeypatch.setattr(cfbd_http.time, 'sleep', lambda _: None)
    monkeypatch.setenv('CFBD_API_KEY', 'k')
    out = tmp_path / 'live_scores.json'
    monkeypatch.setattr(sys, 'argv', ['fetch_live_scores.py', '--out', str(out)])
    live.main()

    published = __import__('json').loads(out.read_text(encoding='utf-8'))
    assert [g['id'] for g in published['games']] == [1], 'the snapshot was not published'
    assert len(attempts) >= 2, 'the scoreboard reset was not retried'


def test_the_live_scoreboard_backoff_fits_inside_its_five_minute_cadence():
    """Five attempts at the usual base would be 30s of waiting in a job that
    runs every five minutes. Three is 6s, which is a reset ridden out."""
    assert fetch_live_scores_attempts() == 3
    assert sum(cfbd_http.BASE_DELAY * 2 ** n
               for n in range(fetch_live_scores_attempts() - 1)) <= 10


def fetch_live_scores_attempts():
    import fetch_live_scores
    return fetch_live_scores.ATTEMPTS
