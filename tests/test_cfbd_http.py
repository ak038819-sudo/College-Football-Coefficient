"""A reset is not an answer: what gets retried, what does not, and what waits.

CFBD reset a connection three times on 2026-10-03. Twice it failed the deploy's
own fetch so nothing published, and once it stopped a 23-season box-score
backfill after one season, because every call site treated a transport failure
as final. These tests pin the distinction that fixes it: a request that never
got an answer is retried, a request the API answered is not.
"""
from __future__ import annotations

import urllib.error

import pytest

import cfbd_http


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError('https://api.collegefootballdata.com/games', code,
                                  'because', {}, None)


class FakeRequestsError(Exception):
    """Stands in for requests' own hierarchy, which cfbd_http must not import.

    The box-score backfill runs without requests installed, so the predicate
    recognises those errors by module and name rather than by isinstance.
    """


FakeRequestsError.__module__ = 'requests.exceptions'


def test_a_connection_that_never_answered_is_retried():
    calls = []
    waits = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionResetError(104, 'Connection reset by peer')
        return 'the payload'

    assert cfbd_http.with_retries(flaky, sleep=waits.append) == 'the payload'
    assert len(calls) == 3
    # Exponential, so a service that is busy is not hammered at a fixed rate.
    assert waits == [2.0, 4.0]


def test_the_backoff_doubles_from_the_base_delay():
    waits = []
    with pytest.raises(ConnectionResetError):
        cfbd_http.with_retries(lambda: (_ for _ in ()).throw(ConnectionResetError(104, 'x')),
                               attempts=5, base_delay=1.0, sleep=waits.append)
    assert waits == [1.0, 2.0, 4.0, 8.0]      # four waits between five attempts


def test_an_answered_request_is_not_retried():
    """401 and 404 are answers. Repeating them only spends time."""
    for code in (400, 401, 403, 404, 422):
        calls = []

        def answered(code=code, calls=calls):
            calls.append(1)
            raise http_error(code)

        with pytest.raises(urllib.error.HTTPError):
            cfbd_http.with_retries(answered, sleep=lambda _: None)
        assert calls == [1], f'{code} must not be retried'


def test_come_back_later_is_retried():
    for code in (429, 500, 502, 503, 504):
        assert cfbd_http.is_transient(http_error(code)), code


def test_a_programming_error_is_never_retried():
    calls = []

    def broken():
        calls.append(1)
        raise ValueError('malformed player response')

    with pytest.raises(ValueError):
        cfbd_http.with_retries(broken, sleep=lambda _: None)
    assert calls == [1]


def test_requests_transport_errors_are_recognised_without_importing_requests():
    connection = FakeRequestsError('reset')
    connection.__class__.__name__ = 'ConnectionError'
    assert cfbd_http.is_transient(connection)

    answered = FakeRequestsError('401')
    answered.__class__.__name__ = 'HTTPError'
    answered.response = type('R', (), {'status_code': 401})()
    assert not cfbd_http.is_transient(answered)

    busy = FakeRequestsError('503')
    busy.__class__.__name__ = 'HTTPError'
    busy.response = type('R', (), {'status_code': 503})()
    assert cfbd_http.is_transient(busy)


def test_the_last_error_is_raised_once_the_attempts_are_spent():
    """A genuine outage must still fail the run, not publish a half-fetched archive."""
    def dead():
        raise ConnectionResetError(104, 'Connection reset by peer')

    with pytest.raises(ConnectionResetError):
        cfbd_http.with_retries(dead, attempts=2, sleep=lambda _: None)


def test_one_attempt_means_no_retry_and_no_wait():
    waits = []
    with pytest.raises(ConnectionResetError):
        cfbd_http.with_retries(lambda: (_ for _ in ()).throw(ConnectionResetError(104, 'x')),
                               attempts=1, sleep=waits.append)
    assert waits == []
    with pytest.raises(ValueError):
        cfbd_http.with_retries(lambda: None, attempts=0)


def test_the_player_fetcher_retries_its_own_calls(monkeypatch):
    """The call site that stopped the backfill, driven rather than read."""
    import fetch_cfbd_players as fetcher

    attempts = []

    class Response:
        def read(self):
            return b'[{"week": 1}]'

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def urlopen(request, timeout=None):
        attempts.append(request.full_url)
        if len(attempts) < 2:
            raise urllib.error.URLError(ConnectionResetError(104, 'Connection reset by peer'))
        return Response()

    monkeypatch.setattr(fetcher, 'urlopen', urlopen)
    monkeypatch.setattr(cfbd_http.time, 'sleep', lambda _: None)
    assert fetcher.get_json('/calendar', {'year': 2005}, 'k') == [{'week': 1}]
    assert len(attempts) == 2, 'the reset was not retried'
    assert 'year=2005' in attempts[0]
