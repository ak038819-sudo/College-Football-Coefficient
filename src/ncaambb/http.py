"""Retried CBBD GET. Standard library only, so loaders need no HTTP package.

Mirrors the football fetchers' hard-won rules: the status check happens INSIDE
the retried call, so 429 and 5xx back off while 401 and 404 fail at once; and
the sleep function is resolved inside the call, never as a def-time default,
because a default argument binds at def time and silently defeats any test that
patches the module attribute.
"""

import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = 'https://api.collegebasketballdata.com'
ATTEMPTS = 5
BACKOFF_SECONDS = 2.0
TIMEOUT_SECONDS = 60
# A rate limit is the one failure the server itself tells you how to handle.
# Blind exponential backoff spends 2+4+8+16 = 30 seconds and then gives up,
# which is what lost five seasons of a refresh to HTTP 429: the limit window
# was longer than the retries. Honour the header and cap it, so a wrong or
# hostile value cannot park a run for an hour.
MAX_RETRY_AFTER_SECONDS = 120
# Retrying an authorization or not-found error cannot help and burns quota.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class CbbdError(RuntimeError):
    """A CBBD request failed. Never carries the key or a response body."""


class CbbdRateLimitError(CbbdError):
    """The provider's rate limit was not survivable by waiting.

    Separated for the same reason as CbbdAuthError: it is not a data result.
    Once a season's very first request has exhausted its retries on 429, every
    later season will do the same, so a caller that tolerates a per-season gap
    must stop rather than report seven seasons as "not stored" -- which reads
    as "the provider has no games" and is the opposite of what happened. It
    also stops the run spending thirty seconds of backoff per remaining season
    to learn the same thing again: one rehearsal sent 35 requests where 5 would
    have told the whole story.
    """


class CbbdAuthError(CbbdError):
    """The key is missing, rejected or not entitled.

    Separated from other failures because it can never be fixed by trying
    again, and because a caller that tolerates a per-season data gap must
    still abort on this: continuing would report an access failure as absent
    coverage and spend the rest of the quota learning nothing.
    """


class Quota:
    """Counts requests actually sent, so a run can report what it spent."""

    def __init__(self):
        self.requests = 0
        self.retries = 0

    def as_dict(self):
        return dict(requests=self.requests, retries=self.retries)


# 401 is a bad key, 403 a key without entitlement for the path.
AUTH_STATUS = frozenset({401, 403})


def retry_after(headers, *, cap=MAX_RETRY_AFTER_SECONDS):
    """The server's own wait in seconds, or None when it did not say.

    Only the integer-seconds form is read. The HTTP-date form is allowed by
    the specification but CBBD does not send it, and parsing a date here would
    mean trusting a clock difference to decide how long to sleep.
    """
    if headers is None:
        return None
    raw = headers.get('Retry-After')
    if raw is None:
        return None
    try:
        seconds = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return min(float(seconds), float(cap))


def _api_key():
    key = os.environ.get('CBBD_API_KEY')
    if not key:
        raise CbbdAuthError('CBBD_API_KEY is not set; supply it from the environment')
    return key


def get_json(path, params=None, *, key=None, quota=None, attempts=ATTEMPTS,
             sleep=None, opener=None):
    """GET a CBBD path and return the decoded JSON array.

    Raises CbbdError without retrying on an authorization, not-found or other
    non-retryable status. Never logs the key, the URL's headers or the
    provider's error body.
    """
    # Resolved here, not as a default, so patching this module's time/urlopen works.
    rest = sleep if sleep is not None else time.sleep
    fetch = opener if opener is not None else urlopen
    token = key if key is not None else _api_key()
    url = BASE + path + ('?' + urlencode(params) if params else '')
    last, told = None, None
    for attempt in range(1, attempts + 1):
        try:
            request = Request(url, headers={'Authorization': f'Bearer {token}',
                                            'Accept': 'application/json'})
            with fetch(request, timeout=TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            if quota is not None:
                quota.requests += 1
            if not isinstance(payload, list):
                raise CbbdError(f'Expected a JSON array from {path}')
            return payload
        except HTTPError as exc:
            if quota is not None:
                quota.requests += 1
            if exc.code in AUTH_STATUS:
                raise CbbdAuthError(f'CBBD HTTP {exc.code} at {path}; the key is '
                                    'missing, rejected or not entitled') from None
            if exc.code not in RETRYABLE_STATUS:
                raise CbbdError(f'CBBD HTTP {exc.code} at {path}; '
                                'check access or the path') from None
            last = f'HTTP {exc.code}'
            told = retry_after(getattr(exc, 'headers', None))
        except URLError:
            if quota is not None:
                quota.requests += 1
            last, told = 'connection failed', None
        except json.JSONDecodeError:
            if quota is not None:
                quota.requests += 1
            last, told = 'malformed JSON', None
        if attempt == attempts:
            break
        if quota is not None:
            quota.retries += 1
        # The server's wait wins when it gave one, because it knows when the
        # window reopens and the curve below is only a guess.
        rest(told if told is not None
             else BACKOFF_SECONDS * 2 ** (attempt - 1))
    if last == 'HTTP 429':
        raise CbbdRateLimitError(
            f'CBBD rate limit not cleared at {path} after {attempts}'
            ' attempts; the quota looks exhausted rather than momentarily busy')
    raise CbbdError(f'CBBD request to {path} failed after {attempts} attempts ({last})')
