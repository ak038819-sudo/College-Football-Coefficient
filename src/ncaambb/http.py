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
# Retrying an authorization or not-found error cannot help and burns quota.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class CbbdError(RuntimeError):
    """A CBBD request failed. Never carries the key or a response body."""


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
    last = None
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
        except URLError:
            if quota is not None:
                quota.requests += 1
            last = 'connection failed'
        except json.JSONDecodeError:
            if quota is not None:
                quota.requests += 1
            last = 'malformed JSON'
        if attempt == attempts:
            break
        if quota is not None:
            quota.retries += 1
        rest(BACKOFF_SECONDS * 2 ** (attempt - 1))
    raise CbbdError(f'CBBD request to {path} failed after {attempts} attempts ({last})')
