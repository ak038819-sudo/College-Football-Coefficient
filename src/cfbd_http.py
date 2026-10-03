#!/usr/bin/env python3
"""Retrying transport for CFBD requests.

CFBD resets a connection often enough to matter. On 2026-10-03 it did so three
times in one evening: twice it failed the deploy's own fetch, so nothing
published, and once it killed a 23-season box-score backfill after a single
season. A reset is not an answer from the API, it is the absence of one, and
every call site here was treating it as fatal.

So a request that failed in transport is retried. A request the API actually
ANSWERED is not: 401 means the key is wrong and 404 means the thing is not
there, and repeating either only spends time to be told the same thing. The
statuses that are retried are the ones that mean "not now" rather than "no":
429 and the 5xx family.

The sleep is injectable so a test can drive the backoff without waiting for it.
"""
from __future__ import annotations

import time
import urllib.error
from typing import Callable, TypeVar

T = TypeVar("T")

# "Come back later", as opposed to an answer about the request itself.
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
ATTEMPTS = 5
BASE_DELAY = 2.0


def _status_of(error: BaseException) -> int | None:
    """The HTTP status an error carries, under urllib's name for it or requests'."""
    code = getattr(error, "code", None)          # urllib.error.HTTPError
    if isinstance(code, int):
        return code
    response = getattr(error, "response", None)  # requests.exceptions.HTTPError
    status = getattr(response, "status_code", None)
    return status if isinstance(status, int) else None


def is_transient(error: BaseException) -> bool:
    """Whether retrying this could plausibly produce a different outcome."""
    status = _status_of(error)
    if status is not None:
        return status in RETRY_STATUSES
    if isinstance(error, (urllib.error.URLError, OSError, TimeoutError)):
        # ConnectionResetError, a timeout, a DNS failure, a refused connection:
        # the request never got an answer.
        return True
    # requests raises its own hierarchy, which this module must not import --
    # the box-score backfill runs without it installed.
    name = type(error).__name__
    module = type(error).__module__ or ""
    return module.startswith("requests.") and name in {
        "ConnectionError", "Timeout", "ConnectTimeout", "ReadTimeout", "ChunkedEncodingError"}


def with_retries(call: Callable[[], T], *, describe: str = "CFBD request",
                 attempts: int = ATTEMPTS, base_delay: float = BASE_DELAY,
                 sleep: Callable[[float], None] = time.sleep) -> T:
    """Call `call`, retrying a transport failure with an exponential backoff.

    Raises the last error once the attempts are spent, so a genuine outage still
    fails the run and names the season to resume from rather than publishing a
    half-fetched archive.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except Exception as error:  # noqa: BLE001 -- re-raised below unless transient
            if attempt == attempts or not is_transient(error):
                raise
            delay = base_delay * 2 ** (attempt - 1)
            print(f"{describe} failed ({type(error).__name__}: {error}); "
                  f"retrying in {delay:.0f}s (attempt {attempt} of {attempts})", flush=True)
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
