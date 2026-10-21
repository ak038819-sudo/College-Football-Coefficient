"""Fetching the NFL game archive, with a committed cache.

The source is nflverse's `games.csv`: one row per NFL game from 1999 to the
game being played this week, with final scores, dates, venue neutrality and
overtime. It is a single public file on raw.githubusercontent.com — no key, no
request allowance, no per-season paging. That is a deliberate choice over a
metered API: the rolling seven-day cap that stalled the basketball track for a
week cannot happen to a file anyone may fetch.

What is retried, and what is not, follows the rules the football fetchers
learned the hard way: the status check happens INSIDE the retried call so that
5xx backs off while 404 fails at once, and the sleep function is resolved
inside the call rather than as a def-time default, because a default argument
binds at def time and silently defeats any test that patches it.
"""

import gzip
import os
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

GAMES_URL = ('https://raw.githubusercontent.com/nflverse/nfldata/'
             'master/data/games.csv')
DEFAULT_RAW_ROOT = Path('data/nfl/raw')
CACHE_NAME = 'games.csv.gz'
ATTEMPTS = 5
BACKOFF_SECONDS = 2.0
TIMEOUT_SECONDS = 120
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
# The whole archive is about 2.2 MB. An order of magnitude of headroom still
# refuses a redirect to something that is not this file.
MAX_BYTES = 64 * 1024 * 1024
USER_AGENT = 'elocast-nfl/1 (+https://github.com/ak038819-sudo/College-Football-Coefficient)'


class NflSourceError(RuntimeError):
    """The archive could not be fetched or is not what was expected."""


def fetch_games_csv(url=GAMES_URL, *, opener=None, sleep=None,
                    attempts=ATTEMPTS):
    """Download the archive and return it as text. Retries 5xx, not 404."""
    open_url = opener if opener is not None else urlopen
    wait = sleep if sleep is not None else time.sleep
    last = None
    for attempt in range(1, attempts + 1):
        try:
            request = Request(url, headers={'User-Agent': USER_AGENT})
            with open_url(request, timeout=TIMEOUT_SECONDS) as response:
                status = getattr(response, 'status', 200) or 200
                if status >= 400:
                    raise HTTPError(url, status, 'error', {}, None)
                body = response.read(MAX_BYTES + 1)
            if len(body) > MAX_BYTES:
                raise NflSourceError('Archive is larger than expected; '
                                     'refusing rather than loading it')
            if not body:
                raise NflSourceError('Archive came back empty')
            return body.decode('utf-8')
        except HTTPError as error:
            last = error
            if error.code not in RETRYABLE_STATUS:
                raise NflSourceError(
                    f'Archive request failed with HTTP {error.code}') from error
        except (URLError, TimeoutError, OSError) as error:
            last = error
        if attempt < attempts:
            wait(BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise NflSourceError(f'Archive request failed after {attempts} attempts: '
                         f'{type(last).__name__}')


def cache_path(root=None):
    """Where the fetched archive is kept. Resolved inside the function."""
    base = Path(root) if root is not None else DEFAULT_RAW_ROOT
    return base / CACHE_NAME


def write_cache(text, *, root=None):
    path = cache_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    # mtime 0 so the same archive gzips to the same bytes and a refresh that
    # fetched nothing new produces no commit.
    with gzip.GzipFile(filename='', mode='wb', fileobj=open(path, 'wb'),
                       mtime=0) as handle:
        handle.write(text.encode('utf-8'))
    return path


def read_cache(*, root=None):
    path = cache_path(root)
    if not path.exists():
        return None
    with gzip.open(path, 'rt', encoding='utf-8') as handle:
        return handle.read()


def load_games_csv(*, root=None, reuse_cache=False, opener=None, sleep=None):
    """The archive text, from the network or the cache.

    `reuse_cache` defaults to FALSE, the opposite of the basketball ingest.
    There every season is settled history and refetching one spends a metered
    allowance; here one file holds the season in progress, so preferring the
    cache by default would freeze this week's results while every export kept
    stamping a fresh as-of time over them. That exact failure had to be
    patched into the basketball refresh after the fact.
    """
    if reuse_cache:
        cached = read_cache(root=root)
        if cached is not None:
            return cached, 'cache'
    text = fetch_games_csv(opener=opener, sleep=sleep)
    write_cache(text, root=root)
    return text, 'network'


def source_revision():
    """The commit this code ran at, for the snapshot row. Never fails a run."""
    try:
        return subprocess.run(['git', 'rev-parse', 'HEAD'],
                              capture_output=True, text=True,
                              check=True).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return os.environ.get('GITHUB_SHA') or None
