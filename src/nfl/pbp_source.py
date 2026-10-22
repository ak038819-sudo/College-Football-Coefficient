"""Fetching the nflverse play-by-play one season at a time, keeping none.

**NOTHING FROM THIS FILE IS EVER STORED OR COMMITTED.** The play-by-play is
the largest thing in this project by two orders of magnitude: 99 MB of CSV for
2024 alone, 372 columns, 49,492 rows, and about 2.8 GB across 1999-2026. The
repository's `.git` is already over 100 MB, so a copy of it anywhere under
version control is out of the question, and a cache of it on disk would be a
2.8 GB directory nobody asked for.

So this module streams. A season is downloaded to a temporary file, reduced to
drives and scoring plays by `src/nfl/plays.py`, and deleted before the next
season starts. Peak disk is one season, about 100 MB, and the derived tables
are four orders of magnitude smaller than the plays behind them: 6,148 drives
and 3,767 scoring plays for 2024's 49,492 plays.

That is the same bargain the college side struck when it stored 441,307 drives
rather than the plays they came from.

Only the ~30 columns the reduction needs are read. `csv.DictReader` over 372
columns builds a 372-key dict for every one of 1.3 million plays; a
`csv.reader` with an index map does not, and the difference is minutes.
"""

import argparse
import csv
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .identity import FIRST_SEASON, current_season, seasons_through
from .source import (ATTEMPTS, BACKOFF_SECONDS, RETRYABLE_STATUS,
                     TIMEOUT_SECONDS, USER_AGENT, NflSourceError)

RELEASE = 'https://github.com/nflverse/nflverse-data/releases/download'
# A season of plays is 33-100 MB. The ceiling is generous enough for a season
# the source enriches further and still refuses a redirect to something that
# is not a season of plays.
MAX_BYTES = 512 * 1024 * 1024
CHUNK = 1024 * 1024


class NflNotPublished(NflSourceError):
    """The file is not there: the server said 404 and nothing else.

    A SEPARATE EXCEPTION BECAUSE THE DIFFERENCE MATTERS. The season in
    progress may have no play-by-play file yet, and tolerating that is how a
    refresh keeps working in September. Tolerating every failure is a
    different thing: a timeout, a 503 or the size cap would then read as
    "not published yet", the run would succeed, and the published export
    would lose the current season's game detail — the season anyone is
    actually looking at.
    """


def play_by_play_url(season):
    return f'{RELEASE}/pbp/play_by_play_{season}.csv'


def download(url, target, *, opener=None, sleep=None, attempts=ATTEMPTS,
             max_bytes=None):
    """Stream a URL to a file. Retries 5xx, not 404. Returns bytes written.

    Streamed rather than read whole: `response.read()` on a 100 MB body holds
    100 MB of it in memory and then writes it again, for no gain over copying
    a megabyte at a time.

    A FAILED DOWNLOAD LEAVES NO FILE. Whatever was written before a cap, a
    reset or a timeout is deleted, because a truncated play-by-play CSV on
    disk is the one thing `rows` exists to refuse and a retry that found it
    would read a season short.

    `max_bytes` is resolved inside the call rather than as a def-time
    default, which this repository has had to learn five times: a default
    argument binds at import and cannot be changed by a test.
    """
    open_url = opener if opener is not None else urlopen
    wait = sleep if sleep is not None else time.sleep
    cap = MAX_BYTES if max_bytes is None else max_bytes
    target = Path(target)
    last = None
    for attempt in range(1, attempts + 1):
        complete = False
        try:
            request = Request(url, headers={'User-Agent': USER_AGENT})
            written = 0
            with open_url(request, timeout=TIMEOUT_SECONDS) as response:
                status = getattr(response, 'status', 200) or 200
                if status >= 400:
                    raise HTTPError(url, status, 'error', {}, None)
                with open(target, 'wb') as handle:
                    while True:
                        chunk = response.read(CHUNK)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > cap:
                            raise NflSourceError(
                                f'{url} is larger than {cap} bytes; '
                                'refusing rather than filling the disk')
                        handle.write(chunk)
            if not written:
                raise NflSourceError(f'{url} came back empty')
            complete = True
            return written
        except HTTPError as error:
            last = error
            if error.code == 404:
                raise NflNotPublished(
                    f'{url} is not there (HTTP 404)') from error
            if error.code not in RETRYABLE_STATUS:
                raise NflSourceError(
                    f'{url} failed with HTTP {error.code}') from error
        except (URLError, TimeoutError, OSError) as error:
            last = error
        finally:
            if not complete:
                target.unlink(missing_ok=True)
        if attempt < attempts:
            wait(BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise NflSourceError(f'{url} failed after {attempts} attempts: '
                         f'{type(last).__name__}')


def rows(path, wanted):
    """Yield dicts of just `wanted` columns, from a CSV on disk.

    A column the file does not carry yields None for every row rather than
    raising: the play-by-play's column set has grown over the years and a
    reduction that needs a column it cannot have should say so where it uses
    it, not here.
    """
    with open(path, newline='', encoding='utf-8') as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return
        index = {name: position for position, name in enumerate(header)}
        picks = [(name, index.get(name)) for name in wanted]
        width = len(header)
        for row in reader:
            if len(row) != width:
                # A short row is a truncated download, not a play. Refusing is
                # the only safe reading: a reduction built from half a game is
                # a game story with the second half missing and no sign of it.
                raise NflSourceError(
                    f'{path} has a row of {len(row)} fields where the header '
                    f'has {width}; the download is truncated')
            yield {name: (row[position] if position is not None else None)
                   for name, position in picks}


def seasons_for(seasons=None, *, now=None, first=FIRST_SEASON):
    """The seasons to fetch: the ones asked for, or every one to date."""
    if seasons:
        return sorted(seasons)
    moment = now or datetime.now(timezone.utc)
    return seasons_through(moment, first)


def each_season(seasons, *, opener=None, sleep=None, now=None, root=None):
    """Download each season in turn, hand over its path, then delete it.

    A generator so the caller never holds two seasons at once, and the file is
    removed in a `finally` so a reduction that raises still leaves no 100 MB
    file behind.
    """
    moment_season = (current_season(now) if now is not None else None)
    for season in seasons:
        directory = Path(root) if root else Path(tempfile.gettempdir())
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f'nfl-pbp-{season}.csv'
        try:
            try:
                size = download(play_by_play_url(season), target,
                                opener=opener, sleep=sleep)
            except NflNotPublished:
                # The season in progress may have no file yet; a settled
                # season missing one is a real failure, and so is any other
                # kind of failure on either — see NflNotPublished.
                if moment_season is not None and season >= moment_season:
                    yield season, None, 0
                    continue
                raise
            yield season, target, size
        finally:
            if target.exists():
                target.unlink()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Report what the play-by-play fetch would cost')
    parser.add_argument('--seasons', default=None)
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    total = 0
    for season, path, size in each_season(seasons_for(seasons)):
        total += size
        print(f'  {season}  {size / 1e6:8.1f} MB'
              + ('' if path else '  (not published yet)'))
    print(f'{total / 1e9:.2f} GB fetched, nothing kept')
    return 0


if __name__ == '__main__':
    sys.exit(main())
