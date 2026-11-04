"""Fetching the NBA archive, one Parquet release asset at a time.

The source is the sportsdataverse project's ESPN NBA data, 2002 to the season
in progress. There is no key, no request allowance and no paging: each dataset
and season is one public file.

WHICH MIRROR, AND WHY IT MATTERS. The same data appears in two places and only
one of them is complete:

- `github.com/sportsdataverse/sportsdataverse-data/releases/download/...`
  — the canonical distribution, which the hoopR R package itself reads. Every
  dataset resolves here.
- `raw.githubusercontent.com/sportsdataverse/hoopR-nba-data/main/nba/...`
  — a repository tree that publishes only schedules, play-by-play and the two
  box scores as Parquet. Rosters, standings, player core, season stats and the
  draft all 404 there.

Measured on 2026-11-03, which is why this module reads the release assets. The
URL shapes were pinned from `R/load_nba.R` in the hoopR package rather than
guessed; several plausible guesses against the repository tree returned 404
while the data was in fact published, which is exactly how a source gets
written off by mistake.

CSV is not published at all, so Parquet is the only format and `pyarrow` is a
real dependency of the ingest. It is a build-time dependency: the published
site fetches JSON.

What is retried and what is not follows the rule the other three leagues'
fetchers learned the hard way: the status check happens INSIDE the retried
call so that 5xx backs off while 404 fails at once, and the sleep function is
resolved inside the call rather than as a def-time default, because a default
argument binds at def time and silently defeats any test that patches it.
"""

import os
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

RELEASE_ROOT = ('https://github.com/sportsdataverse/sportsdataverse-data/'
                'releases/download')

# dataset -> (release tag, file stem). Every one verified 200 on 2026-11-03.
# The stem is not derivable from the tag — `espn_nba_schedules` holds
# `nba_schedule_2027.parquet` while `espn_nba_standings` holds
# `standings_2026.parquet` — so both are written down.
DATASETS = {
    'schedule': ('espn_nba_schedules', 'nba_schedule'),
    'team_box': ('espn_nba_team_boxscores', 'team_box'),
    'player_box': ('espn_nba_player_boxscores', 'player_box'),
    'standings': ('espn_nba_standings', 'standings'),
    'pbp': ('espn_nba_pbp', 'play_by_play'),
    'rosters': ('espn_nba_rosters', 'rosters'),
    'player_core': ('espn_nba_player_core', 'player_core'),
    'player_season_stats': ('espn_nba_player_season_stats',
                            'player_season_stats'),
    'team_season_stats': ('espn_nba_team_season_stats', 'team_season_stats'),
    'draft': ('espn_nba_draft', 'draft'),
}

DEFAULT_RAW_ROOT = Path('data/nba/raw')
ATTEMPTS = 5
BACKOFF_SECONDS = 2.0
TIMEOUT_SECONDS = 180
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
# A season of play-by-play is 22 MB and is the largest thing fetched here.
# An order of magnitude of headroom still refuses a redirect to something
# that is not one of these files.
MAX_BYTES = 256 * 1024 * 1024
# Parquet's magic number, first and last four bytes of every valid file. A
# redirect to an HTML error page is a 200 with a body, and without this check
# it would be cached and then fail later as an unreadable column.
PARQUET_MAGIC = b'PAR1'
USER_AGENT = ('elocast-nba/1 '
              '(+https://github.com/ak038819-sudo/College-Football-Coefficient)')


class NbaSourceError(RuntimeError):
    """The archive could not be fetched or is not what was expected."""


def asset_url(dataset, season):
    """The release asset for one dataset and season."""
    if dataset not in DATASETS:
        raise NbaSourceError(f'Unknown dataset {dataset!r}')
    tag, stem = DATASETS[dataset]
    return f'{RELEASE_ROOT}/{tag}/{stem}_{int(season)}.parquet'


def fetch_asset(url, *, opener=None, sleep=None, attempts=ATTEMPTS):
    """Download one asset and return its bytes. Retries 5xx, not 404."""
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
                raise NbaSourceError(
                    f'{url} is larger than expected; refusing rather than '
                    'loading it')
            if not body:
                raise NbaSourceError(f'{url} came back empty')
            if body[:4] != PARQUET_MAGIC or body[-4:] != PARQUET_MAGIC:
                raise NbaSourceError(
                    f'{url} did not answer with a Parquet file; the body '
                    'starts ' + repr(body[:16]))
            return body
        except HTTPError as error:
            last = error
            if error.code not in RETRYABLE_STATUS:
                raise NbaSourceError(
                    f'{url} failed with HTTP {error.code}') from error
        except (URLError, TimeoutError, OSError) as error:
            last = error
        if attempt < attempts:
            wait(BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise NbaSourceError(f'{url} failed after {attempts} attempts: '
                         f'{type(last).__name__}')


def cache_path(dataset, season, root=None):
    """Where a fetched asset is kept. Resolved inside the function."""
    base = Path(root) if root is not None else DEFAULT_RAW_ROOT
    return base / dataset / f'{int(season)}.parquet'


def load_asset(dataset, season, *, root=None, reuse_cache=False, opener=None,
               sleep=None):
    """An asset's path on disk, fetched unless the cache may be reused.

    `reuse_cache` defaults to FALSE, as the NFL fetcher's does and unlike
    basketball's. There every season is settled history and refetching one
    spends a metered allowance; here nothing is metered, and the season in
    progress changes every day, so preferring the cache would freeze this
    week's results while every export stamped a fresh as-of time over them.
    """
    path = cache_path(dataset, season, root)
    if reuse_cache and path.exists():
        return path, 'cache'
    body = fetch_asset(asset_url(dataset, season), opener=opener, sleep=sleep)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path, 'network'


def read_parquet(path):
    """One asset as a list of dicts.

    `pyarrow` is imported here rather than at module scope so that importing
    this module — which the identity and schema tests do — needs no Parquet
    library. The football loaders keep `requests` lazy for the same reason.
    """
    try:
        import pyarrow.parquet as parquet
    except ImportError as error:        # pragma: no cover - environment
        raise NbaSourceError(
            'Reading the NBA archive needs pyarrow (requirements-dev.txt)'
        ) from error
    return parquet.read_table(str(path)).to_pylist()


def available_seasons(now, first=2002):
    """Seasons whose file should exist, newest last.

    An NBA season is named here for the year it ENDS, which is what the
    source does: `2027` is 2026-27. The turnover is September, before any
    game of the new season is played and after the Finals of the old one, so
    a run in October and a run the following May agree about which season is
    current.
    """
    end_year = now.year + 1 if now.month >= 9 else now.year
    return list(range(first, end_year + 1))


def source_revision():
    """The commit this code ran at, for the snapshot row. Never fails a run."""
    try:
        return subprocess.run(['git', 'rev-parse', 'HEAD'],
                              capture_output=True, text=True,
                              check=True).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return os.environ.get('GITHUB_SHA') or None
