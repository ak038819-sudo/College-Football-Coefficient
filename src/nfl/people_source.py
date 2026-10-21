"""Fetching the nflverse people archives: players, rosters, statistics, draft.

Four sources, all release assets of `nflverse/nflverse-data`, all public, no
key and no request allowance:

| what             | asset                                       |
| ---------------- | ------------------------------------------- |
| every player     | `players/players.csv`                       |
| a season's roster| `rosters/roster_<season>.csv`               |
| season statistics| `stats_player/stats_player_<reg|post>_<season>.csv` |
| the draft        | `draft_picks/draft_picks.csv`               |

**THE RELEASE TAG MATTERS AND THE OBVIOUS ONE IS WRONG.** There is an older
`player_stats` release that still serves `stats_player_reg_2024.csv` and
stops there: every season from 2025 on is a 404 under it, which reads as "the
data does not exist yet" rather than as "you are asking the wrong release".
The live tag is `stats_player`, which is what `nflreadr::load_player_stats`
builds its URL from, and under it 1999-2026 are all present. Its header is
also WIDER — explosive-play counts, punting and blocked kicks — which is why
the statistics are stored in long form rather than as columns.

A 404 is tolerated for the season in progress and refused for any other.
Measured on 2026-10-21: every season 1999-2026 has a roster and a regular
season statistics file, and every season except 2026 has a postseason one,
2026's postseason not having happened. So a missing file for a settled season
is a real failure and must fail the run rather than silently shrink the
archive.
"""

import argparse
import csv
import gzip
import io
import sys
from datetime import datetime, timezone
from pathlib import Path

from .identity import FIRST_SEASON, current_season, seasons_through
from .source import NflSourceError, fetch_games_csv

RELEASE = 'https://github.com/nflverse/nflverse-data/releases/download'
PLAYERS_URL = f'{RELEASE}/players/players.csv'
DRAFT_URL = f'{RELEASE}/draft_picks/draft_picks.csv'
SEASON_TYPES = ('reg', 'post')
DEFAULT_RAW_ROOT = Path('data/nfl/raw/people')


def roster_url(season):
    return f'{RELEASE}/rosters/roster_{season}.csv'


def stats_url(season, season_type):
    if season_type not in SEASON_TYPES:
        raise ValueError(f'{season_type!r} is not one of {SEASON_TYPES}')
    return (f'{RELEASE}/stats_player/'
            f'stats_player_{season_type}_{season}.csv')


def cache_path(name, *, root=None):
    """Where one fetched file is kept. Resolved inside the function."""
    base = Path(root) if root is not None else DEFAULT_RAW_ROOT
    return base / f'{name}.gz'


def write_cache(name, text, *, root=None):
    path = cache_path(name, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    # mtime 0 so an unchanged file gzips to identical bytes.
    with gzip.GzipFile(filename='', mode='wb', fileobj=open(path, 'wb'),
                       mtime=0) as handle:
        handle.write(text.encode('utf-8'))
    return path


def read_cache(name, *, root=None):
    path = cache_path(name, root=root)
    if not path.exists():
        return None
    with gzip.open(path, 'rt', encoding='utf-8') as handle:
        return handle.read()


def rows(text):
    """A CSV as a list of dicts. Every value a string, blanks left blank.

    The source writes a missing number as an empty field and, in a few older
    files, as `NA`. Both are absences and both are normalised to '' here so
    that one check downstream covers them.
    """
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        out.append({key: ('' if value is None or value == 'NA'
                          else value.strip())
                    for key, value in row.items() if key is not None})
    return out


def fetch(name, url, *, root=None, reuse_cache=False, opener=None, sleep=None,
          required=True):
    """One file, from the cache or the network, as a list of dicts.

    Returns None where the file is absent and `required` is False, which is
    the postseason of a season whose postseason has not been played.
    """
    if reuse_cache:
        cached = read_cache(name, root=root)
        if cached is not None:
            return rows(cached), 'cache'
    try:
        text = fetch_games_csv(url, opener=opener, sleep=sleep)
    except NflSourceError:
        if required:
            raise
        return None, 'absent'
    write_cache(name, text, root=root)
    return rows(text), 'network'


def load(*, seasons=None, root=None, reuse_cache=False, opener=None,
         sleep=None, now=None):
    """Every people file for the seasons asked for, fetched or reused.

    Returns `(bundle, origins)`. The bundle holds `players`, `draft`,
    `rosters` (season -> rows) and `stats` ((season, type) -> rows).
    """
    moment = now or datetime.now(timezone.utc)
    wanted = sorted(seasons) if seasons else seasons_through(moment,
                                                             FIRST_SEASON)
    in_progress = current_season(moment)
    origins = {}

    def one(name, url, required=True):
        got, origin = fetch(name, url, root=root, reuse_cache=reuse_cache,
                            opener=opener, sleep=sleep, required=required)
        origins[name] = origin
        return got

    bundle = dict(
        players=one('players', PLAYERS_URL),
        draft=one('draft_picks', DRAFT_URL),
        rosters={},
        stats={},
        seasons=wanted,
    )
    for season in wanted:
        # A file missing for the season in progress is the archive not having
        # caught up; missing for any settled season is a failure, because
        # every one of them was measured present.
        optional = season >= in_progress
        got = one(f'roster_{season}', roster_url(season), required=not optional)
        if got is not None:
            bundle['rosters'][season] = got
        for season_type in SEASON_TYPES:
            # The postseason file of the season in progress is absent until a
            # playoff game has been played, which is not a failure in January
            # either: `optional` already covers it.
            got = one(f'stats_{season_type}_{season}',
                      stats_url(season, season_type), required=not optional)
            if got is not None:
                bundle['stats'][(season, season_type)] = got
    return bundle, origins


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Fetch the nflverse people archives into the raw cache')
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default=None,
                        help='comma-separated seasons; every season by default')
    parser.add_argument('--reuse-cache', action='store_true')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    try:
        bundle, origins = load(seasons=seasons, root=args.raw_root,
                               reuse_cache=args.reuse_cache)
    except NflSourceError as error:
        print(f'Cannot fetch the people archive: {error}', file=sys.stderr)
        return 1
    absent = sorted(name for name, origin in origins.items()
                    if origin == 'absent')
    print(f'{len(bundle["players"])} players, {len(bundle["draft"])} draft '
          f'picks, {len(bundle["rosters"])} rosters, '
          f'{len(bundle["stats"])} statistics files')
    if absent:
        print(f'  not published yet: {", ".join(absent)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
