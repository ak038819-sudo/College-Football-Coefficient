"""Load the NFL archive into db/nfl.db.

One command does the whole thing, because the source is one file: there is no
per-season paging, no request budget to spread across runs and nothing to
resume. `python -m src.nfl.ingest` fetches, validates and stores.
"""

import argparse
import csv
import io
from datetime import datetime, timezone

from . import games as games_module
from . import source as source_module
from . import store as store_module

TEAMS_URL = ('https://raw.githubusercontent.com/nflverse/nfldata/'
             'master/data/teams.csv')
TEAMS_CACHE = 'teams.csv.gz'


def fetch_teams(*, root=None, reuse_cache=False, opener=None, sleep=None):
    """The source's per-season team names. Cached beside the archive."""
    path = source_module.cache_path(root).with_name(TEAMS_CACHE)
    if reuse_cache and path.exists():
        import gzip
        with gzip.open(path, 'rt', encoding='utf-8') as handle:
            return list(csv.DictReader(io.StringIO(handle.read())))
    text = source_module.fetch_games_csv(TEAMS_URL, opener=opener, sleep=sleep)
    import gzip
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.GzipFile(filename='', mode='wb', fileobj=open(path, 'wb'),
                       mtime=0) as handle:
        handle.write(text.encode('utf-8'))
    return list(csv.DictReader(io.StringIO(text)))


def ingest(connection, *, raw_root=None, reuse_cache=False, opener=None,
           now=None, teams=None, archive_text=None):
    """Fetch (or reuse) the archive and store every game. Idempotent."""
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    if archive_text is None:
        archive_text, origin = source_module.load_games_csv(
            root=raw_root, reuse_cache=reuse_cache, opener=opener)
    else:
        origin = 'given'
    parsed = games_module.parse(archive_text)
    snapshot_id = store_module.record_snapshot(
        connection, 'archive', stamp, source=origin,
        source_revision=source_module.source_revision(),
        rows_seen=len(parsed), notes=source_module.GAMES_URL)
    if teams is None:
        try:
            teams = fetch_teams(root=raw_root, reuse_cache=reuse_cache,
                                opener=opener)
        except source_module.NflSourceError as error:
            # Names are a convenience; the games are the data. A team file
            # that will not fetch leaves the franchises as code stubs rather
            # than failing an ingest that has the archive in hand.
            print(f'Team names unavailable ({error}); storing code stubs')
            teams = []
    if teams:
        store_module.upsert_teams(connection, teams, snapshot_id=snapshot_id)
    store_module.upsert_team_stubs(connection, parsed, snapshot_id=snapshot_id)
    store_module.upsert_games(connection, parsed, snapshot_id=snapshot_id)
    store_module.refresh_team_spans(connection)
    return dict(snapshot_id=snapshot_id, origin=origin, games=len(parsed),
                coverage=games_module.coverage(parsed))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Load the NFL archive')
    parser.add_argument('--db', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--reuse-cache', action='store_true',
                        help='read the committed archive instead of fetching')
    args = parser.parse_args(argv)
    connection = store_module.connect(args.db)
    result = ingest(connection, raw_root=args.raw_root,
                    reuse_cache=args.reuse_cache)
    print(f'{result["games"]} games from the {result["origin"]}')
    for row in result['coverage']:
        print(f'  {row["season"]}  games {row["games"]:>4}  finals '
              f'{row["finals"]:>4}  rateable {row["rateable"]:>4}  '
              f'teams {row["teams"]:>2}  ties {row["ties"]}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
