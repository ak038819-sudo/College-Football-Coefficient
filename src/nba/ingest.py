"""Load the NBA archive into db/nba.db.

`python -m src.nba.ingest` fetches every season's schedule and standings and
stores them. Unlike the NFL's one-file archive there is a file per season, so
this takes `--seasons` for a partial run, but the default is the whole
archive: nothing here is metered and the database is built from scratch on
every refresh, which is what keeps it in step with a source that revises
yesterday's rows.

A SEASON THAT WILL NOT FETCH FAILS THE RUN. The alternative — carrying on and
storing the seasons that did — writes a database that is quietly short of a
year, and every count taken from it afterwards is wrong without looking
wrong. The one exception is the standings, which only supply conference
labels: those are a convenience, and a missing one leaves the column NULL
rather than losing a season of games.
"""

import argparse
from datetime import datetime, timezone

from . import games as games_module
from . import source as source_module
from . import store as store_module


def conferences_for(season, *, raw_root=None, reuse_cache=False, opener=None,
                    sleep=None, rows=None):
    """team id -> conference name for one season, from the standings feed.

    The standings file is long-format — one row per team per statistic — so
    it is read for its grouping columns alone. Returns {} rather than raising
    when the file is absent: the season in progress has no standings file
    until the source writes one, and a missing conference label is not worth
    failing an ingest that has the games in hand.
    """
    if rows is None:
        try:
            path, _ = source_module.load_asset(
                'standings', season, root=raw_root, reuse_cache=reuse_cache,
                opener=opener, sleep=sleep)
            rows = source_module.read_parquet(path)
        except source_module.NbaSourceError:
            return {}
    found = {}
    for row in rows:
        team_id, name = row.get('team_id'), row.get('group_name')
        if team_id is None or not name:
            continue
        found[int(team_id)] = ' '.join(str(name).split())
    return found


def ingest_season(connection, season, *, raw_root=None, reuse_cache=False,
                  opener=None, now=None, schedule_rows=None,
                  standings_rows=None):
    """Fetch (or reuse) one season's schedule and store it. Idempotent."""
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    if schedule_rows is None:
        path, origin = source_module.load_asset(
            'schedule', season, root=raw_root, reuse_cache=reuse_cache,
            opener=opener)
        schedule_rows = source_module.read_parquet(path)
    else:
        origin = 'given'
    parsed, skipped = games_module.parse(schedule_rows)
    snapshot_id = store_module.record_snapshot(
        connection, 'schedule', stamp, season=season, source=origin,
        source_revision=source_module.source_revision(),
        rows_seen=len(schedule_rows),
        notes=source_module.asset_url('schedule', season))
    conferences = conferences_for(
        season, raw_root=raw_root, reuse_cache=reuse_cache, opener=opener,
        rows=standings_rows)
    store_module.upsert_team_seasons(
        connection, games_module.teams_from_schedule(schedule_rows, season),
        conferences=conferences, snapshot_id=snapshot_id)
    store_module.upsert_games(connection, parsed, snapshot_id=snapshot_id)
    # The schedule just parsed is the whole truth about this season, so a
    # stored game it does not name is gone from the archive. Without this the
    # database is the union of every schedule ever ingested.
    dropped = store_module.reconcile_season_games(
        connection, season, [game['game_key'] for game in parsed])
    return dict(season=season, origin=origin, games=len(parsed),
                exhibitions=skipped, conferences=len(conferences),
                dropped=dropped, snapshot_id=snapshot_id)


def ingest(connection, seasons, *, raw_root=None, reuse_cache=False,
           opener=None, now=None):
    """Every season named, oldest first. A failure stops the run."""
    results = []
    for season in seasons:
        results.append(ingest_season(
            connection, season, raw_root=raw_root, reuse_cache=reuse_cache,
            opener=opener, now=now))
    store_module.refresh_team_identity(connection)
    return results


def parse_seasons(text, *, now=None):
    """`--seasons 2024,2025` or `2024-2026`, or every season when empty."""
    moment = now or datetime.now(timezone.utc)
    if not text:
        return source_module.available_seasons(moment)
    seasons = set()
    for piece in str(text).replace(' ', '').split(','):
        if not piece:
            continue
        if '-' in piece.lstrip('-'):
            low, _, high = piece.partition('-')
            seasons.update(range(int(low), int(high) + 1))
        else:
            seasons.add(int(piece))
    return sorted(seasons)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Load the NBA archive')
    parser.add_argument('--db', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default='',
                        help='e.g. 2026 or 2024-2027; default is all of them')
    parser.add_argument('--reuse-cache', action='store_true',
                        help='read the cached files instead of fetching')
    args = parser.parse_args(argv)
    seasons = parse_seasons(args.seasons)
    connection = store_module.connect(args.db)
    results = ingest(connection, seasons, raw_root=args.raw_root,
                     reuse_cache=args.reuse_cache)
    games = sum(row['games'] for row in results)
    exhibitions = sum(row['exhibitions'] for row in results)
    dropped = sum(len(row['dropped']) for row in results)
    print(f'{games} games across {len(results)} seasons '
          f'({exhibitions} exhibitions skipped)')
    if dropped:
        print(f'{dropped} stored games are no longer in the schedule and '
              'were removed')
    for row in store_module.season_coverage(connection):
        print(f'  {row["label"]:>8}  games {row["games"]:>4}  '
              f'played {row["played"]:>4}  rateable {row["rateable"]:>4}  '
              f'teams {row["home_teams"]:>2}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
