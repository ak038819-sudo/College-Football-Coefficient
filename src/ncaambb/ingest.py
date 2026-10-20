"""National basketball ingestion: fetch, cache, load, report.

Raw provider responses are cached per season so a reload needs no requests and
a resumed run does not refetch what it already has. Loading is idempotent, so
running this twice is safe by design rather than by luck.

Nothing here commits or publishes. It writes the basketball database and the
raw cache; the publishing gate in B04 does not exist yet.
"""

import argparse
import gzip
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from .coverage import render, summarize_season
from .games import fetch_season
from .http import CbbdAuthError, CbbdError, Quota, get_json
from .store import connect, load_season, record_snapshot

RAW_ROOT = Path('data/ncaambb/raw')


def cache_paths(season, root=None):
    base = (Path(root) if root is not None else RAW_ROOT) / str(season)
    return base / 'teams.json.gz', base / 'games.json.gz'


def write_cache(path, payload):
    """Write atomically, so an interrupted run leaves no half file to reuse."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with gzip.open(temporary, 'wt', encoding='utf-8') as handle:
        json.dump(payload, handle, sort_keys=True)
    temporary.replace(path)
    return path


def read_cache(path):
    with gzip.open(path, 'rt', encoding='utf-8') as handle:
        return json.load(handle)


def source_revision():
    """The commit the ingestion ran from, so stored rows are traceable."""
    try:
        result = subprocess.run(['git', 'rev-parse', 'HEAD'],
                                capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None if result.returncode == 0 else None


def gather(season, *, raw_root=None, reuse_cache=True, getter=None, **kwargs):
    """Teams and games for one season, from the cache when it is already there.

    Returns (teams, games, from_cache). A cached season costs no requests,
    which is what makes a reload and a resume cheap.
    """
    teams_path, games_path = cache_paths(season, raw_root)
    if reuse_cache and teams_path.exists() and games_path.exists():
        return read_cache(teams_path), read_cache(games_path), True
    fetch = getter if getter is not None else get_json
    teams = fetch('/teams', {'season': season}, **kwargs)
    games, _ = fetch_season(season, getter=fetch, **kwargs)
    write_cache(teams_path, teams)
    write_cache(games_path, games)
    return teams, games, False


def ingest_season(connection, season, *, raw_root=None, reuse_cache=True,
                  getter=None, quota=None, revision=None, **kwargs):
    """Fetch or reuse one season, store it, and summarize what was stored."""
    counter = quota if quota is not None else Quota()
    teams, games, from_cache = gather(season, raw_root=raw_root,
                                      reuse_cache=reuse_cache, getter=getter,
                                      quota=counter, **kwargs)
    snapshot_id = record_snapshot(
        connection, 'schedule', datetime.now(timezone.utc).isoformat(),
        season=season, source_revision=revision,
        requests=counter.requests, retries=counter.retries,
        rows_seen=len(games),
        notes='reused cached responses' if from_cache else 'fetched from provider')
    stored = load_season(connection, season, games, teams,
                         snapshot_id=snapshot_id)
    summary = summarize_season(season, games,
                               {team['id'] for team in teams})
    summary.update(from_cache=from_cache, snapshot_id=snapshot_id,
                   stored_games=stored['games'],
                   placeholder_teams=stored['other_teams'])
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seasons', required=True,
                        help='Comma-separated ending years, e.g. 2025,2026,2027')
    parser.add_argument('--database', default=None,
                        help='Defaults to db/ncaambb.db')
    parser.add_argument('--raw-root', default=None,
                        help=f'Defaults to {RAW_ROOT}')
    parser.add_argument('--refetch', action='store_true',
                        help='Ignore the raw cache and request everything again')
    args = parser.parse_args(argv)
    try:
        seasons = [int(part) for part in args.seasons.split(',') if part.strip()]
    except ValueError:
        parser.error('Seasons must be integer ending years')
    if not seasons:
        parser.error('Give at least one season')

    revision = source_revision()
    quota = Quota()
    connection = connect(args.database)
    summaries, failed = [], []
    try:
        for season in seasons:
            try:
                summaries.append(ingest_season(
                    connection, season, raw_root=args.raw_root,
                    reuse_cache=not args.refetch, quota=quota,
                    revision=revision))
            except CbbdAuthError as exc:
                # Not a coverage result. Stop rather than report absent data.
                print(f'Cannot reach the provider: {exc}', file=sys.stderr)
                return 2
            except (CbbdError, ValueError) as exc:
                failed.append(dict(season=season, error=str(exc)))
    finally:
        connection.close()

    print('\n'.join(render(summaries)))
    for problem in failed:
        print(f'\n- season {problem["season"]} was not stored: {problem["error"]}')
    print(f'\nRequests sent: {quota.requests} ({quota.retries} retries)')
    for row in summaries:
        print(f'- season {row["season"]}: stored {row["stored_games"]} games,'
              f' {row["placeholder_teams"]} non-Division I placeholders,'
              f' {"reused cache" if row["from_cache"] else "fetched"}')
    return 1 if failed or not summaries else 0


if __name__ == '__main__':
    sys.exit(main())
