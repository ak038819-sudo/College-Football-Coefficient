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
from .http import (CbbdAuthError, CbbdError, CbbdRateLimitError, Quota,
                   get_json)
from .people import (box_score_people, fetch_box_scores, fetch_rosters,
                     one_sided_games, roster_players, roster_teams)
from .store import (connect, load_season, record_snapshot, upsert_box_scores,
                    upsert_player_team_seasons, upsert_players,
                    upsert_roster_teams)

RAW_ROOT = Path('data/ncaambb/raw')


# One file per endpoint, so a season's schedule can be reloaded without
# refetching its box scores and the other way round.
CACHE_FILES = ('teams', 'games', 'rosters', 'box_scores')


def cache_paths(season, root=None):
    """Where each endpoint's raw response for a season is cached."""
    base = (Path(root) if root is not None else RAW_ROOT) / str(season)
    return {name: base / f'{name}.json.gz' for name in CACHE_FILES}


# Beside the gzipped responses, one small JSON file per season recording when
# each endpoint was actually fetched. It is committed with the cache, so a run
# that reloads a season from the repository can say how old that season's data
# is instead of dating it to the moment it was read off the disk. Without it
# an export built entirely from cache looked exactly as fresh as one built
# from a fetch, which is the difference between marking staleness and hiding
# it.
FETCHED_AT = 'fetched_at.json'


def fetched_at_path(season, root=None):
    base = (Path(root) if root is not None else RAW_ROOT) / str(season)
    return base / FETCHED_AT


def read_fetch_times(season, root=None):
    """What the cache records about its own age. Missing or unreadable is {}."""
    path = fetched_at_path(season, root)
    try:
        times = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    return {name: value for name, value in times.items()
            if isinstance(value, str) and value} if isinstance(times, dict) \
        else {}


def record_fetch_times(season, names, root=None, now=None):
    """Note that these endpoints were just fetched. Merges, never replaces."""
    moment = (now or datetime.now(timezone.utc)).isoformat()
    times = read_fetch_times(season, root)
    times.update({name: moment for name in names})
    path = fetched_at_path(season, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(times, indent=2, sort_keys=True) + '\n',
                         encoding='utf-8')
    temporary.replace(path)
    return times


def fetched_at(season, names, root=None):
    """The OLDEST recorded fetch among these endpoints, or None.

    Oldest, because a season is only as fresh as the stalest file behind it.
    None when any of them has no record at all: an unknown age must not be
    reported as a known one, and a cache written before this existed has
    none.
    """
    times = read_fetch_times(season, root)
    recorded = [times.get(name) for name in names]
    return min(recorded) if all(recorded) else None


def cache_note(from_cache, age):
    """What this snapshot is, in the words a person reads in the summary."""
    if not from_cache:
        return 'fetched from provider'
    return (f'reused cached responses fetched {age}' if age
            else 'reused cached responses of unrecorded age')


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


def gather(season, *, raw_root=None, reuse_cache=True, getter=None,
           allow_cache_fallback=False, **kwargs):
    """Teams and games for one season, from the cache when it is already there.

    Returns (teams, games, from_cache). A cached season costs no requests,
    which is what makes a reload and a resume cheap.

    `allow_cache_fallback` is for the season being played, the one season a
    refresh asks about again every day. When the provider refuses that
    request and a cache of it exists, falling back to the cache publishes
    yesterday's games marked with yesterday's date, which is what the export
    gate's own rule asks for: staleness is marked, never refused. Refusing
    instead takes the whole page down over a quota that will recover, and
    measured on 2026-11-03 it did exactly that. With no cache there is
    nothing to fall back to and the error stands.
    """
    paths = cache_paths(season, raw_root)
    cached = paths['teams'].exists() and paths['games'].exists()
    if reuse_cache and cached:
        return read_cache(paths['teams']), read_cache(paths['games']), True
    fetch = getter if getter is not None else get_json
    try:
        teams = fetch('/teams', {'season': season}, **kwargs)
        games, _ = fetch_season(season, getter=fetch, **kwargs)
    except CbbdRateLimitError:
        if not (allow_cache_fallback and cached):
            raise
        return read_cache(paths['teams']), read_cache(paths['games']), True
    write_cache(paths['teams'], teams)
    write_cache(paths['games'], games)
    record_fetch_times(season, ('teams', 'games'), raw_root)
    return teams, games, False


def gather_people(season, *, raw_root=None, reuse_cache=True, getter=None,
                  **kwargs):
    """Rosters and box scores for one season, cached the same way.

    Returns (rosters, boxes, from_cache). Cached separately from the schedule
    because the two have different costs: a season's rosters are one request,
    while its box scores are two dozen windows and any split those need.
    """
    paths = cache_paths(season, raw_root)
    if reuse_cache and paths['rosters'].exists() and paths['box_scores'].exists():
        return read_cache(paths['rosters']), read_cache(paths['box_scores']), True
    fetch = getter if getter is not None else get_json
    rosters = fetch_rosters(season, getter=fetch, **kwargs)
    boxes, _ = fetch_box_scores(season, getter=fetch, **kwargs)
    write_cache(paths['rosters'], rosters)
    write_cache(paths['box_scores'], boxes)
    record_fetch_times(season, ('rosters', 'box_scores'), raw_root)
    return rosters, boxes, False


def ingest_people(connection, season, *, raw_root=None, reuse_cache=True,
                  getter=None, quota=None, revision=None, **kwargs):
    """Store one season's rosters, player identities and box score lines.

    Identities come from the rosters AND from the box scores, because a player
    can record a stat line without appearing in the roster snapshot and the
    line must not be the thing that gets dropped.

    Box score lines for games this database does not hold are counted, not
    stored: that count is the signal that the schedule load was incomplete.
    """
    counter = quota if quota is not None else Quota()
    rosters, boxes, from_cache = gather_people(
        season, raw_root=raw_root, reuse_cache=reuse_cache, getter=getter,
        quota=counter, **kwargs)
    pairs = roster_players(rosters)
    people = [player for player, _ in pairs] + box_score_people(boxes)
    age = fetched_at(season, ('rosters', 'box_scores'), raw_root)
    snapshot_id = record_snapshot(
        connection, 'people', datetime.now(timezone.utc).isoformat(),
        season=season, data_fetched_at=age, source_revision=revision,
        requests=counter.requests, retries=counter.retries,
        rows_seen=len(boxes),
        notes=cache_note(from_cache, age))
    stored_people = upsert_players(connection, people, snapshot_id=snapshot_id)
    roster_only_teams = upsert_roster_teams(connection, season,
                                            roster_teams(rosters),
                                            snapshot_id=snapshot_id)
    memberships, unknown_teams = upsert_player_team_seasons(
        connection, season, pairs, snapshot_id=snapshot_id)
    lines = upsert_box_scores(connection, boxes, snapshot_id=snapshot_id)
    return dict(season=season, from_cache=from_cache, snapshot_id=snapshot_id,
                rosters=len(rosters), people=stored_people,
                memberships=memberships,
                roster_only_teams=roster_only_teams,
                half_games=len(one_sided_games(boxes)),
                skipped_unknown_teams=unknown_teams, **lines)


def ingest_season(connection, season, *, raw_root=None, reuse_cache=True,
                  getter=None, quota=None, revision=None,
                  allow_cache_fallback=False, **kwargs):
    """Fetch or reuse one season, store it, and summarize what was stored."""
    counter = quota if quota is not None else Quota()
    teams, games, from_cache = gather(season, raw_root=raw_root,
                                      reuse_cache=reuse_cache, getter=getter,
                                      quota=counter,
                                      allow_cache_fallback=allow_cache_fallback,
                                      **kwargs)
    age = fetched_at(season, ('teams', 'games'), raw_root)
    snapshot_id = record_snapshot(
        connection, 'schedule', datetime.now(timezone.utc).isoformat(),
        season=season, data_fetched_at=age, source_revision=revision,
        requests=counter.requests, retries=counter.retries,
        rows_seen=len(games),
        notes=cache_note(from_cache, age))
    stored = load_season(connection, season, games, teams,
                         snapshot_id=snapshot_id)
    summary = summarize_season(season, games,
                               {team['id'] for team in teams})
    # A run that ASKED for a refetch and got the cache anyway was refused.
    # Inferred rather than passed back through another return value, so the
    # two cannot drift apart.
    summary.update(from_cache=from_cache, snapshot_id=snapshot_id,
                   refetch_refused=bool(from_cache and not reuse_cache),
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
    parser.add_argument('--refetch-seasons', default=None,
                        help='Comma-separated seasons whose cache is ignored '
                             'while the rest are reused. This is how a '
                             'scheduled refresh asks again about the season '
                             'being played without paying for the settled '
                             'archive: a cached season is never asked about '
                             'again, so without this the active season would '
                             'be frozen at the first run that cached it, and '
                             'every later export would carry a new '
                             'generated_at over unchanged games.')
    parser.add_argument('--schedule-only', action='store_true',
                        help='Skip rosters and box scores')
    args = parser.parse_args(argv)
    try:
        seasons = [int(part) for part in args.seasons.split(',') if part.strip()]
    except ValueError:
        parser.error('Seasons must be integer ending years')
    if not seasons:
        parser.error('Give at least one season')
    try:
        refetch = {int(part) for part in
                   (args.refetch_seasons or '').split(',') if part.strip()}
    except ValueError:
        parser.error('--refetch-seasons must be integer ending years')
    unasked = sorted(refetch - set(seasons))
    if unasked:
        # Naming a season here that --seasons does not include is a mistake
        # worth refusing: it reads as "this season is refreshed" while nothing
        # fetches it at all.
        parser.error(f'--refetch-seasons names {unasked}, which --seasons does '
                     'not include')

    revision = source_revision()
    quota = Quota()
    connection = connect(args.database)
    summaries, people, failed = [], [], []
    rate_limited = None
    try:
        for season in seasons:
            try:
                reuse = not (args.refetch or season in refetch)
                summaries.append(ingest_season(
                    connection, season, raw_root=args.raw_root,
                    reuse_cache=reuse, quota=quota,
                    revision=revision,
                    # Only for a season this run asked to refetch, and only
                    # when a cache of it exists. A blanket --refetch is a
                    # deliberate "ignore the cache", so falling back to it
                    # would answer the opposite question.
                    allow_cache_fallback=not reuse and not args.refetch))
                if not args.schedule_only:
                    people.append(ingest_people(
                        connection, season, raw_root=args.raw_root,
                        reuse_cache=reuse, quota=quota,
                        revision=revision))
            except CbbdAuthError as exc:
                # Not a coverage result. Stop rather than report absent data.
                print(f'Cannot reach the provider: {exc}', file=sys.stderr)
                return 2
            except CbbdRateLimitError as exc:
                # Also not a coverage result, and also not worth continuing:
                # whatever is behind the 429, it held for every attempt on this
                # season's first request, so the seasons after it would each
                # burn a full backoff to fail the same way -- and would then be
                # reported as "not stored", which reads as absent data.
                print(f'Rate limited by the provider: {exc}', file=sys.stderr)
                rate_limited = season
                break
            except (CbbdError, ValueError) as exc:
                failed.append(dict(season=season, error=str(exc)))
    finally:
        connection.close()

    print('\n'.join(render(summaries)))
    for problem in failed:
        print(f'\n- season {problem["season"]} was not stored: {problem["error"]}')
    refused = [row['season'] for row in summaries
               if row.get('refetch_refused')]
    if refused:
        names = ', '.join(str(year) for year in refused)
        print(f'\nThe provider refused a refetch of season(s) {names}, so'
              ' each was loaded from its committed cache instead. The games'
              ' are as of when that cache was fetched, NOT as of now, and'
              ' every export built from this database says so in its'
              ' `data_as_of` -- a reader sees the real age and a stale'
              ' badge. Nothing here is a statement about coverage.')
    if rate_limited is not None:
        remaining = [year for year in seasons if year >= rate_limited]
        print(f'\nStopped at season {rate_limited}: the provider refused with'
              ' HTTP 429 and retrying did not clear it. This is NOT a'
              ' statement about coverage;'
              f' {len(remaining)} season(s) were not asked for at all'
              f' ({", ".join(str(year) for year in remaining)}). Whatever was'
              ' fetched before this point is cached and a later run will reuse'
              ' it, so re-running once the quota recovers resumes rather than'
              ' starts over.')
    print(f'\nRequests sent: {quota.requests} ({quota.retries} retries)')
    for row in summaries:
        print(f'- season {row["season"]}: stored {row["stored_games"]} games,'
              f' {row["placeholder_teams"]} non-Division I placeholders,'
              f' {"reused cache" if row["from_cache"] else "fetched"}')
    for row in people:
        print(f'- season {row["season"]} people: {row["people"]} players,'
              f' {row["memberships"]} roster memberships,'
              f' {row["team_lines"]} team box scores,'
              f' {row["player_lines"]} player lines')
        if row['half_games']:
            print(f'    {row["half_games"]} games arrived with only one side\'s'
                  ' box score, which is what a response truncated at the row'
                  ' cap looks like')
        if row['roster_only_teams']:
            print(f'    {row["roster_only_teams"]} teams were named only by the'
                  " roster feed, not by the season's team list, and were"
                  ' registered as non-Division I')
        if row['skipped_unknown_teams']:
            print(f'    {row["skipped_unknown_teams"]} roster memberships name'
                  ' a team the season\'s team list does not carry, so they'
                  ' were counted and not stored')
        if row['skipped_unknown_games']:
            print(f'    {row["skipped_unknown_games"]} box score lines were for'
                  ' games this database does not hold, so the schedule load for'
                  ' this season is incomplete')
        if row['points_mismatches']:
            print(f'    {row["points_mismatches"]} box scores disagree with the'
                  " game's own final score")
    return 1 if failed or rate_limited is not None or not summaries else 0


if __name__ == '__main__':
    sys.exit(main())
