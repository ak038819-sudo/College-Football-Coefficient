"""Measure what the provider actually has, per season. Fetches, stores nothing.

This answers the questions B02's schema and ingestion depend on: how many games
a season really holds, whether the date partitioning is working, which seasons
have Division I team lists, and whether the upcoming season's schedule is
published yet. It writes no database and no data file.
"""

import argparse
import json
import sys

from .coverage import render, summarize_season
from .games import fetch_season
from .http import CbbdAuthError, CbbdError, Quota, get_json


def division_one_ids(season, *, getter=None, **kwargs):
    """Team ids the provider lists for a season, i.e. its Division I members."""
    fetch = getter if getter is not None else get_json
    teams = fetch('/teams', {'season': season}, **kwargs)
    return {team['id'] for team in teams if type(team.get('id')) is int}


def probe(seasons, *, getter=None, **kwargs):
    """Summarize each season. A season with no games is reported, not an error.

    A data or coverage failure is recorded against its season and the rest of
    the seasons still run. An access failure is not: it aborts, because
    continuing would report missing access as missing coverage.
    """
    summaries, problems = [], []
    for season in seasons:
        try:
            members = division_one_ids(season, getter=getter, **kwargs)
            games, stats = fetch_season(season, getter=getter, **kwargs)
        except CbbdAuthError:
            raise
        except CbbdError as exc:
            problems.append(dict(season=season, error=str(exc)))
            continue
        summary = summarize_season(season, games, members)
        summary.update(windows_requested=stats['windows_requested'],
                       duplicate_rows=stats['duplicates'])
        summaries.append(summary)
    return summaries, problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seasons', required=True,
                        help='Comma-separated ending years, e.g. 2024,2025,2026,2027')
    args = parser.parse_args(argv)
    try:
        seasons = [int(part) for part in args.seasons.split(',') if part.strip()]
    except ValueError:
        parser.error('Seasons must be integer ending years')
    if not seasons:
        parser.error('Give at least one season')
    quota = Quota()
    try:
        summaries, problems = probe(seasons, quota=quota)
    except CbbdAuthError as exc:
        # Loud and immediate: this is not a coverage result.
        print(f'Cannot reach the provider: {exc}', file=sys.stderr)
        return 2
    print('\n'.join(render(summaries)))
    print()
    for problem in problems:
        print(f'- season {problem["season"]} could not be measured: {problem["error"]}')
    print(f'\nRequests sent: {quota.requests} ({quota.retries} retries)')
    print('\nMachine-readable summary:')
    print(json.dumps(dict(seasons=summaries, problems=problems,
                          quota=quota.as_dict()), indent=2, sort_keys=True))
    return 0 if summaries else 1


if __name__ == '__main__':
    sys.exit(main())
