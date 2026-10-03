#!/usr/bin/env python3
"""Archive CFBD player box scores by season, retaining previously published games.

Run with a season after the game fetch. The current season is refreshed on every
model rebuild; older seasons can be backfilled one at a time. Missing box scores
remain missing rather than becoming invented zeroes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import cfbd_http
import player_archive
from fetch_live_scores import clean_player_boxscores

BASE = 'https://api.collegefootballdata.com'
FIRST_SEASON = 2004  # CFBD's published team/player box-score coverage boundary.


def merge_archive(previous: dict, fresh: dict) -> dict:
    """An empty or delayed response cannot erase a previously archived box score."""
    return {**previous, **{str(k): v for k, v in fresh.items() if v}}


def get_json(path: str, params: dict, key: str):
    url = BASE + path + '?' + urlencode(params)
    request = Request(url, headers={'Authorization': 'Bearer ' + key, 'Accept': 'application/json',
                                   'User-Agent': 'cfb-coefficient-player-archive/1.0'})

    def once():
        with urlopen(request, timeout=120) as response:
            return json.load(response)

    # A backfill makes hundreds of these calls in a row, so one reset must not
    # end the run: on 2026-10-03 a reset on the 2005 calendar stopped a
    # 23-season archive after a single season.
    return cfbd_http.with_retries(once, describe=f'GET {path} {params}')


def fetch_season(year: int, key: str, previous: dict,
                 now: dt.datetime | None = None) -> dict:
    if year < FIRST_SEASON:
        raise ValueError(f'CFBD player box scores begin in {FIRST_SEASON}')
    now = now or dt.datetime.now(dt.timezone.utc)
    calendar = get_json('/calendar', {'year': year}, key)
    if not isinstance(calendar, list) or not calendar:
        raise ValueError(f'No calendar for {year}; keeping existing archive')
    archive = dict(previous)
    for period in calendar:
        if not isinstance(period, dict) or period.get('week') is None:
            continue
        # A current-season archive only needs weeks that have actually begun.
        try:
            start = dt.datetime.fromisoformat(str(period['startDate']).replace('Z', '+00:00'))
            if start > now:
                continue
        except (KeyError, TypeError, ValueError):
            pass
        rows = get_json('/games/players', {'year': year, 'week': period['week'],
                                         'seasonType': period.get('seasonType', 'regular')}, key)
        if not isinstance(rows, list):
            raise ValueError(f'Invalid player response for {year} week {period["week"]}')
        ids = {r.get('id') for r in rows if isinstance(r, dict) and type(r.get('id')) is int}
        archive = merge_archive(archive, clean_player_boxscores(
            rows, ids, max_athletes=None, keep_ids=True))
    return archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('year', type=int, help='First season (2004 or later)')
    parser.add_argument('end_year', type=int, nargs='?', help='Last season for a resumable backfill')
    parser.add_argument('--out-dir', type=Path, default=Path('data/raw/player_boxscores'))
    args = parser.parse_args()
    key = os.environ.get('CFBD_API_KEY')
    if not key:
        parser.error('CFBD_API_KEY is required')
    end = args.end_year or args.year
    if args.year < FIRST_SEASON or end < args.year:
        parser.error(f'Choose a season range from {FIRST_SEASON} onward, in ascending order')
    for year in range(args.year, end + 1):
        previous = player_archive.read_season(args.out_dir, year)
        archive = fetch_season(year, key, previous)
        if archive != previous:
            player_archive.write_season(args.out_dir, year, archive)
        # The numbered count is the point of a re-fetch: a season that comes back
        # with no ids has been archived from a feed that did not carry them, and
        # saying so beats discovering it when no line attributes to anybody.
        lines = numbered = 0
        for teams in archive.values():
            for team in teams:
                for category in team.get('categories', []):
                    for line in category.get('lines', []):
                        lines += 1
                        numbered += 'id' in line
        print(f'{year}: {len(archive)} games with player box scores, '
              f'{numbered} of {lines} lines carry an athlete id', flush=True)


if __name__ == '__main__':
    main()
