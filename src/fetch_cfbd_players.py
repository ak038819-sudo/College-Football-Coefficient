#!/usr/bin/env python3
"""Archive CFBD player box scores by season, retaining previously published games.

Run with a season after the game fetch. The current season is refreshed on every
model rebuild; older seasons can be backfilled one at a time. Missing box scores
remain missing rather than becoming invented zeroes.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fetch_live_scores import clean_player_boxscores

BASE = 'https://api.collegefootballdata.com'


def merge_archive(previous: dict, fresh: dict) -> dict:
    """An empty or delayed response cannot erase a previously archived box score."""
    return {**previous, **{str(k): v for k, v in fresh.items() if v}}


def get_json(path: str, params: dict, key: str):
    url = BASE + path + '?' + urlencode(params)
    request = Request(url, headers={'Authorization': 'Bearer ' + key, 'Accept': 'application/json',
                                   'User-Agent': 'cfb-coefficient-player-archive/1.0'})
    with urlopen(request, timeout=120) as response:
        return json.load(response)


def fetch_season(year: int, key: str, previous: dict) -> dict:
    calendar = get_json('/calendar', {'year': year}, key)
    if not isinstance(calendar, list) or not calendar:
        raise ValueError(f'No calendar for {year}; keeping existing archive')
    archive = dict(previous)
    for period in calendar:
        if not isinstance(period, dict) or period.get('week') is None:
            continue
        rows = get_json('/games/players', {'year': year, 'week': period['week'],
                                         'seasonType': period.get('seasonType', 'regular')}, key)
        if not isinstance(rows, list):
            raise ValueError(f'Invalid player response for {year} week {period["week"]}')
        ids = {r.get('id') for r in rows if isinstance(r, dict) and type(r.get('id')) is int}
        archive = merge_archive(archive, clean_player_boxscores(rows, ids, max_athletes=None))
    return archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('year', type=int)
    parser.add_argument('--out-dir', type=Path, default=Path('data/raw/player_boxscores'))
    args = parser.parse_args()
    key = os.environ.get('CFBD_API_KEY')
    if not key:
        parser.error('CFBD_API_KEY is required')
    path = args.out_dir / f'{args.year}.json'
    previous = json.loads(path.read_text()) if path.exists() else {}
    archive = fetch_season(args.year, key, previous)
    if archive != previous:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix('.tmp')
        temp.write_text(json.dumps(archive, ensure_ascii=False, separators=(',', ':')) + '\n')
        temp.replace(path)
    print(f'{args.year}: {len(archive)} games with player box scores')


if __name__ == '__main__':
    main()
