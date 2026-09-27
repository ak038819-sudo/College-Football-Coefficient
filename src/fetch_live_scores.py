#!/usr/bin/env python3
"""Publish a small, public scoreboard snapshot from CFBD's subscriber endpoint.

The API key stays on the runner. A failed request leaves the last good snapshot
untouched. This feed is display-only and never updates historical model inputs.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

URL = 'https://api.collegefootballdata.com/scoreboard?classification=fbs'
DEFAULT_OUT = Path('ui/data/live_scores.json')


def clean_game(item):
    if not isinstance(item, dict) or not isinstance(item.get('id'), int):
        raise ValueError('invalid scoreboard game ID')
    if item.get('status') not in ('scheduled', 'in_progress', 'completed'):
        raise ValueError('invalid scoreboard status')
    teams = []
    for key in ('awayTeam', 'homeTeam'):
        team = item.get(key)
        if not isinstance(team, dict) or not isinstance(team.get('name'), str) or not team['name']:
            raise ValueError('invalid scoreboard team')
        points = team.get('points')
        if points is not None and (type(points) is not int or points < 0):
            raise ValueError('invalid scoreboard points')
        teams.append({'id': team.get('id') if type(team.get('id')) is int else None,
                      'name': team['name'], 'points': points,
                      'classification': team.get('classification')})
    return {'id': item['id'], 'start_date': item.get('startDate'),
            'status': item['status'], 'period': item.get('period'),
            'clock': item.get('clock'), 'tv': item.get('tv'),
            'neutral_site': item.get('neutralSite') is True,
            'away': teams[0], 'home': teams[1]}


def clean_player_boxscores(payload, game_ids):
    """Keep only displayable player lines for games in the current scoreboard."""
    if not isinstance(payload, list):
        raise ValueError('invalid player box-score response')
    result = {}
    for game in payload:
        if not isinstance(game, dict) or game.get('id') not in game_ids:
            continue
        teams = []
        for team in game.get('teams', []):
            if not isinstance(team, dict):
                continue
            categories = []
            for category in team.get('categories', []):
                for typ in category.get('types', []):
                    lines = [{'name': str(a['name'])[:100], 'stat': str(a['stat'])[:80]}
                             for a in typ.get('athletes', [])[:8]
                             if isinstance(a, dict) and a.get('name') and a.get('stat') is not None]
                    if lines:
                        categories.append({'name': str(category.get('name', ''))[:60],
                                           'type': str(typ.get('name', ''))[:60], 'lines': lines})
            teams.append({'name': str(team.get('team', ''))[:100],
                          'home_away': team.get('homeAway'), 'categories': categories[:24]})
        result[str(game['id'])] = teams[:2]
    return result


def fetch_player_boxscores(key, games, now):
    """One weekly batch request; failure never blocks the scoreboard."""
    active = {g['id'] for g in games if g['status'] != 'scheduled'}
    if not active:
        return {}
    year = now.year - 1 if now.month <= 2 else now.year
    def get(path, params):
        request = Request('https://api.collegefootballdata.com' + path + '?' + urlencode(params),
                          headers={'Authorization': 'Bearer ' + key, 'Accept': 'application/json',
                                   'User-Agent': 'cfb-coefficient-live-scores/1.0'})
        with urlopen(request, timeout=25) as response:
            return json.load(response)
    calendar = get('/calendar', {'year': year})
    if not isinstance(calendar, list):
        raise ValueError('invalid calendar response')
    candidates = []
    for week in calendar:
        try:
            start = dt.datetime.fromisoformat(week['startDate'].replace('Z', '+00:00'))
            end = dt.datetime.fromisoformat(week['endDate'].replace('Z', '+00:00'))
            if start <= now <= end + dt.timedelta(days=1):
                candidates.append((start, week))
        except (KeyError, TypeError, ValueError):
            continue
    if not candidates:
        return {}
    week = max(candidates, key=lambda pair: pair[0])[1]
    payload = get('/games/players', {'year': year, 'week': week['week'],
                                     'seasonType': week.get('seasonType', 'regular')})
    return clean_player_boxscores(payload, active)


def make_snapshot(payload, now):
    if not isinstance(payload, list):
        raise ValueError('scoreboard response must be a list')
    games = [clean_game(item) for item in payload]
    if len({g['id'] for g in games}) != len(games):
        raise ValueError('duplicate scoreboard game ID')
    return {'source': 'CollegeFootballData.com', 'updated_at': now.isoformat().replace('+00:00', 'Z'),
            'games': games}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    key = os.getenv('CFBD_API_KEY')
    if not key:
        parser.error('CFBD_API_KEY is required (live scoreboard requires a subscribed key)')
    request = Request(URL, headers={'Authorization': 'Bearer ' + key,
                                   'Accept': 'application/json', 'User-Agent': 'cfb-coefficient-live-scores/1.0'})
    with urlopen(request, timeout=25) as response:
        snapshot = make_snapshot(json.load(response), dt.datetime.now(dt.timezone.utc))
    try:
        snapshot['player_boxscores'] = fetch_player_boxscores(key, snapshot['games'], dt.datetime.now(dt.timezone.utc))
    except (OSError, ValueError, KeyError) as exc:
        print(f'Player box scores unavailable: {exc}')
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.out.with_suffix(args.out.suffix + '.tmp')
    temporary.write_text(json.dumps(snapshot, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')
    temporary.replace(args.out)
    print(f"Published {len(snapshot['games'])} scoreboard games at {snapshot['updated_at']}")


if __name__ == '__main__':
    main()
