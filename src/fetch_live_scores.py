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

import cfbd_http

URL = 'https://api.collegefootballdata.com/scoreboard?classification=fbs'
DEFAULT_OUT = Path('ui/data/live_scores.json')
# Three attempts, not the usual five: this runs every five minutes in season,
# so the backoff has to finish well inside that. 2s + 4s is a reset ridden out
# without the scoreboard going stale; a longer outage is the next run's
# problem, and the last good snapshot stays published meanwhile.
ATTEMPTS = 3
# How often the player box-score batch is refetched, independent of how often
# the scoreboard is polled. Fifteen minutes on a weekly box score is generous,
# and `boxscores_due` explains why the two cadences are separate at all.
BOXSCORE_INTERVAL = 900


def clean_venue(venue):
    """The venue's name, from either shape CFBD might send.

    THIS WAS A SILENT, TOTAL DATA LOSS, found 2026-11-12. `/scoreboard` sends
    `venue` as an OBJECT -- `{"state": ..., "city": ..., "name": ...}`, which is
    what CFBD's own generated client declares (`ScoreboardGameVenue`) -- and
    this function previously accepted only a string:

        venue.strip()[:120] if isinstance(venue, str) and venue.strip() else None

    So every live game published `"venue": null`. Measured across three
    snapshots from October and November 2026 plus the file published that day:
    **0 of 238 games carried a venue.** The page's game detail reads
    `g.venue || archive?.stadium || UPCOMING_BY_ID...`, so a live game absent
    from the static archive read "Venue unverified" -- and did, for as long as
    these snapshots go back.

    IT SURVIVED BECAUSE THE TEST FIXTURE WAS WRITTEN FROM AN ASSUMPTION rather
    than from the wire. `tests/test_live_scores.py` passes
    `'venue': 'LaVell Edwards Stadium'`, a string, and asserts it comes through,
    which it does. The guard reads like validation and behaves like a filter,
    and nothing compared it against a real response. Both shapes are accepted
    below so the fix cannot itself become the next version of this bug.
    """
    if isinstance(venue, dict):
        venue = venue.get('name')
    if not isinstance(venue, str):
        return None
    trimmed = venue.strip()
    # 120 characters, as before: the published payload is size-sensitive.
    return trimmed[:120] if trimmed else None


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
            'venue': clean_venue(item.get('venue')),
            'status': item['status'], 'period': item.get('period'),
            'clock': item.get('clock'), 'tv': item.get('tv'),
            'neutral_site': item.get('neutralSite') is True,
            'away': teams[0], 'home': teams[1]}


def _player_line(athlete, keep_ids):
    line = {'name': str(athlete['name'])[:100], 'stat': str(athlete['stat'])[:80]}
    if keep_ids:
        # Whatever key the feed uses, and only if it is actually a number: an
        # id this project cannot match to a CFBD athlete id is not an identity,
        # and storing a name-shaped one would invite exactly the name matching
        # the id exists to replace.
        for key in ('id', 'athleteId', 'athlete_id'):
            value = athlete.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text.lstrip('-').isdigit():
                line['id'] = text
                break
    return line


def clean_player_boxscores(payload, game_ids, max_athletes=8, keep_ids=False):
    """Keep only displayable player lines for games in the current scoreboard.

    `keep_ids` carries CFBD's athlete id through onto each line, which is what
    lets a box-score line be ATTRIBUTED to a person rather than matched to one
    by name. It is off by default because this function also builds the live
    scoreboard file, which every visitor downloads on every page load: ids would
    add about 96 KB to a 358 KB file for games whose lines the roster-name rule
    already links. The archive pays no such toll -- it is gzipped and read only
    when a game page opens -- so the backfill asks for them.

    A line gets no `id` key at all when the source gave none, rather than a null:
    a reader must be able to tell "CFBD did not number this line" from "the line
    is numbered 0", and an absent key says the first without being mistaken for
    an identity.
    """
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
                    athletes = typ.get('athletes', [])
                    lines = [_player_line(a, keep_ids)
                             for a in (athletes if max_athletes is None else athletes[:max_athletes])
                             if isinstance(a, dict) and a.get('name') and a.get('stat') is not None]
                    if lines:
                        categories.append({'name': str(category.get('name', ''))[:60],
                                           'type': str(typ.get('name', ''))[:60], 'lines': lines})
            teams.append({'name': str(team.get('team', ''))[:100],
                          'home_away': team.get('homeAway'), 'categories': categories[:24]})
        # An empty response for a game can precede CFBD's completed box score.
        # Keep the last published lines instead of replacing them with blanks.
        if any(category['lines'] for team in teams for category in team['categories']):
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
        def once():
            with urlopen(request, timeout=25) as response:
                return json.load(response)
        return cfbd_http.with_retries(once, describe='GET ' + path, attempts=ATTEMPTS)
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


def boxscores_due(previous, now, interval=None):
    """Is the player box-score batch due, or can this tick skip it?

    WHY THIS EXISTS. Every tick used to make THREE CFBD requests -- /scoreboard
    for the live scores, plus /calendar and /games/players for the weekly player
    batch -- and `run_live_score_session.py` says so beside its idle interval:
    "Three CFBD requests a tick is what makes it worth slowing down at all." So
    the request cost of the live scoreboard was three times what the live
    scoreboard actually needs, and that multiplier is what makes a faster
    cadence unaffordable. The scoreboard genuinely needs one request per tick.
    The player batch does not need six-second, six-minute or even hourly
    granularity: it is a weekly box score, and the page labels it with
    `player_stats_checked_at` so its age is disclosed rather than hidden.

    Splitting them cuts the live cycle to one request on most ticks, and the
    saving grows as the cadence gets faster -- at a fifteen-second cadence it is
    one extra request in sixty rather than two in every one. That is the whole
    point: this is what makes fast polling affordable.

    THE DECISION IS MADE FROM THE PUBLISHED FILE, not from state held in a
    process, because the two callers differ. A polling session could remember
    when it last fetched; the five-minute cron is a fresh process every time and
    could not. Reading `player_stats_checked_at` out of the previous snapshot
    works identically for both and needs nothing plumbed through.

    Anything unreadable means DUE. A missing, unparseable or future-dated stamp
    is not evidence that a fetch happened recently, and the failure mode of
    guessing the other way is never fetching again.
    """
    if interval is None:
        interval = BOXSCORE_INTERVAL
    stamp = (previous or {}).get('player_stats_checked_at')
    if not isinstance(stamp, str) or not stamp:
        return True
    try:
        last = dt.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    except ValueError:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    age = (now - last).total_seconds()
    # A negative age is a clock disagreement, not a recent fetch.
    return not 0 <= age < interval


def make_snapshot(payload, now):
    if not isinstance(payload, list):
        raise ValueError('scoreboard response must be a list')
    games = [clean_game(item) for item in payload]
    if len({g['id'] for g in games}) != len(games):
        raise ValueError('duplicate scoreboard game ID')
    return {'source': 'CollegeFootballData.com', 'updated_at': now.isoformat().replace('+00:00', 'Z'),
            'games': games}


def merge_player_boxscores(snapshot, previous, fresh, checked_at):
    """Preserve previously published lines for games still on the scoreboard."""
    ids = {str(game['id']) for game in snapshot['games']}
    old = previous if isinstance(previous, dict) else {}
    saved = old.get('player_boxscores') or {}
    times = old.get('player_boxscore_times') or {}
    if not isinstance(saved, dict):
        saved = {}
    if not isinstance(times, dict):
        times = {}
    snapshot['player_boxscores'] = {key: value for key, value in saved.items()
                                    if key in ids and isinstance(value, list) and value}
    snapshot['player_boxscore_times'] = {key: value for key, value in times.items()
                                         if key in snapshot['player_boxscores']}
    if fresh is not None:
        snapshot['player_stats_checked_at'] = checked_at
        snapshot['player_boxscores'].update(fresh)
        snapshot['player_boxscore_times'].update({key: checked_at for key in fresh})
    elif old.get('player_stats_checked_at'):
        snapshot['player_stats_checked_at'] = old['player_stats_checked_at']
    return snapshot


def refresh(out, key):
    """Fetch the scoreboard and write one snapshot to `out`; return the snapshot.

    Split out of main() so a long-running polling session can take the same
    path a cron run takes. There must be exactly one implementation of "what a
    published snapshot is": a second one would drift, and the drift would show
    up as a game attributed differently depending on which job happened to
    publish it.
    """
    request = Request(URL, headers={'Authorization': 'Bearer ' + key,
                                   'Accept': 'application/json', 'User-Agent': 'cfb-coefficient-live-scores/1.0'})
    def scoreboard():
        with urlopen(request, timeout=25) as response:
            return json.load(response)
    # The scoreboard is the whole point of the run: without it there is no
    # snapshot to write, and a CFBD reset meant a five-minute hole in the live
    # scores for no better reason than the absence of an answer.
    snapshot = make_snapshot(cfbd_http.with_retries(
        scoreboard, describe='GET /scoreboard', attempts=ATTEMPTS),
        dt.datetime.now(dt.timezone.utc))
    try:
        previous = json.loads(out.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        previous = {}
    now = dt.datetime.now(dt.timezone.utc)
    checked_at = now.isoformat().replace('+00:00', 'Z')
    fresh = None
    due = boxscores_due(previous, now)
    if due:
        try:
            fresh = fetch_player_boxscores(key, snapshot['games'], now)
        except (OSError, ValueError, KeyError) as exc:
            print(f'Player box scores unavailable: {exc}')
    merge_player_boxscores(snapshot, previous, fresh, checked_at)
    print(f"Player box scores: "
          f"{'not due' if not due else len(fresh) if fresh is not None else 'fetch failed'} new/updated, "
          f"{len(snapshot['player_boxscores'])} published; "
          f"{sum(g['status'] == 'completed' for g in snapshot['games'])} completed games")
    out.parent.mkdir(parents=True, exist_ok=True)
    temporary = out.with_suffix(out.suffix + '.tmp')
    temporary.write_text(json.dumps(snapshot, separators=(',', ':'), ensure_ascii=False) + '\n', encoding='utf-8')
    temporary.replace(out)
    print(f"Published {len(snapshot['games'])} scoreboard games at {snapshot['updated_at']}")
    return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    key = os.getenv('CFBD_API_KEY')
    if not key:
        parser.error('CFBD_API_KEY is required (live scoreboard requires a subscribed key)')
    refresh(args.out, key)


if __name__ == '__main__':
    main()
