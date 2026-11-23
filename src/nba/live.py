"""A live NBA scoreboard, published beside the archive the nightly refresh builds.

WHY A SECOND SOURCE AT ALL. The archive under `ui/data/nba/` is rebuilt from
sportsdataverse's Parquet release assets, which are republished once a day.
That is the right source for everything a rating is replayed from, and the
wrong source for a game that is being played right now: a reader opening the
NBA tab at nine in the evening sees the night's slate listed with no scores,
because the file that would carry them is published the following morning.

So this module writes a small, separate snapshot of what is happening today,
from ESPN's own public scoreboard. It is DISPLAY ONLY. Not one number here
reaches a rating, a record or an export; `src/nba/build_ratings.py` still
replays the archive and nothing else. Measured from an Actions runner on
2026-11-13: the endpoint answered in 0.05-0.12 s for each of four days.

THE IDS JOIN WITHOUT A CROSSWALK, which is the whole reason this is cheap.
The archive is keyed on ESPN's ids because sportsdataverse's data is
ESPN-derived, so an event id here (401909966) is a `provider_id` there, and a
competitor id (11) is an ESPN team id. Nothing is matched on a name.

WHERE THE FILE GOES, AND WHY NOT BESIDE THE REST. `src/nba/publish.py` swaps
the whole of `ui/data/nba/` for a freshly built directory. Anything written
into it by another job is deleted by the next refresh — which is why
`ui/nba_logo_assets.json` sits outside it too. This snapshot therefore goes to
`ui/data/nba_live.json`.

FOUR TRAPS IN THIS FEED, each with a test:

1. AN UNPLAYED GAME CARRIES A SCORE OF "0", not an absent one, exactly as the
   Parquet archive writes 0-0 for a game not yet played. Tonight's nine games
   would publish as nine scoreless ties. The score is read ONLY where the
   status says the game has started.
2. A MISSING CLOCK IS ABSENT, and `Number()` of absent is 0, which reads as
   0:00 — a game that has not tipped off showing as one about to end. Every
   optional field is tested against None BEFORE it is coerced.
3. AN ALL-STAR GAME IS ON THIS SCOREBOARD like any other, and its sides are
   not franchises. An event whose competitors are not both among the thirty
   is skipped, the same rule `src/nba/identity.py` applies to the archive.
4. THE SCOREBOARD'S DAY IS NOT A UTC DAY. A game tipping off at 10pm Pacific
   is already tomorrow in UTC, so asking for "today" in UTC loses half a
   night's slate. The days asked for are US EASTERN days, which is the day
   ESPN files a game under and the day the archive's `game_date` holds.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from . import identity

SCOREBOARD = ('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/'
              'scoreboard')
DEFAULT_OUT = Path('ui/data/nba_live.json')
USER_AGENT = ('elocast-nba-live/1 '
              '(+https://github.com/ak038819-sudo/College-Football-Coefficient)')
# Three attempts, not the archive fetcher's five: this runs on a short cron
# through the evening, so the backoff has to finish well inside one interval.
# 2s + 4s rides out a reset; a longer outage is the next run's problem, and
# the last good snapshot stays published meanwhile.
ATTEMPTS = 3
BACKOFF_SECONDS = 2.0
TIMEOUT_SECONDS = 30
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
# An ESPN day's scoreboard is a few tens of kilobytes. Two orders of
# magnitude of headroom still refuses a redirect to something that is not one.
MAX_BYTES = 8 * 1024 * 1024
# US Eastern, which is the day ESPN files a game under. Fixed offsets rather
# than a zoneinfo lookup, because the only thing this decides is WHICH DAYS
# to ask for and the window asked for is wider than a day either way: an hour
# of drift at a DST boundary cannot drop a game.
EASTERN_OFFSET = dt.timedelta(hours=-5)
# Yesterday through the day after tomorrow. Yesterday because a game that
# ended after midnight Eastern is still filed under the previous day and a
# reader looking at last night's slate wants its final; the two ahead because
# the page's day strip opens on the next slate when tonight's is over.
DAYS_BACK = 1
DAYS_AHEAD = 2


class NbaLiveError(RuntimeError):
    """The scoreboard could not be read. The published snapshot stays as it is."""


def eastern_today(now=None):
    """Today's date on the NBA's own calendar."""
    moment = now or dt.datetime.now(dt.timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return (moment.astimezone(dt.timezone.utc) + EASTERN_OFFSET).date()


def days_to_ask(now=None, back=DAYS_BACK, ahead=DAYS_AHEAD):
    centre = eastern_today(now)
    return [centre + dt.timedelta(days=offset)
            for offset in range(-back, ahead + 1)]


def fetch_day(day, *, opener=None, sleep=None, attempts=ATTEMPTS):
    """One ESPN day's scoreboard as a decoded payload.

    The status check happens INSIDE the retried call, so a 5xx backs off while
    a 404 fails at once. `sleep` is resolved here rather than as a default
    argument, because a default binds at def time and would silently defeat
    any test that patches it.
    """
    open_url = opener or urllib.request.urlopen
    wait = sleep or time.sleep
    url = f'{SCOREBOARD}?dates={day.strftime("%Y%m%d")}&limit=100'
    request = urllib.request.Request(
        url, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
    last = None
    for attempt in range(1, attempts + 1):
        try:
            with open_url(request, timeout=TIMEOUT_SECONDS) as response:
                status = getattr(response, 'status', 200)
                if status != 200:
                    raise urllib.error.HTTPError(url, status, 'unexpected',
                                                 None, None)
                body = response.read(MAX_BYTES + 1)
        except urllib.error.HTTPError as error:
            last = error
            if error.code not in RETRYABLE_STATUS or attempt == attempts:
                raise NbaLiveError(
                    f'{url} answered {error.code}') from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last = error
            if attempt == attempts:
                raise NbaLiveError(f'{url} could not be reached: {error}') \
                    from error
        else:
            if len(body) > MAX_BYTES:
                raise NbaLiveError(f'{url} returned more than {MAX_BYTES} bytes')
            try:
                return json.loads(body)
            except ValueError as error:
                raise NbaLiveError(f'{url} did not return JSON') from error
        wait(BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise NbaLiveError(f'{url} could not be read: {last}')


def _text(value, limit=120):
    if not isinstance(value, str):
        return None
    trimmed = ' '.join(value.split())
    return trimmed[:limit] or None


def _broadcast(competition):
    """The first network named, or None. Every level of this may be absent."""
    for entry in competition.get('broadcasts') or []:
        for name in (entry or {}).get('names') or []:
            text = _text(name, 24)
            if text:
                return text
    return None


def _started(status):
    """Whether this game's score means anything yet.

    `state` is ESPN's own three-way: `pre` before the tip, `in` while it is
    being played, `post` once it is over. A postponed or cancelled game sits
    in `pre` with a description saying so, and carries 0-0 — which is trap 1.
    """
    state = (status or {}).get('state')
    return state in ('in', 'post')


def _score(side, started):
    """A competitor's points, or None where there is no game yet to have any."""
    if not started:
        return None
    raw = side.get('score')
    # Trap 2: `score` is a STRING here, and an absent one must not become 0.
    if raw is None or raw == '':
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def read_event(event):
    """One scoreboard event as a row, or None where it is not a league game."""
    competition = (event.get('competitions') or [{}])[0]
    status = (event.get('status') or competition.get('status') or {})
    kind = status.get('type') or {}
    started = _started(kind)

    sides = {}
    for side in competition.get('competitors') or []:
        where = side.get('homeAway')
        if where not in ('home', 'away'):
            return None
        try:
            team_id = int(str(side.get('id')).strip())
        except (TypeError, ValueError):
            return None
        # Trap 3: an All-Star side is on this scoreboard like any other.
        if not identity.is_franchise(team_id):
            return None
        team = side.get('team') or {}
        sides[where] = {
            'id': team_id,
            'key': identity.team_key(team_id),
            'abbrev': _text(team.get('abbreviation'), 8),
            'name': _text(team.get('displayName')),
            'points': _score(side, started),
        }
    if 'home' not in sides or 'away' not in sides:
        return None

    provider_id = _text(event.get('id'), 32)
    if not provider_id:
        return None

    period = kind.get('period')
    if period is None:
        period = status.get('period')
    clock = status.get('displayClock')
    if clock is None:
        clock = kind.get('displayClock')

    return {
        'id': provider_id,
        'game_key': identity.game_key(provider_id),
        'state': kind.get('state'),
        'completed': bool(kind.get('completed')),
        'detail': _text(kind.get('shortDetail')) or _text(kind.get('detail')),
        # Trap 2 again: both of these stay absent rather than becoming 0.
        'period': int(period) if isinstance(period, (int, float)) else None,
        'clock': _text(clock, 12) if started else None,
        'tipoff_utc': _text(event.get('date') or competition.get('date'), 32),
        'neutral_site': bool(competition.get('neutralSite')),
        'venue': _text(((competition.get('venue') or {}).get('fullName'))),
        'broadcast': _broadcast(competition),
        'home': sides['home'],
        'away': sides['away'],
    }


def snapshot(days, *, opener=None, sleep=None, now=None):
    """Every league game on those ESPN days, as one publishable object."""
    rows, seen = [], set()
    for day in days:
        payload = fetch_day(day, opener=opener, sleep=sleep)
        for event in payload.get('events') or []:
            row = read_event(event)
            if row is None or row['id'] in seen:
                continue
            seen.add(row['id'])
            row['day'] = day.strftime('%Y-%m-%d')
            rows.append(row)
    rows.sort(key=lambda row: (row['tipoff_utc'] or '', row['id']))
    moment = (now or dt.datetime.now(dt.timezone.utc))
    return {
        'source': 'espn-scoreboard',
        'sport': 'nba',
        'updated_at': moment.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'days': [day.strftime('%Y-%m-%d') for day in days],
        'games': rows,
    }


def write(payload, out=DEFAULT_OUT):
    """Write the snapshot beside the archive, replacing it only once complete.

    Staged and renamed rather than written in place, so a run killed halfway
    leaves the previous snapshot readable instead of half a JSON document that
    every reader's `fetch` then throws on.
    """
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + '.incoming')
    staged.write_text(json.dumps(payload, separators=(',', ':'),
                                 sort_keys=True) + '\n', encoding='utf-8')
    os.replace(staged, path)
    return path


def summarise(payload):
    states = {}
    for row in payload['games']:
        states[row['state']] = states.get(row['state'], 0) + 1
    parts = ', '.join(f'{count} {state}'
                      for state, count in sorted(states.items())) or 'none'
    return (f"# NBA live scoreboard\n\n"
            f"{len(payload['games'])} games over "
            f"{len(payload['days'])} days ({parts}), as of "
            f"{payload['updated_at']}.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', default=str(DEFAULT_OUT),
                        help='where to write the snapshot')
    parser.add_argument('--back', type=int, default=DAYS_BACK,
                        help='how many days before today to ask for')
    parser.add_argument('--ahead', type=int, default=DAYS_AHEAD,
                        help='how many days after today to ask for')
    args = parser.parse_args(argv)
    days = days_to_ask(back=args.back, ahead=args.ahead)
    payload = snapshot(days)
    write(payload, args.out)
    print(summarise(payload))
    return 0


if __name__ == '__main__':
    sys.exit(main())
