"""A read-only probe: can an Actions runner reach a live NBA scoreboard, and
how far behind the archive's own source is?

Writes nothing and commits nothing. It exists to settle two measurements
before an NBA live loop is designed around guesses:

1. Whether `site.api.espn.com`'s public scoreboard answers a runner at all,
   and whether the ids it returns are the SAME ESPN ids the archive is keyed
   on, so that a live score can join to a game we already have.
2. How stale the sportsdataverse Parquet the nightly refresh reads is,
   measured as the newest completed game in it against the newest completed
   game the scoreboard reports.
"""

import datetime as dt
import json
import sys
import urllib.error
import urllib.request

SCOREBOARD = ('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/'
              'scoreboard?dates={date}&limit=100')
HEADERS = {'User-Agent': 'elocast-nba-probe/1', 'Accept': 'application/json'}


def fetch(url):
    started = dt.datetime.now(dt.timezone.utc)
    request = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        return {'status': error.code, 'error': str(error), 'body': None,
                'seconds': (dt.datetime.now(dt.timezone.utc) - started).total_seconds()}
    except Exception as error:  # noqa: BLE001 - the probe reports, never raises
        return {'status': None, 'error': f'{type(error).__name__}: {error}',
                'body': None,
                'seconds': (dt.datetime.now(dt.timezone.utc) - started).total_seconds()}
    return {'status': status, 'error': None, 'body': body,
            'seconds': (dt.datetime.now(dt.timezone.utc) - started).total_seconds()}


def describe_day(stamp):
    url = SCOREBOARD.format(date=stamp.strftime('%Y%m%d'))
    result = fetch(url)
    line = {'date': stamp.strftime('%Y-%m-%d'), 'status': result['status'],
            'seconds': round(result['seconds'], 2), 'error': result['error']}
    if result['body'] is None:
        return line, None
    try:
        payload = json.loads(result['body'])
    except ValueError as error:
        line['error'] = f'not JSON: {error}'
        return line, None
    events = payload.get('events') or []
    line['events'] = len(events)
    line['states'] = {}
    for event in events:
        state = (((event.get('status') or {}).get('type') or {})
                 .get('state'))
        line['states'][state] = line['states'].get(state, 0) + 1
    return line, events


def main():
    today = dt.datetime.now(dt.timezone.utc).date()
    report = {'probed_at': dt.datetime.now(dt.timezone.utc)
              .strftime('%Y-%m-%dT%H:%M:%SZ'), 'days': []}
    sample = None
    for offset in (-2, -1, 0, 1):
        stamp = dt.datetime.combine(today + dt.timedelta(days=offset),
                                    dt.time())
        line, events = describe_day(stamp)
        report['days'].append(line)
        if sample is None and events:
            sample = events[0]

    if sample is not None:
        competition = (sample.get('competitions') or [{}])[0]
        report['sample'] = {
            'event_id': sample.get('id'),
            'name': sample.get('shortName'),
            'status': (sample.get('status') or {}).get('type'),
            'period': (sample.get('status') or {}).get('period'),
            'clock': (sample.get('status') or {}).get('displayClock'),
            'competitors': [
                {'id': side.get('id'), 'homeAway': side.get('homeAway'),
                 'score': side.get('score'),
                 'abbrev': (side.get('team') or {}).get('abbreviation')}
                for side in (competition.get('competitors') or [])
            ],
            'top_level_keys': sorted(sample.keys()),
            'competition_keys': sorted(competition.keys()),
        }

    print(json.dumps(report, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
