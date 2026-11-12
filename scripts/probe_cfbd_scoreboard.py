#!/usr/bin/env python3
"""Report the SHAPE of a live CFBD /scoreboard response. No data file, no commit.

WHY THIS EXISTS. Every test of the live scoreboard in this repository compares
against the output of `src/fetch_live_scores.py`, which means none of them can
see a field that file never read -- and it never read seven of the seventeen
fields `/scoreboard` sends, one of which it was actively discarding. The venue
was published as `null` on 238 of 238 live games because `clean_game` accepted
only a string and CFBD sends an object. A fixture written from an assumption
cannot catch that; only a real response can.

So this prints the response's structure -- every key path, its JSON types, how
often it is present, and whether this project reads it -- and nothing else. It
writes no file and commits nothing, so it can run against production with no
risk of overwriting a snapshot, and it deliberately does NOT print a whole
response: the question is the shape, and the data belongs to the provider.

A FEW REAL VALUES ARE PRINTED for the fields whose shape is the question, and
only those: short, non-identifying strings such as a status, a clock, a
possession marker. `--values` widens that and `--no-values` turns it off.

Run it from `.github/workflows/cfbd-scoreboard-probe.yml`; the API is not
reachable from a development container.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from urllib.request import Request, urlopen

URL = 'https://api.collegefootballdata.com/scoreboard?classification=fbs'

# What CFBD's generated client declares (ScoreboardGame, OpenAPI 5.33.2) and
# what this project actually reads. The gap between the two columns is the
# point of the whole exercise.
DECLARED = ['id', 'startDate', 'startTimeTBD', 'tv', 'neutralSite', 'conferenceGame',
            'status', 'period', 'clock', 'situation', 'possession', 'lastPlay',
            'venue', 'homeTeam', 'awayTeam', 'weather', 'betting']
# src/fetch_live_scores.py's clean_game.
PUBLISHER_READS = ['id', 'startDate', 'status', 'period', 'clock', 'tv', 'venue',
                   'neutralSite', 'homeTeam', 'awayTeam']
# live/src/providers/cfbd.mjs's normalizeGame.
ADAPTER_READS = PUBLISHER_READS + ['startTimeTBD', 'conferenceGame', 'situation',
                                   'possession', 'lastPlay']

# Fields whose real values settle a question. Nothing identifying, nothing long.
SHOW_VALUES_FOR = {'status', 'clock', 'period', 'possession', 'situation',
                   'classification', 'conference', 'startTimeTBD', 'conferenceGame'}


def kind(value):
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'bool'
    if isinstance(value, int):
        return 'int'
    if isinstance(value, float):
        return 'float'
    if isinstance(value, str):
        return 'string'
    if isinstance(value, list):
        return 'list'
    if isinstance(value, dict):
        return 'object'
    return type(value).__name__


def walk(value, path, types, values, depth=0):
    """Record the type at `path`, then recurse. Depth-capped, never unbounded."""
    types[path].add(kind(value))
    leaf = path.rsplit('.', 1)[-1].rstrip('[]')
    if isinstance(value, str) and value and leaf in SHOW_VALUES_FOR:
        values[path].add(value[:40])
    elif isinstance(value, bool) and leaf in SHOW_VALUES_FOR:
        values[path].add(str(value))
    elif isinstance(value, int) and not isinstance(value, bool) and leaf in SHOW_VALUES_FOR:
        values[path].add(str(value))
    if depth >= 4:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            walk(item, path + '.' + key if path else key, types, values, depth + 1)
    elif isinstance(value, list):
        # One summary path for the whole list, not one per index.
        for item in value[:20]:
            walk(item, path + '[]', types, values, depth + 1)


def probe(payload, show_values=True):
    lines = []
    if not isinstance(payload, list):
        return ['REFUSED: /scoreboard did not return a list, it returned %s' % kind(payload)]
    lines.append('%d games in the response' % len(payload))
    if not payload:
        lines.append('')
        lines.append('An EMPTY slate says nothing about the shape. Re-run during a slate.')
        return lines

    statuses = Counter(row.get('status') for row in payload if isinstance(row, dict))
    lines.append('statuses: ' + ', '.join('%s=%d' % kv for kv in sorted(statuses.items(),
                                                                       key=lambda kv: str(kv[0]))))
    if 'in_progress' not in statuses:
        lines.append('')
        lines.append('NO GAME IS IN PROGRESS, so period, clock, possession, situation and')
        lines.append('lastPlay are expected to be null here. This run cannot settle their')
        lines.append('shape; re-run while a game is live.')

    types = defaultdict(set)
    values = defaultdict(set)
    present = Counter()
    for row in payload:
        if not isinstance(row, dict):
            continue
        for key in row:
            present[key] += 1
        walk(row, '', types, values)

    rows = sum(1 for row in payload if isinstance(row, dict))

    lines.append('')
    lines.append('TOP-LEVEL FIELDS')
    lines.append('%-16s %-8s %-22s %s' % ('field', 'present', 'types', 'read by'))
    unknown = []
    for key in sorted(set(present) | set(DECLARED)):
        if key not in present:
            lines.append('%-16s %-8s %-22s %s' % (key, '0/%d' % rows, '(absent)', 'declared only'))
            continue
        if key not in DECLARED:
            unknown.append(key)
        who = ('publisher+adapter' if key in PUBLISHER_READS
               else 'adapter only' if key in ADAPTER_READS else 'NOTHING')
        lines.append('%-16s %-8s %-22s %s' % (
            key, '%d/%d' % (present[key], rows),
            ','.join(sorted(types.get(key, {'?'}))),
            who + ('  <-- NOT IN THE DECLARED LIST' if key not in DECLARED else '')))

    lines.append('')
    lines.append('NESTED PATHS')
    for path in sorted(p for p in types if '.' in p or '[' in p):
        lines.append('%-44s %s' % (path, ','.join(sorted(types[path]))))

    if show_values:
        lines.append('')
        lines.append('SAMPLE VALUES (short, non-identifying fields only)')
        for path in sorted(values):
            sample = sorted(values[path])[:8]
            lines.append('%-44s %s' % (path, ', '.join(sample)))

    lines.append('')
    lines.append('WHAT THIS SETTLES')
    venue_types = types.get('venue', set())
    if venue_types:
        lines.append('  venue is %s on the wire. The publisher accepted only a string until'
                     % ','.join(sorted(venue_types)))
        lines.append('  2026-11-12, which is why every live game published a null venue.')
    if unknown:
        lines.append('  UNKNOWN FIELDS, not in CFBD\'s generated client: %s' % ', '.join(unknown))
        lines.append('  Decide for each whether to carry it; do not leave it dropped silently.')
    else:
        lines.append('  No field outside the declared list.')
    ignored = [k for k in present if k not in ADAPTER_READS]
    if ignored:
        lines.append('  Sent but read by nothing: %s' % ', '.join(sorted(ignored)))
    return lines


def fetch(key, timeout=25):
    request = Request(URL, headers={'Authorization': 'Bearer ' + key,
                                    'Accept': 'application/json',
                                    'User-Agent': 'cfb-coefficient-scoreboard-probe/1.0'})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--from-file', help='read a saved response instead of calling the API')
    parser.add_argument('--values', dest='values', action='store_true', default=True)
    parser.add_argument('--no-values', dest='values', action='store_false')
    args = parser.parse_args(argv)

    if args.from_file:
        with open(args.from_file, encoding='utf-8') as handle:
            payload = json.load(handle)
    else:
        key = os.getenv('CFBD_API_KEY')
        if not key:
            parser.error('CFBD_API_KEY is required (the scoreboard needs a subscribed key)')
        payload = fetch(key)

    print('\n'.join(probe(payload, show_values=args.values)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
