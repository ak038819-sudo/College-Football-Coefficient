#!/usr/bin/env python3
"""Measure when a fetched scoreboard snapshot first appears on GitHub Pages.

The Pages response is polled with a unique query string. This measures first
observed availability, not GitHub's internal deployment timestamp or CFBD's
own score update time. It makes no CFBD API requests.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def parse_stamp(value: str) -> dt.datetime:
    stamp = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('snapshot timestamp must include a timezone')
    return stamp.astimezone(dt.timezone.utc)


def wait_for_snapshot(expected: str, read, now, monotonic, sleep, timeout: float):
    """Return the first observation time and number of attempts, or raise."""
    start = monotonic()
    attempts = 0
    while True:
        attempts += 1
        try:
            observed = read()
            if observed.get('updated_at') == expected:
                return now(), attempts
        except (OSError, ValueError, KeyError) as exc:
            print(f'Pages check {attempts} unavailable: {exc}')
        if monotonic() - start >= timeout:
            raise TimeoutError(f'snapshot {expected} was not visible on Pages within {timeout:g}s')
        sleep(min(5, max(0, timeout - (monotonic() - start))))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True, help='Public Pages URL of live_scores.json')
    parser.add_argument('--snapshot', type=Path, default=Path('ui/data/live_scores.json'))
    parser.add_argument('--timeout', type=float, default=120)
    args = parser.parse_args()
    if args.timeout <= 0 or not args.url.startswith('https://'):
        parser.error('a positive timeout and HTTPS Pages URL are required')
    expected = json.loads(args.snapshot.read_text(encoding='utf-8'))['updated_at']
    fetched = parse_stamp(expected)
    pushed = dt.datetime.now(dt.timezone.utc)

    def read():
        url = args.url + ('&' if '?' in args.url else '?') + urlencode({'snapshot': expected, 'try': time.time_ns()})
        request = Request(url, headers={'Accept': 'application/json', 'Cache-Control': 'no-cache'})
        with urlopen(request, timeout=10) as response:
            return json.load(response)

    try:
        visible, attempts = wait_for_snapshot(expected, read, lambda: dt.datetime.now(dt.timezone.utc),
                                              time.monotonic, time.sleep, args.timeout)
        line = (f'CFBD fetched {expected}; push finished about {(pushed - fetched).total_seconds():.0f}s later; '
                f'Pages first showed this snapshot about {(visible - fetched).total_seconds():.0f}s after fetch '
                f'({attempts} check(s)).')
    except TimeoutError as exc:
        line = (f'CFBD fetched {expected}; push finished about {(pushed - fetched).total_seconds():.0f}s later; '
                f'Pages visibility unconfirmed: {exc}.')
    print(line)
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as summary:
            summary.write('### Live score delivery\n\n' + line + '\n')


if __name__ == '__main__':
    main()
