#!/usr/bin/env python3
"""Measure when a fetched scoreboard snapshot first appears on GitHub Pages.

The Pages response is polled with a unique query string. This measures first
observed availability, not GitHub's internal deployment timestamp or CFBD's
own score update time. It makes no CFBD API requests.

WHY THE TIMEOUT IS TEN MINUTES AND NOT TWO. It was 120s, and at that setting
27 of the last 40 sessions reported "Pages visibility unconfirmed" while the
confirmed ones landed at 74-112s -- that is, clustered just under the ceiling.
A measurement whose window sits inside the distribution it is measuring cannot
tell "slow" from "never", so two thirds of the samples carried no information.
See docs/phase1-live-architecture-audit.md. The step runs once at the end of a
job, so the cost of waiting longer is bounded and small against a four-hour
session; the cost of not waiting is a blind spot in the only measurement of
this leg that exists.

The summary line is also emitted as one machine-readable LIVE_DELIVERY record
so a run of sessions can be aggregated into the median/p95 the Phase 1 plan
asks for, without parsing prose. A timed-out observation is reported with
visible_after_s null and waited_s set, because "we stopped looking after N
seconds" is evidence and must not be silently read as a delivery time.
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
    parser.add_argument('--timeout', type=float, default=600)
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

    pushed_after = (pushed - fetched).total_seconds()
    record = {
        'snapshot': expected,
        'pushed_after_s': round(pushed_after, 1),
        'timeout_s': args.timeout,
        'visible_after_s': None,
        'waited_s': None,
        'checks': None,
    }
    try:
        visible, attempts = wait_for_snapshot(expected, read, lambda: dt.datetime.now(dt.timezone.utc),
                                              time.monotonic, time.sleep, args.timeout)
        visible_after = (visible - fetched).total_seconds()
        record['visible_after_s'] = round(visible_after, 1)
        record['checks'] = attempts
        line = (f'CFBD fetched {expected}; push finished about {pushed_after:.0f}s later; '
                f'Pages first showed this snapshot about {visible_after:.0f}s after fetch '
                f'({attempts} check(s)).')
    except TimeoutError as exc:
        record['waited_s'] = args.timeout
        line = (f'CFBD fetched {expected}; push finished about {pushed_after:.0f}s later; '
                f'Pages visibility unconfirmed: {exc}.')
    print(line)
    print('LIVE_DELIVERY ' + json.dumps(record, sort_keys=True))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as summary:
            summary.write('### Live score delivery\n\n' + line + '\n')


if __name__ == '__main__':
    main()
