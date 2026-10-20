"""Ask the provider what its limits actually are, instead of guessing.

Written after two refresh rehearsals were stopped by HTTP 429 while the
account dashboard reported more than 27,000 calls remaining. Those two facts
cannot both be about a monthly quota, so something short-window is in play and
nothing in this repository knew its shape. The provider's own documentation is
not reachable from the development container, so this asks the API.

Three questions, in order of how much they would change the design:

1. What does the usage endpoint report, for the shared pool and for each sport?
   The football and basketball APIs are understood to share one allowance, so a
   basketball backfill may be spending football's budget -- which would bear
   directly on the rule that a basketball failure must not disturb football.
2. Does a 429 carry `Retry-After`? The retry logic honours it when present and
   falls back to a curve when not; which of those is live decides whether the
   backoff is informed or guessed.
3. What request spacing avoids a 429 at all? A measured interval is worth more
   than any amount of retrying, because it never trips the limit.

It is a diagnostic, not part of the pipeline. It spends a couple of dozen
requests, prints what it learned, and never prints the key.
"""

import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

FOOTBALL = 'https://api.collegefootballdata.com'
BASKETBALL = 'https://api.collegebasketballdata.com'
TIMEOUT = 30
# Cheap, small and cacheable upstream: the point is the status code, not the
# body. A season's team list is one row per team and no date maths.
PROBE_PATH = '/teams'
PROBE_PARAMS = {'season': 2015}


def call(base, path, params=None, *, key):
    """Return (status, headers, body_or_None, seconds). Never raises for HTTP."""
    url = base + path + ('?' + urlencode(params) if params else '')
    request = Request(url, headers={'Authorization': f'Bearer {key}',
                                    'Accept': 'application/json'})
    started = time.monotonic()
    try:
        with urlopen(request, timeout=TIMEOUT) as response:
            body = response.read().decode('utf-8', 'replace')
            return response.status, dict(response.headers), body, \
                time.monotonic() - started
    except HTTPError as exc:
        body = None
        try:
            body = exc.read().decode('utf-8', 'replace')[:400]
        except Exception:
            pass
        return exc.code, dict(exc.headers or {}), body, \
            time.monotonic() - started
    except URLError as exc:
        return None, {}, f'connection failed: {exc.reason}', \
            time.monotonic() - started


def interesting(headers):
    """Only the headers that say something about limits."""
    wanted = ('retry-after', 'x-ratelimit-limit', 'x-ratelimit-remaining',
              'x-ratelimit-reset', 'ratelimit-limit', 'ratelimit-remaining',
              'ratelimit-reset', 'ratelimit-policy', 'x-rate-limit-limit',
              'x-rate-limit-remaining', 'retry-after-ms')
    found = {name: value for name, value in headers.items()
             if name.lower() in wanted}
    return found


def report_usage(key):
    print('## What the usage endpoint reports\n')
    asked = 0
    for base, label in ((FOOTBALL, 'football host'),
                        (BASKETBALL, 'basketball host')):
        for api in (None, 'all', 'cbb', 'cfb'):
            params = {'api': api} if api else None
            status, headers, body, _ = call(base, '/info/usage', params,
                                            key=key)
            asked += 1
            where = f'{label} /info/usage' + (f'?api={api}' if api else '')
            if status == 200:
                print(f'- `{where}` -> {body.strip()[:600]}')
            else:
                print(f'- `{where}` -> HTTP {status}'
                      + (f' {body.strip()[:200]}' if body else ''))
            limits = interesting(headers)
            if limits:
                print(f'    limit headers: {json.dumps(limits)}')
    print(f'\n{asked} requests spent here.\n')
    return asked


def report_burst(key, count):
    """Fire with no pacing at all and find where the limit bites."""
    print(f'## Unpaced burst of {count} to `{PROBE_PATH}`\n')
    print('| # | status | seconds | limit headers |')
    print('| --- | --- | --- | --- |')
    first_429 = None
    for index in range(1, count + 1):
        status, headers, _, took = call(BASKETBALL, PROBE_PATH, PROBE_PARAMS,
                                        key=key)
        limits = interesting(headers)
        print(f'| {index} | {status} | {took:.2f} |'
              f' {json.dumps(limits) if limits else "none"} |')
        if status == 429 and first_429 is None:
            first_429 = index
            if 'Retry-After' not in headers and 'retry-after' not in \
                    {name.lower() for name in headers}:
                print('\n**The 429 carries no `Retry-After`.** Backoff here can'
                      ' only be a guess, so spacing requests matters more than'
                      ' retrying them.\n')
            break
    if first_429 is None:
        print(f'\nNo 429 in {count} unpaced requests.\n')
    else:
        print(f'\nFirst 429 at request {first_429}.\n')
    return first_429


def report_paced(key, interval, count):
    """Does a fixed gap between requests avoid it?"""
    print(f'## {count} requests spaced {interval:.2f}s apart\n')
    codes = []
    for index in range(count):
        if index:
            time.sleep(interval)
        status, headers, _, _ = call(BASKETBALL, PROBE_PATH, PROBE_PARAMS,
                                     key=key)
        codes.append(status)
    ok = sum(1 for code in codes if code == 200)
    print(f'- {ok} of {count} succeeded; statuses {codes}')
    if ok == count:
        print(f'- **{interval:.2f}s between requests was enough.**'
              f' That is {60 / interval:.0f} requests a minute.')
    print()
    return codes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--burst', type=int, default=8,
                        help='How many unpaced requests to try')
    parser.add_argument('--interval', type=float, default=2.0,
                        help='Seconds between requests in the paced run')
    parser.add_argument('--paced', type=int, default=6,
                        help='How many paced requests to try')
    parser.add_argument('--cooldown', type=float, default=90.0,
                        help='Seconds to wait between the two measurements')
    args = parser.parse_args(argv)
    key = os.environ.get('CBBD_API_KEY')
    if not key:
        print('CBBD_API_KEY is not set', file=sys.stderr)
        return 2
    print('# What the provider will actually allow\n')
    spent = report_usage(key)
    first = report_burst(key, args.burst)
    spent += first if first else args.burst
    if args.cooldown > 0:
        print(f'Waiting {args.cooldown:.0f}s before the paced run, so the'
              ' burst above is not what the paced run measures.\n')
        time.sleep(args.cooldown)
    report_paced(key, args.interval, args.paced)
    spent += args.paced
    print(f'---\n\nRoughly {spent} requests spent by this probe.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
