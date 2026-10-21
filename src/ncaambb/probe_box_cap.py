"""Measure the real limit on `/games/players`, rather than assuming it.

The audit found 1,535 Division I games with no box score, spread over every
fetch window in proportion to how busy the window was. That is the signature of
a response truncated below the cap we check for: a window that comes back with
fewer games than the cap is treated as complete, so if the provider stops short
of 1,000 games the shortfall is invisible.

This settles it by measurement. One window is requested whole, then day by day,
and the two totals are compared. A whole-window answer smaller than the sum of
its days is truncation, and the whole-window number is the real limit.

Read-only against the provider. Stores nothing.
"""

import argparse
import sys
from datetime import date, timedelta

from .games import _stamp
from .http import CbbdAuthError, CbbdError, Quota, get_json
from .people import box_score_day


def distinct_games(rows):
    """How many distinct games a response covers.

    The cap on `/games/players` is on rows, so the game count is what makes a
    truncation legible: 1,000 rows is 501 games, with the last one halved.
    """
    return len({row.get('gameId') for row in rows})


def request(path, season, first, last, *, getter=None, **kwargs):
    fetch = getter if getter is not None else get_json
    rows = fetch(path, {'season': season,
                        'startDateRange': _stamp(first, False),
                        'endDateRange': _stamp(last, True)}, **kwargs)
    return rows


def days(first, last):
    span, out = (last - first).days, []
    for offset in range(span + 1):
        out.append(first + timedelta(days=offset))
    return out


def compare(season, first, last, *, path='/games/players', getter=None, **kwargs):
    """One window whole, then day by day. Returns the two measurements."""
    whole = request(path, season, first, last, getter=getter, **kwargs)
    by_day, per_day = set(), []
    for day in days(first, last):
        rows = request(path, season, day, day, getter=getter, **kwargs)
        ids = {row.get('gameId') for row in rows}
        by_day |= ids
        per_day.append((day.isoformat(), len(rows), len(ids)))
    return dict(path=path, window=(first.isoformat(), last.isoformat()),
                whole_rows=len(whole), whole_games=distinct_games(whole),
                day_games=len(by_day), per_day=per_day,
                missing_from_whole=len(by_day
                                       - {row.get('gameId') for row in whole}))


def render(result):
    lines = [f'# `{result["path"]}` limit, {result["window"][0]}'
             f'..{result["window"][1]}', '',
             f'- whole window: {result["whole_rows"]} rows,'
             f' {result["whole_games"]} distinct games',
             f'- day by day: {result["day_games"]} distinct games',
             f'- games the whole-window request did not return:'
             f' {result["missing_from_whole"]}']
    if result['missing_from_whole']:
        lines.append('')
        lines.append(f'**The whole-window response is TRUNCATED at'
                     f' {result["whole_games"]} games.** A window is only safe'
                     ' below that, and the cap this code checks is wrong.')
    else:
        lines.append('')
        lines.append('The whole-window response is complete, so the gap is not'
                     ' truncation at this window size.')
    lines.append('')
    lines.append('| day | rows | games |')
    lines.append('| --- | --- | --- |')
    for day, rows, games in result['per_day']:
        lines.append(f'| {day} | {rows} | {games} |')
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int, required=True)
    parser.add_argument('--first', required=True, help='YYYY-MM-DD')
    parser.add_argument('--last', required=True, help='YYYY-MM-DD')
    parser.add_argument('--path', default='/games/players')
    args = parser.parse_args(argv)
    quota = Quota()
    try:
        result = compare(args.season, date.fromisoformat(args.first),
                         date.fromisoformat(args.last), path=args.path,
                         quota=quota)
    except CbbdAuthError as exc:
        print(f'Cannot reach the provider: {exc}', file=sys.stderr)
        return 2
    except CbbdError as exc:
        print(f'The probe failed: {exc}', file=sys.stderr)
        return 1
    print('\n'.join(render(result)))
    print(f'\nRequests sent: {quota.requests} ({quota.retries} retries)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
