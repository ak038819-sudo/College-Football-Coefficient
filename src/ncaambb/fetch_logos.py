#!/usr/bin/env python3
"""Bank each Division I school's mark beside the basketball export.

The provider sends a school's logo URLs with every team row, filed under the
school's ESPN id and served from its own CDN in a light and a dark cut:

    https://cdn.collegefootballdata.com/cbb-logos/96/2561.png
    https://cdn.collegefootballdata.com/cbb-logos-dark/96/2561.png

`store.logo_id` keeps the id; this fetches the two files and writes them to
`ui/assets/ncaambb/`, named from that id, which is the only path the page
builds. Measured 2026-11-04: 365 of 365 Division I schools carry both cuts.

WHY BANK THEM rather than point the page at that CDN. Hotlinking costs this
repository nothing and would have been one export field, but it makes every
reader's page depend on a third party staying up and serving the right
school, it breaks the installed app offline, and it tells that CDN who is
reading the site. The rest of Elocast self-hosts its marks for the same
reasons, so this does too.

These are not ours and this grants nothing: the marks belong to their schools
and are used nominatively, to identify the club that played the game, exactly
as the football side's are. See docs/non-fbs-logos.md.

A RUN IS IDEMPOTENT and cheap: a file already on disk is left alone unless
--refetch is given, so a second run of 365 schools makes no requests at all.
The CDN is blocked from the development container, so this is meant to run in
CI -- see .github/workflows/cbb-logos.yml.

Usage:
    python -m src.ncaambb.fetch_logos            # fetch what is missing
    python -m src.ncaambb.fetch_logos --refetch  # fetch everything again
    python -m src.ncaambb.fetch_logos --dry-run  # report only, no network
"""
from __future__ import annotations

import argparse
import sys
import urllib.error
import urllib.request
from pathlib import Path

from . import store

REPO = Path(__file__).resolve().parent.parent.parent
ASSETS = REPO / 'ui/assets/ncaambb'
CDN = 'https://cdn.collegefootballdata.com'
# 96px for a mark drawn at 20-28px: enough for a 2x screen with room to
# spare, and a tenth of the 500px cut's weight across 365 schools twice over.
SIZE = 96
CUTS = (('', 'cbb-logos'), ('-dark', 'cbb-logos-dark'))
TIMEOUT = 30


def wanted(connection):
    """Every school with a mark id, as (id, school) ordered by id."""
    # Grouped by the MARK, not by the team row. The id is the school's, and
    # two provider rows for one school (a rename, a duplicate) would
    # otherwise each ask the CDN for the same two files.
    rows = connection.execute(
        'SELECT logo_id, MIN(school) AS school FROM team'
        ' WHERE logo_id IS NOT NULL GROUP BY logo_id'
        ' ORDER BY logo_id').fetchall()
    return [(row['logo_id'], row['school']) for row in rows]


def target(logo, suffix, root=None):
    """Where one cut of one school's mark is written."""
    return (Path(root) if root is not None else ASSETS) / f'{logo}{suffix}.png'


def source(logo, folder, size=SIZE):
    return f'{CDN}/{folder}/{size}/{logo}.png'


def fetch(url, opener=None):
    """The bytes at a URL. Raises on anything but a 200 with a body."""
    get = opener or (lambda target: urllib.request.urlopen(target,
                                                           timeout=TIMEOUT))
    with get(url) as response:
        body = response.read()
    if not body:
        raise ValueError(f'{url} answered with an empty body')
    return body


def run(connection, *, root=None, refetch=False, dry_run=False, opener=None,
        size=SIZE):
    """Fetch every missing cut. Returns a summary of what happened.

    A school whose mark cannot be fetched is REPORTED AND SKIPPED, not fatal:
    the page draws a monogram where a file is missing, so one 404 should not
    cost the other 364 schools their marks. A run that reaches nothing at all
    is a different thing, and the caller treats it as a failure.
    """
    summary = {'schools': 0, 'written': 0, 'skipped': 0, 'failed': []}
    for logo, school in wanted(connection):
        summary['schools'] += 1
        for suffix, folder in CUTS:
            path = target(logo, suffix, root)
            if path.exists() and not refetch:
                summary['skipped'] += 1
                continue
            url = source(logo, folder, size)
            if dry_run:
                summary['written'] += 1
                continue
            try:
                body = fetch(url, opener)
            except (urllib.error.URLError, OSError, ValueError) as error:
                summary['failed'].append(f'{school} ({logo}{suffix}): {error}')
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            summary['written'] += 1
    return summary


def render(summary, *, dry_run=False):
    lines = ['## Basketball club marks', '']
    verb = 'would fetch' if dry_run else 'fetched'
    lines.append(f'- {summary["schools"]} schools carry a mark id')
    lines.append(f'- {verb} {summary["written"]} files, '
                 f'{summary["skipped"]} already banked')
    if summary['failed']:
        lines.append(f'- {len(summary["failed"])} could not be fetched:')
        lines.extend(f'  - {entry}' for entry in summary['failed'][:20])
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--refetch', action='store_true',
                        help='fetch marks already on disk again')
    parser.add_argument('--dry-run', action='store_true',
                        help='report what would be fetched, make no requests')
    parser.add_argument('--db', default=None, help='database path')
    parser.add_argument('--size', type=int, default=SIZE)
    args = parser.parse_args(argv)

    connection = store.connect(args.db)
    try:
        summary = run(connection, refetch=args.refetch, dry_run=args.dry_run,
                      size=args.size)
    finally:
        connection.close()
    print(render(summary, dry_run=args.dry_run))
    if not summary['schools']:
        print('No school carries a mark id; has the database been loaded?')
        return 1
    # Every single school failing is a broken host or a changed URL shape,
    # not 365 individually missing files.
    if summary['failed'] and not summary['written']:
        return 1
    return 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main())
