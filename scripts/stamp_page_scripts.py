#!/usr/bin/env python3
"""Stamp a hand-written page's <script src> with the hash of the file it names.

`ui/dashboard.html` is GENERATED, and `build_dashboard.py` already writes
`stats.js?v=43e2926a3bdc` for every module it inlines a reference to. That is
not decoration. The service worker serves documents network-first and
everything else stale-while-revalidate, so an UNVERSIONED script can be
answered from the cache while the HTML beside it is fresh from the network.
A page that calls a helper the cached module does not have throws a
TypeError, and the reader sees a blank panel until they reload.

`ui/nfl.html` is hand-written, so nothing was stamping it, and its three
modules were unversioned from the day it shipped. This does what
build_dashboard.py does, for a page with no generator:
`sha256(file)[:12]`, the same stamp, which makes the URL change whenever the
bytes do and puts the request on the service worker's `isVersioned`
cache-first path.

`--check` writes nothing and exits non-zero if any stamp is stale, which is
what the test runs: a stamp nobody re-computes is worse than no stamp,
because it reads as a promise that the file has not changed.
"""

import argparse
import hashlib
import re
import sys
from pathlib import Path

# Only pages with no generator of their own. `ui/dashboard.html` is NOT here:
# build_dashboard.py stamps it, and two things writing one file is how they
# disagree.
PAGES = (Path('ui/nfl.html'),)

SCRIPT = re.compile(r'(<script src=")([A-Za-z0-9_.\-]+\.js)(\?v=[0-9a-f]+)?(")')


def stamp(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:12]


def rewrite(page):
    """The page's text with every script stamped, and what changed."""
    text = page.read_text()
    changes = []

    def one(match):
        name = match.group(2)
        target = page.parent / name
        if not target.exists():
            # A script this page names and the tree does not have is a
            # broken page, not a stamping problem; leave it to say so.
            return match.group(0)
        want = '?v=' + stamp(target)
        if match.group(3) != want:
            changes.append((name, match.group(3), want))
        return match.group(1) + name + want + match.group(4)

    return SCRIPT.sub(one, text), changes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true',
                        help='write nothing; exit 1 if a stamp is stale')
    args = parser.parse_args(argv)
    stale = 0
    for page in PAGES:
        text, changes = rewrite(page)
        for name, was, now in changes:
            stale += 1
            print(f'{page}: {name} {was or "(unstamped)"} -> {now}')
        if changes and not args.check:
            page.write_text(text)
    if not stale:
        print('every script stamp is current')
        return 0
    if args.check:
        print('\nRun scripts/stamp_page_scripts.py to bring these up to date.',
              file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
