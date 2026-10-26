"""The 32 franchise marks, from the archive the college side already uses.

**THE OBVIOUS SOURCE IS UNREACHABLE AND THE RIGHT ONE IS NOT.**
`a.espncdn.com` is refused by this container's egress proxy, and nflverse's
own `logos.csv` points at Wikipedia uploads, which is its own licensing
question. `sportsdataverse/sdv-assets` is the archive
`src/fetch_non_fbs_logos.py` has fetched college marks from since 2026-10:
its manifest is on `raw.githubusercontent.com`, which IS reachable, and it
carries 624 NFL team marks.

**TWO VARIANTS, BECAUSE THIS PAGE HAS TWO THEMES.** `ui/nfl.html` is dark by
default with a light override on `prefers-color-scheme`. A white-ink mark
is invisible on the light theme and a dark-primary mark (the Raiders, the
Ravens, the Steelers) is close to invisible on the dark one, so each
franchise gets both: `default` for light, `dark` for dark. All 32 have both
in the manifest; this was measured, not assumed.

**THE JOIN IS AN EXACT NAME MATCH, AND A MISS FAILS THE RUN.** The archive's
`entity_name` is the full club name, which is what `team.name` holds, and
all 32 match exactly. The manifest also lists OAK, SD and STL — the
relocation predecessors, by code rather than name — which this correctly
does not resolve, because a franchise's mark follows the franchise. A team
this cannot place is raised rather than skipped: a bare lookup with
`continue` on a miss is how six conference gaps hid for twelve seasons.

**THIS IS NOT A LICENCE.** The archive grants nothing and claims nothing.
The marks belong to their clubs and are used nominatively, to identify the
club that played the game, exactly as the college side's are.

Usage:
    python -m src.nfl.logos                # fetch and write ui/nfl_logo_assets.json
    python -m src.nfl.logos --dry-run      # resolve and report only, no images
"""

import argparse
import csv
import io
import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / 'ui/nfl_logo_assets.json'
MANIFEST_URL = ('https://raw.githubusercontent.com/sportsdataverse/sdv-assets'
                '/main/manifest/marks.csv')
# `default` is the club's own colours, `dark` is the light-ink cut. The keys
# are what the page asks for, so they name the THEME, not the artwork.
VARIANTS = (('light', 'default'), ('dark', 'dark'))
MARK_SIZE = 256
TIMEOUT = 60


def fetch(url, *, opener=None):
    open_url = opener or urllib.request.urlopen
    with open_url(url, timeout=TIMEOUT) as response:
        return response.read()


def manifest_rows(text):
    """The NFL team logo rows, as the manifest states them."""
    return [row for row in csv.DictReader(io.StringIO(text))
            if row.get('league') == 'nfl' and row.get('level') == 'team'
            and row.get('mark_type') == 'logo']


def resolve(rows, names):
    """{name: {theme: url}} for every name, or raise naming what is missing.

    `names` is the set of franchise names to place. Rows for anything else —
    the OAK, SD and STL predecessors among them — are simply not asked for.
    """
    by_variant = {}
    for row in rows:
        by_variant.setdefault(row['variant'], {})[row['entity_name']] = row
    found, missing = {}, []
    for name in sorted(names):
        marks = {}
        for theme, variant in VARIANTS:
            row = by_variant.get(variant, {}).get(name)
            if row is not None and row.get('archive_url'):
                marks[theme] = row['archive_url']
        if not marks:
            missing.append(name)
            continue
        # A club with no light-ink cut takes its colour mark on both themes,
        # which is dim rather than absent.
        marks.setdefault('dark', marks.get('light'))
        marks.setdefault('light', marks.get('dark'))
        found[name] = marks
    if missing:
        raise LookupError(
            'the mark archive has no logo for: ' + ', '.join(missing))
    return found


def encode(data):
    """A mark as a data URI, resized so 32 of them are not 20 MB."""
    from PIL import Image
    image = Image.open(io.BytesIO(data)).convert('RGBA')
    image.thumbnail((MARK_SIZE, MARK_SIZE), Image.LANCZOS)
    buffer = io.BytesIO()
    image.save(buffer, format='PNG', optimize=True)
    import base64
    return 'data:image/png;base64,' + base64.b64encode(
        buffer.getvalue()).decode('ascii')


def build(teams, *, opener=None, dry_run=False):
    """`teams` maps franchise key -> name. Returns {key: {theme: data URI}}."""
    rows = manifest_rows(fetch(MANIFEST_URL, opener=opener).decode('utf-8'))
    placed = resolve(rows, set(teams.values()))
    if dry_run:
        return {key: {theme: placed[name][theme] for theme in ('light', 'dark')}
                for key, name in teams.items()}
    cache, out = {}, {}
    for key, name in sorted(teams.items()):
        marks = {}
        for theme in ('light', 'dark'):
            url = placed[name][theme]
            if url not in cache:
                cache[url] = encode(fetch(url, opener=opener))
            marks[theme] = cache[url]
        out[key] = marks
    return out


def team_names(connection):
    return {row['team_key']: row['name'] for row in
            connection.execute('SELECT team_key, name FROM team')}


def main(argv=None):
    from .store import connect

    parser = argparse.ArgumentParser(description='Fetch the NFL club marks')
    parser.add_argument('--database', default=None)
    parser.add_argument('--out', default=None)
    parser.add_argument('--dry-run', action='store_true',
                        help='resolve and report; fetch no images')
    args = parser.parse_args(argv)
    teams = team_names(connect(args.database))
    if not teams:
        print('No team is loaded; run the ingestion first', file=sys.stderr)
        return 1
    marks = build(teams, dry_run=args.dry_run)
    print(f'{len(marks)} clubs resolved, both themes')
    if args.dry_run:
        return 0
    path = Path(args.out) if args.out else OUT
    path.write_text(json.dumps(marks, indent=0, sort_keys=True))
    size = path.stat().st_size
    print(f'{path} written, {size / 1_000_000:.1f} MB')
    return 0


if __name__ == '__main__':
    sys.exit(main())
