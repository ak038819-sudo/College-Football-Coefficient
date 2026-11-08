"""The 30 franchise marks, from the archive the other two leagues use.

**THE SOURCE THE ARCHIVE ALREADY NAMES IS UNREACHABLE.** Every `team` row
carries ESPN's own logo URL, fetched with the club, and
`a.espncdn.com` is refused by this container's egress proxy — measured
again while this was written. So the marks come from
`sportsdataverse/sdv-assets`, whose manifest is on
`raw.githubusercontent.com`: the same archive `src/fetch_non_fbs_logos.py`
has fetched college marks from since 2026-10 and `src/nfl/logos.py` since
2026-10-29. It carries 486 NBA rows, 30 clubs in sixteen variants.

**THE JOIN IS ESPN'S OWN ID, NOT THE NAME.** The NFL side matches on the
club name because that is all its archive rows agree on. Here the manifest's
`entity_id` IS the ESPN team id, which is the number inside every team key
(`nba:espn:team:1`), so the join is an identity rather than a string match.
Measured 2026-11-08: all 30 clubs resolve by id, and the name beside each id
agrees with the name the database holds — which this checks rather than
assumes, because an id that has been reused would otherwise publish the
wrong club's mark in silence.

**TWO VARIANTS, BECAUSE THIS PAGE HAS TWO THEMES.** `ui/nba.html` is dark by
default with a light override on `prefers-color-scheme`. A white-ink mark is
invisible on the light theme and a dark-primary mark is close to invisible on
the dark one, so each franchise gets both: `default` for light, `dark` for
dark. All 30 carry both, each 500x500, with no duplicated rows to
disambiguate — also measured, not assumed.

**THIRTY CLUBS COVER TWENTY-FIVE SEASONS.** The archive stores a franchise,
not a club-era: Seattle's rows and Oklahoma City's share one key, as New
Jersey's and Brooklyn's do. So a 2008 game draws today's mark for the club
that played it. That is a statement about the franchise, which is what the
rating is about too, and it is why this needs no dated set like the college
side's.

**THIS IS NOT A LICENCE.** The archive grants nothing and claims nothing. The
marks belong to their clubs and are used nominatively, to identify the club
that played the game, exactly as the other three leagues' are.

Usage:
    python -m src.nba.logos                # fetch and write ui/nba_logo_assets.json
    python -m src.nba.logos --dry-run      # resolve and report only, no images
"""

import argparse
import csv
import io
import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / 'ui/nba_logo_assets.json'
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
    """The NBA team logo rows, as the manifest states them.

    PNG only, as on the other two leagues: Pillow cannot open the handful of
    SVGs the archive carries. Every NBA logo row is a PNG today; this keeps a
    future SVG from failing the run rather than being quietly ignored.
    """
    return [row for row in csv.DictReader(io.StringIO(text))
            if row.get('league') == 'nba' and row.get('level') == 'team'
            and row.get('mark_type') == 'logo' and row.get('ext') == 'png']


def espn_id(team_key):
    """The ESPN team id inside a team key, or None where there is none.

    `nba:espn:team:1` -> `'1'`. A key of another shape returns None and is
    reported by `resolve` as unplaceable, rather than being joined on a
    fragment that happens to be numeric.
    """
    parts = str(team_key).split(':')
    if len(parts) != 4 or parts[0] != 'nba' or parts[1] != 'espn':
        return None
    if parts[2] != 'team' or not parts[3].isdigit():
        return None
    return parts[3]


def resolve(rows, teams):
    """{team key: {theme: url}} for every club, or raise naming what is missing.

    `teams` maps team key -> club name. The name is not what the join is made
    on; it is what the join is CHECKED against, so an id the archive has
    reassigned is a failed run rather than another club's logo.

    THE NEWEST CAPTURE WINS where an id and variant appear twice. No NBA pair
    is doubled today — unlike the NFL's, where three are — but the archive
    keeps one row per set of bytes, so a club that redraws its mark will
    double, and `last_seen` is the only thing that says which one the provider
    is serving now. This is the rule `sdv_marks()` in
    src/fetch_non_fbs_logos.py already follows.
    """
    by_variant = {}
    for row in rows:
        marks = by_variant.setdefault(row['variant'], {})
        current = marks.get(row['entity_id'])
        if current is None or row.get('last_seen', '') > current.get(
                'last_seen', ''):
            marks[row['entity_id']] = row
    found, missing, renamed = {}, [], []
    for key in sorted(teams):
        identifier = espn_id(key)
        marks = {}
        for theme, variant in VARIANTS:
            row = by_variant.get(variant, {}).get(identifier)
            if row is None or not row.get('archive_url'):
                continue
            if row.get('entity_name') != teams[key]:
                renamed.append(
                    f"{key} is {teams[key]} here and "
                    f"{row.get('entity_name')!r} in the archive")
                continue
            marks[theme] = row['archive_url']
        if not marks:
            missing.append(f'{teams[key]} ({key})')
            continue
        # A club with no light-ink cut takes its colour mark on both themes,
        # which is dim rather than absent.
        marks.setdefault('dark', marks.get('light'))
        marks.setdefault('light', marks.get('dark'))
        found[key] = marks
    if renamed:
        raise LookupError('the mark archive disagrees about who a club is: '
                          + '; '.join(sorted(set(renamed))))
    if missing:
        raise LookupError(
            'the mark archive has no logo for: ' + ', '.join(missing))
    return found


def shrink(data):
    """The mark's bytes, resized so 30 of them are not 20 MB."""
    from PIL import Image
    image = Image.open(io.BytesIO(data)).convert('RGBA')
    image.thumbnail((MARK_SIZE, MARK_SIZE), Image.LANCZOS)
    buffer = io.BytesIO()
    image.save(buffer, format='PNG', optimize=True)
    return buffer.getvalue()


def encode(data):
    """Already-shrunk bytes as a data URI."""
    import base64
    return 'data:image/png;base64,' + base64.b64encode(data).decode('ascii')


def dim_themes(dark_bytes, light_bytes):
    """The themes a club's artwork disappears on, as the page names them.

    The same rule, against the same two backgrounds, that the college and NFL
    shells use — see src/logo_contrast.py. Imported inside the function, like
    PIL above, so that merely importing this module does not need an image
    library: the loaders read CSVs from this package.
    """
    from src.logo_contrast import dim_themes as measure
    return measure(dark_bytes, light_bytes)


def build(teams, *, opener=None, dry_run=False):
    """`teams` maps team key -> club name. Returns {key: {theme: data URI}}."""
    rows = manifest_rows(fetch(MANIFEST_URL, opener=opener).decode('utf-8'))
    placed = resolve(rows, teams)
    if dry_run:
        return {key: {theme: placed[key][theme] for theme in ('light', 'dark')}
                for key in placed}
    cache, out = {}, {}
    for key in sorted(placed):
        marks, raw = {}, {}
        for theme in ('light', 'dark'):
            url = placed[key][theme]
            if url not in cache:
                cache[url] = shrink(fetch(url, opener=opener))
            raw[theme] = cache[url]
            marks[theme] = encode(cache[url])
        # Measured on the SHRUNK bytes, which are the ones the page draws.
        dim = dim_themes(raw['dark'], raw['light'])
        if dim:
            marks['dim'] = dim
        out[key] = marks
    return out


def team_names(connection):
    return {row['team_key']: row['name'] for row in
            connection.execute('SELECT team_key, name FROM team')}


def main(argv=None):
    from .store import connect

    parser = argparse.ArgumentParser(description='Fetch the NBA club marks')
    parser.add_argument('--database', default=None)
    parser.add_argument('--out', default=None)
    parser.add_argument('--dry-run', action='store_true',
                        help='resolve and report; fetch no images')
    args = parser.parse_args(argv)
    teams = team_names(connect(args.database))
    if not teams:
        print('No club is loaded; run the ingestion first', file=sys.stderr)
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
    raise SystemExit(main())
