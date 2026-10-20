"""The gate a basketball export must pass before anything may publish it.

The plan's rule is that a BYU sample must never be published as national
coverage, and that a production loader must refuse a team sample or an
unsupported schema rather than show it as national rankings. This is that
refusal, written once and used twice: `src/ncaambb/export.py` runs it before it
writes a file, and `python -m src.ncaambb.publish_gate <dir>` runs it against an
export that already exists, which is what a publishing workflow checks.

Every check returns a reason a human can act on. The gate never repairs an
export and never downgrades a failure to a warning: an export either describes
national coverage of a model that was scored on unseen games, or it does not
publish.

The staleness of an export is NOT a gate. An export whose as-of time is hours
old is still the last good snapshot and must be publishable, marked stale, which
is what `ui/ncaambb_loader.js` does. An as-of time in the future is a different
thing and is refused, because it means the clock or the pipeline is wrong.
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# The export contract's version, not the database's. A loader states which
# versions it understands and refuses the rest; bumping this is a deliberate
# break that requires the loader to be taught the new shape first.
SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = (1,)

# Division I measured 351 teams in 2015 and 365 in 2026. A floor of 300 passes
# every real season and fails any sample: one team, one conference, or one
# tournament field would all be far below it.
MIN_DIVISION_ONE_TEAMS = 300

# A season may be published when nearly every Division I team has a rating.
# Below that the board would silently omit teams a reader expects to find.
MIN_RATED_FRACTION = 0.9

# A season's games, as a floor. A real season is about 6,000.
MIN_SEASON_GAMES = 2000

# How far ahead of the stated as-of time a clock may plausibly be.
FUTURE_TOLERANCE = timedelta(minutes=5)


def _parse_time(value):
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def file_version(path):
    """The version stamp a manifest carries for a file: sha256 of its bytes.

    The same ten characters football's manifest uses, so a reviewer reads one
    convention across both sports.
    """
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:10]


def check_schema(manifest):
    version = manifest.get('schema_version')
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        return [f'schema_version {version!r} is not one this build understands '
                f'({list(SUPPORTED_SCHEMA_VERSIONS)})']
    if manifest.get('sport') != 'ncaambb':
        return [f'sport {manifest.get("sport")!r} is not ncaambb; this export '
                'must never be read as football data']
    return []


def check_as_of(manifest, *, now=None):
    """The as-of time must exist, parse, and not be in the future."""
    moment = _parse_time(manifest.get('generated_at'))
    if moment is None:
        return ['generated_at is missing or not an ISO 8601 time; a reader '
                'cannot tell whether a rating is current']
    current = now or datetime.now(timezone.utc)
    if moment > current + FUTURE_TOLERANCE:
        return [f'generated_at {manifest["generated_at"]} is in the future; '
                'the clock or the pipeline is wrong']
    return []


def check_model(manifest):
    """A published rating must name the model that made it and what that model
    scored on games it had never seen."""
    model = manifest.get('model')
    if not isinstance(model, dict):
        return ['no model section; a rating cannot be published without '
                'saying which model made it']
    reasons = []
    if not model.get('name'):
        reasons.append('the model has no name')
    if not model.get('config_sha256'):
        reasons.append('the model carries no configuration hash, so two '
                       'exports claiming one model cannot be compared')
    fitted = list(model.get('fitted_on') or [])
    held = list(model.get('held_out') or [])
    if not held:
        reasons.append('the model was never scored on a held-out season')
    if set(fitted) & set(held):
        reasons.append(f'seasons {sorted(set(fitted) & set(held))} are both '
                       'fitted on and held out; the holdout leaked')
    brier, games = model.get('holdout_brier'), model.get('holdout_games')
    if not isinstance(brier, (int, float)) or not 0 < brier < 0.25:
        reasons.append(f'holdout Brier {brier!r} is missing or no better than '
                       'a coin flip (0.25)')
    if not isinstance(games, int) or games < MIN_SEASON_GAMES:
        reasons.append(f'the holdout score was measured on {games!r} games, '
                       f'which is below {MIN_SEASON_GAMES}; a sample-sized '
                       'holdout is not validation')
    return reasons


def check_coverage(manifest):
    """National coverage, season by season. This is the check that fails a
    single-team pilot, which is the whole point of the gate."""
    coverage = manifest.get('coverage')
    if not isinstance(coverage, list) or not coverage:
        return ['no coverage section; an export with no measured coverage '
                'cannot be called national']
    reasons = []
    for season in coverage:
        label = season.get('season')
        teams = season.get('division_one_teams')
        rated = season.get('rated_teams')
        games = season.get('games')
        if not isinstance(teams, int) or teams < MIN_DIVISION_ONE_TEAMS:
            reasons.append(f'season {label}: {teams!r} Division I teams is '
                           f'below {MIN_DIVISION_ONE_TEAMS}; this is a sample, '
                           'not a national export')
            continue
        if not isinstance(games, int) or games < MIN_SEASON_GAMES:
            reasons.append(f'season {label}: {games!r} games is below '
                           f'{MIN_SEASON_GAMES}')
        if not isinstance(rated, int) or rated < teams * MIN_RATED_FRACTION:
            reasons.append(f'season {label}: {rated!r} of {teams} Division I '
                           f'teams are rated, below {MIN_RATED_FRACTION:.0%}')
    return reasons


def manifest_files(manifest):
    """Every path the manifest points at, with its stated version stamp.

    Flattened from the nested entries so one check covers the whole manifest
    rather than the shapes a reviewer happens to remember.
    """
    found = []

    def walk(value):
        if isinstance(value, str):
            path, _, query = value.partition('?v=')
            if path.endswith('.js') or path.endswith('.json'):
                found.append((path, query or None))
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(manifest.get('files'))
    walk(manifest.get('seasons'))
    return found


def check_files(manifest, root):
    """Every file the manifest names must exist, be non-empty, and hash to the
    version the manifest states.

    A manifest pointing at a file that is missing, truncated or stale is how a
    deploy serves last week's ratings under this week's as-of time.
    """
    if root is None:
        return []
    root = Path(root)
    named = manifest_files(manifest)
    if not named:
        return ['the manifest names no data file']
    reasons = []
    for path, version in named:
        target = root / path
        if not target.exists():
            reasons.append(f'{path} is named by the manifest but not on disk')
            continue
        if target.stat().st_size == 0:
            reasons.append(f'{path} is empty; an empty file must never replace '
                           'a valid one')
            continue
        if version is None:
            reasons.append(f'{path} carries no ?v= version stamp')
        elif file_version(target) != version:
            reasons.append(f'{path} hashes to {file_version(target)} but the '
                           f'manifest says {version}')
    return reasons


def check(manifest, *, root=None, now=None):
    """Every reason this export must not publish. Empty means it may."""
    reasons = check_schema(manifest)
    if reasons:
        # A manifest of an unknown shape cannot be checked further: every later
        # check would be reading fields whose meaning is not agreed.
        return reasons
    return (check_as_of(manifest, now=now) + check_model(manifest)
            + check_coverage(manifest) + check_files(manifest, root))


def render(reasons):
    if not reasons:
        return 'Publish gate PASSED.'
    lines = [f'Publish gate REFUSED this export ({len(reasons)}):']
    lines += [f'- {reason}' for reason in reasons]
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Refuse a basketball export that must not be published')
    parser.add_argument('directory', help='the export directory holding '
                                          'manifest.json')
    parser.add_argument('--manifest', default='manifest.json')
    args = parser.parse_args(argv)
    root = Path(args.directory)
    path = root / args.manifest
    if not path.exists():
        print(f'Publish gate REFUSED this export: {path} does not exist')
        return 1
    manifest = json.loads(path.read_text(encoding='utf-8'))
    reasons = check(manifest, root=root)
    print(render(reasons))
    return 1 if reasons else 0


if __name__ == '__main__':
    sys.exit(main())
