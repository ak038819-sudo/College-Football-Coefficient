"""The gate an NFL export must pass before anything may publish it.

Written once and used twice: `src/nfl/export.py` runs it before it writes a
file, and `python -m src.nfl.publish_gate <dir>` runs it against an export that
already exists, which is what the publishing workflow checks. A check against a
plan is not a check against a disk.

Every check returns a reason a human can act on. The gate never repairs an
export and never downgrades a failure to a warning.

The NFL lets this gate be stricter than the college ones in a way worth
stating: the league is a FIXED SIZE. A season after the 2002 expansion has
exactly 32 teams and a season before it exactly 31, so the team check is an
equality rather than a floor, and an export missing one franchise fails
instead of passing a floor by a comfortable margin.

Staleness is NOT a gate. An export hours old is still the last good snapshot
and must stay publishable, marked stale, which is what `ui/nfl_loader.js`
does. An as-of time in the FUTURE is a different thing and is refused, because
it means the clock or the pipeline is wrong.
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 2 adds the game context the archive always carried (coaches, quarterbacks,
# betting lines, rest days, roof, temperature, wind), a `playoff` section per
# season, per-season coach rows, and a `history` file holding conference
# history, the track record and the coach leaderboard.
#
# 3 adds the people layer: a player index, sharded player careers, the
# in-progress season's player statistics in a file of its own, and the draft.
#
# 1 is still ACCEPTED, not just understood. The published export on main is a
# version 1 export, and this gate runs against it in CI before the refresh
# that would rewrite it: refusing 1 here would fail the publish check on data
# that is perfectly good and leave the site unable to update at all.
# 4 adds the game layer: per-week drives, scoring summaries and the fitted
# win-probability trace, under `games/<season>/<week>.js`.
SCHEMA_VERSION = 5
SUPPORTED_SCHEMA_VERSIONS = (1, 2, 3, 4, 5)

# Houston joined in 2002. Before that the league had 31 teams in this archive.
EXPANSION_SEASON = 2002
TEAMS_BEFORE_EXPANSION = 31
TEAMS_AFTER_EXPANSION = 32

# A finished season is 267 games (1999-2020) or 285 (2021 on, with the
# seventeenth game). 240 passes every real season and fails anything partial.
MIN_SEASON_GAMES = 240

# The home team wins about 55% of NFL games, so a coin flip is a weak thing to
# beat. The floor is the home-base-rate baseline measured on 2021-2025
# (0.24715): a model that cannot beat "pick the home team" is not a rating.
MAX_HOLDOUT_BRIER = 0.245

# The holdout must be more than one season. 285 games is a measurement with a
# wide error bar; the committed fit holds out five seasons and 1,424 games.
MIN_HOLDOUT_GAMES = 500

FUTURE_TOLERANCE = timedelta(minutes=5)

# The people layer's career shards. A literal HERE, not an import of
# people_export.PLAYER_SHARDS: a gate that reads the exporter's constant
# agrees with whatever the exporter did, including a mistake.
PEOPLE_SHARDS = 64


def expected_teams(season):
    """How many franchises a season must carry. Exact, not a floor."""
    return (TEAMS_AFTER_EXPANSION if season >= EXPANSION_SEASON
            else TEAMS_BEFORE_EXPANSION)


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
    """The version stamp a manifest carries for a file: sha256 of its bytes."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:10]


def check_schema(manifest):
    version = manifest.get('schema_version')
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        return [f'schema_version {version!r} is not one this build understands '
                f'({list(SUPPORTED_SCHEMA_VERSIONS)})']
    if manifest.get('sport') != 'nfl':
        return [f'sport {manifest.get("sport")!r} is not nfl; this export must '
                'never be read as college football or basketball data']
    return []


def check_as_of(manifest, *, now=None):
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
    """A published rating must name the model that made it and say what that
    model scored on games it had never seen."""
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
    if not isinstance(brier, (int, float)) or isinstance(brier, bool) \
            or not 0 < brier < MAX_HOLDOUT_BRIER:
        reasons.append(f'holdout Brier {brier!r} is missing or no better than '
                       f'picking the home team ({MAX_HOLDOUT_BRIER})')
    if not isinstance(games, int) or isinstance(games, bool) \
            or games < MIN_HOLDOUT_GAMES:
        reasons.append(f'the holdout score was measured on {games!r} games, '
                       f'below {MIN_HOLDOUT_GAMES}; one NFL season is too '
                       'small a holdout to separate these models')
    return reasons


def check_coverage(manifest):
    """Season by season. A fixed-size league makes this an equality check."""
    coverage = manifest.get('coverage')
    if not isinstance(coverage, list) or not coverage:
        return ['no coverage section; an export with no measured coverage '
                'cannot be called a league export']
    reasons = []
    for season in coverage:
        label = season.get('season')
        teams = season.get('teams')
        rated = season.get('rated_teams')
        games = season.get('games')
        # A season the export did not mark is judged as a finished one: a
        # missing flag must never be the way a thin export slips past.
        complete = season.get('complete', True) is not False
        if not isinstance(label, int):
            reasons.append(f'a coverage row has no season: {season!r}')
            continue
        wanted = expected_teams(label)
        if teams != wanted:
            reasons.append(f'season {label}: {teams!r} teams, and the league '
                           f'had exactly {wanted}; this export is missing a '
                           'franchise or carries one twice')
            continue
        if complete:
            if not isinstance(games, int) or games < MIN_SEASON_GAMES:
                reasons.append(f'season {label}: {games!r} games is below '
                               f'{MIN_SEASON_GAMES}')
            if rated != teams:
                reasons.append(f'season {label}: {rated!r} of {teams} teams '
                               'are rated; a finished season rates every team')
            continue
        # A season in progress: a full season's games cannot be demanded, but
        # every team that has finished a rateable game must carry a rating, so
        # a replay that dropped teams is still caught.
        if not isinstance(games, int) or games < 1:
            reasons.append(f'season {label}: {games!r} games; a season in '
                           'progress with no game at all has nothing to '
                           'publish and must be left out of the export')
            continue
        played = season.get('played_teams')
        if not isinstance(played, int) or played < 0:
            reasons.append(f'season {label}: in progress but does not report '
                           'how many teams have played, so its rated count '
                           'cannot be judged')
        elif rated != played:
            reasons.append(f'season {label}: {rated!r} teams rated but '
                           f'{played} have played a rateable game')
    return reasons


def check_people(manifest):
    """An export that carries the people layer must carry all of it.

    Absent is allowed: a schema 2 export has no people and is still a good
    export. Present-but-partial is not, because a player whose shard is
    missing has an index entry that opens onto nothing, and every player in
    that shard is one of those.
    """
    people = (manifest.get('files') or {}).get('people')
    if people is None:
        return []
    if not isinstance(people, dict):
        return ['files.people is not a section; the people layer cannot be '
                'read']
    reasons = []
    for name in ('index', 'current', 'draft'):
        if not isinstance(people.get(name), str) or not people.get(name):
            reasons.append(f'the people layer names no {name} file')
    careers = people.get('careers')
    if not isinstance(careers, list):
        reasons.append('the people layer lists no career shards')
    else:
        if people.get('shards') != PEOPLE_SHARDS \
                or len(careers) != PEOPLE_SHARDS:
            reasons.append(f'the people layer has {len(careers)} career '
                           f'shards and says {people.get("shards")!r}; it '
                           f'must have exactly {PEOPLE_SHARDS}, or a player '
                           'index entry opens onto a file that is not there')
        for index, path in enumerate(careers):
            if not isinstance(path, str) or not path.startswith(
                    f'people/careers/{index}.js'):
                reasons.append(f'career shard {index} is listed as '
                               f'{path!r}; the shards must be listed in order')
                break
    season = people.get('season')
    if not isinstance(season, int) or isinstance(season, bool):
        reasons.append('the people layer does not say which season its '
                       'current file holds')
    return reasons


# A season's week files are keyed 1, 2, ... and `post`. Written out here
# rather than imported from the exporter, exactly as PEOPLE_SHARDS is: a gate
# that imports the thing it is checking agrees with it by construction.
POSTSEASON_WEEK = 'post'


def week_order(weeks):
    return sorted(weeks, key=lambda week: (week == POSTSEASON_WEEK,
                                           int(week) if week.isdigit() else 0))


def check_games(manifest, root=None):
    """An export that carries the game layer must carry all of it.

    Absent is allowed: a schema 3 export has no game detail and is still a
    good export. Present-but-partial is not. A season states one cache token
    covering all of its week files, so the check is that every week it lists
    is on disk and that the token is the hash of those files in playing
    order — which is the only thing standing between a reader and a cached
    week file from a previous export.
    """
    games = (manifest.get('files') or {}).get('games')
    if games is None:
        return []
    if not isinstance(games, dict) or not isinstance(games.get('seasons'),
                                                     dict):
        return ['files.games is not a section; the game layer cannot be read']
    seasons = games['seasons']
    if not seasons:
        return ['the game layer lists no seasons']
    reasons = []
    for season in sorted(seasons):
        entry = seasons[season]
        if not isinstance(entry, dict):
            reasons.append(f'the game layer\u2019s {season} is not a section')
            continue
        weeks = entry.get('weeks')
        token = entry.get('v')
        if not isinstance(weeks, list) or not weeks \
                or not all(isinstance(week, str) and week for week in weeks):
            reasons.append(f'the game layer names no weeks for {season}')
            continue
        if weeks != week_order(weeks):
            reasons.append(f'the game layer lists {season}\u2019s weeks out '
                           'of playing order, so its cache token cannot be '
                           'reproduced')
            continue
        if not isinstance(token, str) or not token:
            reasons.append(f'the game layer gives {season} no version stamp')
            continue
        if root is None:
            continue
        bodies, missing = [], []
        for week in weeks:
            target = Path(root) / 'games' / season / f'{week}.js'
            if not target.exists():
                missing.append(f'games/{season}/{week}.js')
                continue
            bodies.append(target.read_text(encoding='utf-8'))
        if missing:
            reasons.append('the game layer names files that are not there: '
                           + ', '.join(missing))
            continue
        stamp = hashlib.sha256(
            ''.join(bodies).encode('utf-8')).hexdigest()[:10]
        if stamp != token:
            reasons.append(f'games/{season} hashes to {stamp} but the '
                           f'manifest says {token}')
    return reasons


def manifest_files(manifest):
    """Every path the manifest points at, with its stated version stamp."""
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

    files = dict(manifest.get('files') or {})
    # The game layer states one token per season rather than one path per
    # file, so `check_games` verifies it instead; walking it here would read
    # its week keys as paths.
    files.pop('games', None)
    walk(files)
    walk(manifest.get('seasons'))
    return found


def check_files(manifest, root):
    """Every file the manifest names must exist, be non-empty, and hash to the
    version the manifest states."""
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
        # A manifest of an unknown shape cannot be checked further: every
        # later check would read fields whose meaning is not agreed.
        return reasons
    return (check_as_of(manifest, now=now) + check_model(manifest)
            + check_coverage(manifest) + check_people(manifest)
            + check_games(manifest, root) + check_files(manifest, root))


def render(reasons):
    if not reasons:
        return 'Publish gate PASSED.'
    lines = [f'Publish gate REFUSED this export ({len(reasons)}):']
    lines += [f'- {reason}' for reason in reasons]
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Refuse an NFL export that must not be published')
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
