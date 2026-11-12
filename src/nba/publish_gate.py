"""The gate an NBA export must pass before anything may publish it.

Written once and used twice: `src/nba/export.py` runs it before it writes a
file, and `python -m src.nba.publish_gate <dir>` runs it against an export
that already exists, which is what the publishing workflow checks. A check
against a plan is not a check against a disk.

Every check returns a reason a human can act on. The gate never repairs an
export and never downgrades a failure to a warning.

Like the NFL's, this gate can be stricter than the college ones in one way
worth stating: the league is a FIXED SIZE. A season from 2004-05 has exactly
30 franchises and one before it exactly 29, so the team check is an equality
rather than a floor, and an export missing one franchise fails instead of
clearing a floor by a comfortable margin.

Staleness is NOT a gate. An export hours old is still the last good snapshot
and must stay publishable, marked stale, which is what the page does. An
as-of time in the FUTURE is a different thing and is refused, because it
means the clock or the pipeline is wrong.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Schema 4 adds the team statistics: `coverage[].team_stats`, the board's
# PxP+ and opportunity columns, and the per-game grades in the month files.
# Schema 2 adds the game layer: `files.games`, and the month files under
# `games/<season>/`. Schema 3 adds the people layer: `files.people`, and the
# shards under `people/`. Both earlier versions are still SUPPORTED, because
# an export written before a loader ran is a complete export of everything it
# claims to carry — it simply has no game or no player detail, which a page
# learns from the absence of the `files` entry rather than from the version
# number. What a bump buys is that the version says which shape a reader is
# holding.
SCHEMA_VERSION = 4
SUPPORTED_SCHEMA_VERSIONS = (1, 2, 3, 4)

# The Charlotte expansion team arrived in 2004-05, taking the league from 29
# franchises to 30. Measured from the archive, not recalled: every schedule
# file 2002-2004 names 29 franchises and every one from 2005 names 30.
EXPANSION_SEASON = 2005
TEAMS_BEFORE_EXPANSION = 29
TEAMS_AFTER_EXPANSION = 30

# A full season is 1,230 regular-season games plus about 80 of postseason.
# The shortest real seasons in the archive are the 2011-12 lockout (1,074),
# 2019-20 (1,155) and 2020-21 (1,203), so a floor of 1,000 passes every
# complete season and fails anything partial.
MIN_SEASON_GAMES = 1000

# MEASURED. NBA home teams won 55.53% of the 6,605 held-out games, which puts
# the all-home baseline at a Brier of 0.24694. A model that cannot beat "pick
# the home team" is not a rating. The floor is NOT copied from another
# league: the NFL's 0.245 and this are close by coincidence of two sports
# having a similar home edge.
MAX_HOLDOUT_BRIER = 0.246

# The holdout must be more than one season. The committed fit holds out five
# and 6,605 games; 2,000 is comfortably more than one season and far less
# than that.
MIN_HOLDOUT_GAMES = 2000

FUTURE_TOLERANCE = timedelta(minutes=5)


def expected_teams(season):
    """How many franchises a season must carry. Exact, not a floor."""
    return (TEAMS_AFTER_EXPANSION if season >= EXPANSION_SEASON
            else TEAMS_BEFORE_EXPANSION)


def _parse_time(value):
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def check_shape(manifest):
    reasons = []
    if not isinstance(manifest, dict):
        return ['the manifest is not an object']
    version = manifest.get('schema_version')
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        reasons.append(f'schema_version {version!r} is not one this gate '
                       f'understands ({SUPPORTED_SCHEMA_VERSIONS})')
    if manifest.get('sport') != 'nba':
        reasons.append(f'sport is {manifest.get("sport")!r}, not "nba"; this '
                       'gate must never pass another league\'s export')
    for key in ('generated_at', 'model', 'coverage', 'seasons', 'files'):
        if key not in manifest:
            reasons.append(f'the manifest has no {key!r}')
    return reasons


def check_model(manifest):
    """A reader must be able to ask what the model scored before it draws."""
    model = manifest.get('model')
    if not isinstance(model, dict):
        return ['the manifest carries no model section']
    reasons = []
    if not model.get('name'):
        reasons.append('the model has no name')
    config = model.get('config')
    if not isinstance(config, dict) or 'k' not in config:
        reasons.append('the model carries no fitted configuration')
    held = model.get('held_out')
    if not isinstance(held, list) or not held:
        reasons.append('the model names no held-out season')
    fitted = model.get('fitted_on')
    if isinstance(fitted, list) and isinstance(held, list):
        leaked = sorted(set(fitted) & set(held))
        if leaked:
            reasons.append(f'the model both fits on and holds out {leaked}; '
                           'the holdout leaked')
    brier = model.get('holdout_brier')
    if not isinstance(brier, (int, float)) or isinstance(brier, bool) \
            or not 0 < brier < MAX_HOLDOUT_BRIER:
        reasons.append(
            f'the held-out Brier is {brier!r}, which is missing or no better '
            f'than picking the home team (the floor is {MAX_HOLDOUT_BRIER})')
    games = model.get('holdout_games')
    if not isinstance(games, int) or isinstance(games, bool) \
            or games < MIN_HOLDOUT_GAMES:
        reasons.append(f'the model was held out on {games!r} games, below '
                       f'{MIN_HOLDOUT_GAMES}; one NBA season is too narrow '
                       'a measurement to publish a model on')
    return reasons


def check_coverage(manifest):
    coverage = manifest.get('coverage')
    if not isinstance(coverage, list) or not coverage:
        return ['the manifest describes no season']
    reasons = []
    for row in coverage:
        if not isinstance(row, dict):
            reasons.append('a coverage row is not an object')
            continue
        season = row.get('season')
        if not isinstance(season, int):
            reasons.append(f'a coverage row has season {season!r}')
            continue
        teams, wanted = row.get('teams'), expected_teams(season)
        if teams != wanted:
            reasons.append(f'{season} carries {teams!r} franchises, not the '
                           f'{wanted} the league had')
        if row.get('complete'):
            games = row.get('games')
            if not isinstance(games, int) or games < MIN_SEASON_GAMES:
                reasons.append(f'{season} is marked complete with {games!r} '
                               f'games, below {MIN_SEASON_GAMES}')
            rated = row.get('rated_teams')
            if rated != wanted:
                reasons.append(f'{season} is marked complete with {rated!r} '
                               f'rated franchises, not {wanted}')
    return reasons


def check_team_statistics(manifest):
    """The team statistics must be internally consistent, or absent.

    NOT A COVERAGE FLOOR. Which games can be measured is a property of the
    feed — one 2025-26 game is short a turnover count — and a gate that
    demanded a share of them would refuse a correct export because a
    provider dropped a field. Staleness and shortfall are MARKED, in the
    coverage row and on the page; what is refused is arithmetic that cannot
    be true of any archive: more games measured than played, a negative
    count, or a league denominator of zero under a board that divides by it.
    """
    reasons = []
    for row in manifest.get('coverage') or []:
        if not isinstance(row, dict):
            continue
        stats = row.get('team_stats')
        if stats is None:
            continue
        if not isinstance(stats, dict):
            reasons.append(f'{row.get("season")} has a team_stats that is '
                           'not an object')
            continue
        games, measured = stats.get('games'), stats.get('measured')
        for name, value in (('games', games), ('measured', measured),
                            ('unmeasured', stats.get('unmeasured')),
                            ('teams', stats.get('teams'))):
            if not isinstance(value, int) or isinstance(value, bool) \
                    or value < 0:
                reasons.append(f'{row.get("season")} reports team_stats '
                               f'{name} as {value!r}')
        if isinstance(games, int) and isinstance(measured, int) \
                and measured > games:
            reasons.append(f'{row.get("season")} measures team statistics on '
                           f'{measured} games of {games}, which is more '
                           'games than it has')
    return reasons


def check_time(manifest, *, now=None):
    moment = _parse_time(manifest.get('generated_at'))
    if moment is None:
        return [f'generated_at {manifest.get("generated_at")!r} is not a '
                'readable time']
    reference = now or datetime.now(timezone.utc)
    if moment > reference + FUTURE_TOLERANCE:
        return [f'generated_at {manifest["generated_at"]} is in the future; '
                'the clock or the pipeline is wrong']
    return []


def check_files(manifest, root):
    """Every file the manifest names is on disk. Only with a root to look in."""
    if root is None:
        return []
    root = Path(root)
    reasons = []
    named = []
    for url in (manifest.get('seasons') or {}).values():
        named.append(url)
    files = manifest.get('files') or {}
    for value in files.values():
        if isinstance(value, str):
            named.append(value)
    for url in named:
        path = root / str(url).split('?', 1)[0]
        if not path.exists():
            reasons.append(f'the manifest names {url}, which is not on disk')
        elif path.stat().st_size == 0:
            reasons.append(f'{url} is on disk but empty')
    return reasons


def check(manifest, *, root=None, now=None):
    """Every reason this export may not be published. Empty means it may."""
    reasons = check_shape(manifest)
    if reasons:
        return reasons
    return (check_model(manifest) + check_coverage(manifest)
            + check_team_statistics(manifest)
            + check_time(manifest, now=now) + check_files(manifest, root))


def render(reasons):
    if not reasons:
        return 'The export passes the gate.'
    lines = ['The export is REFUSED:']
    lines += [f'  - {reason}' for reason in reasons]
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Check an NBA export against the publish gate')
    parser.add_argument('directory')
    args = parser.parse_args(argv)
    path = Path(args.directory) / 'manifest.json'
    try:
        manifest = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        print(f'Cannot read {path}: {exc}', file=sys.stderr)
        return 1
    reasons = check(manifest, root=Path(args.directory))
    print(render(reasons))
    return 1 if reasons else 0


if __name__ == '__main__':
    sys.exit(main())
