"""Bounded CBBD pilot collector and structural lineup audit (standard library)."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

if __package__:
    from .contract import pilot_export
else:
    from contract import pilot_export

BASE = 'https://api.collegebasketballdata.com'


def fetch(path, params, key):
    url = BASE + path + ('?' + urlencode(params) if params else '')
    request = Request(url, headers={'Authorization': f'Bearer {key}',
                                    'Accept': 'application/json'})
    try:
        with urlopen(request, timeout=45) as response:
            data = json.load(response)
    except HTTPError as exc:
        # Never log request headers, secrets, or provider error bodies.
        raise RuntimeError(f'CBBD HTTP {exc.code} at {path}; check access/quota') from None
    except URLError:
        raise RuntimeError(f'Cannot reach CBBD at {path}') from None
    if not isinstance(data, list):
        raise ValueError(f'Expected an array from {path}')
    return data


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + '\n'
    temporary = path.with_suffix('.tmp')
    temporary.write_text(payload, encoding='utf-8')
    temporary.replace(path)
    return hashlib.sha256(payload.encode()).hexdigest()


def audit_lineups(game, lineups):
    """Necessary structural checks only; aggregates cannot prove stint validity."""
    issues, team_summaries, seen = [], {}, set()
    expected_teams = {game['homeTeamId'], game['awayTeamId']}
    periods = game.get('homePeriodPoints')
    away_periods = game.get('awayPeriodPoints')
    duration = None
    if isinstance(periods, list) and isinstance(away_periods, list) and len(periods) == len(away_periods) and len(periods) >= 2:
        duration = 2400 + 300 * (len(periods) - 2)
    else:
        issues.append('Cannot verify regulation/overtime duration from period scores')
    for row in lineups:
        team = row.get('teamId')
        if team not in expected_teams:
            issues.append('Unexpected team in lineup response')
            continue
        ids = [a.get('id') for a in row.get('athletes', [])]
        if len(ids) != 5 or None in ids or len(set(ids)) != 5:
            issues.append(f'{team}: lineup does not have five distinct player IDs')
            continue
        identity = (team, tuple(sorted(ids)))
        if identity in seen:
            issues.append(f'{team}: duplicate lineup')
        seen.add(identity)
        seconds = row.get('totalSeconds')
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or not math.isfinite(seconds) or seconds <= 0:
            issues.append(f'{team}: invalid lineup seconds')
            continue
        summary = team_summaries.setdefault(team, {'seconds': 0, 'points': 0, 'points_against': 0, 'lineups': 0})
        summary['seconds'] += seconds
        summary['lineups'] += 1
        for field, target in [('teamStats', 'points'), ('opponentStats', 'points_against')]:
            stats = row.get(field, {})
            points, possessions = stats.get('points'), stats.get('possessions')
            if type(points) is not int or points < 0:
                issues.append(f'{team}: invalid {field} points')
            else:
                summary[target] += points
            if not isinstance(possessions, (float, int)) or isinstance(possessions, bool) or not math.isfinite(possessions) or possessions < 0:
                issues.append(f'{team}: invalid {field} possessions')
    for team in expected_teams:
        summary = team_summaries.get(team)
        if summary is None:
            issues.append(f'{team}: missing usable lineups')
            continue
        if duration is not None and abs(summary['seconds'] - duration) > 1:
            issues.append(f'{team}: lineup seconds do not cover game duration')
        home = team == game['homeTeamId']
        if summary['points'] != game['homePoints' if home else 'awayPoints'] or summary['points_against'] != game['awayPoints' if home else 'homePoints']:
            issues.append(f'{team}: lineup points do not reconcile to final score')
    return dict(game_id=game['id'], structural_checks_passed=not issues,
                player_model_ready=False, issues=issues, teams=team_summaries,
                next_gate='Reconstruct paired ten-player stints and reconcile possessions/minutes')


def collect(season, team, limit, output, key, getter=fetch):
    # Separate directory per run prevents stale files looking like current results.
    if output.exists():
        raise ValueError('Output already exists; choose a new directory for this snapshot')
    games = getter('/games', {'season': season, 'team': team}, key)
    if len(games) >= 3000:
        raise ValueError('Response reached provider cap; split request before proceeding')
    if not games:
        raise ValueError('No games returned; verify season and provider team name')
    if len({g['id'] for g in games}) != len(games) or any(g['season'] != season for g in games):
        raise ValueError('Duplicate games or unexpected season')
    completed = sorted((g for g in games if g['status'] == 'final'), key=lambda g: (g['startDate'], g['id']))
    if not completed:
        raise ValueError('No completed games available for pilot')
    manifest = dict(provider=BASE, retrieved_at=datetime.now(timezone.utc).isoformat(),
                    season=season, team=team, scope='team sample; not national rankings',
                    schedule_games=len(games), completed_games=len(completed),
                    sampled_games=min(limit, len(completed)), files={}, status='collecting')
    # Fail before writing if the schedule cannot satisfy the sport-specific
    # contract. Never publish this team sample as national rankings.
    integration = pilot_export(games, season=season, team=team,
                               retrieved_at=manifest['retrieved_at'])
    manifest['files']['games.json'] = write_json(output / 'games.json', games)
    write_json(output / 'manifest.json', manifest)
    audits = []
    try:
        manifest['files']['integration.json'] = write_json(output / 'integration.json', integration)
        for game in completed[:limit]:
            for family in ('lineups', 'substitutions', 'plays'):
                route = f'/{family}/game/{game["id"]}'
                rows = getter(route, {}, key)
                name = f'{game["id"]}/{family}.json'
                manifest['files'][name] = write_json(output / name, rows)
                if family == 'lineups':
                    audits.append(audit_lineups(game, rows))
        manifest['files']['audit.json'] = write_json(output / 'audit.json', audits)
        manifest['status'] = 'collected; player-model validation pending'
    except Exception:
        manifest['status'] = 'incomplete; do not use for model fitting'
        raise
    finally:
        write_json(output / 'manifest.json', manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int, default=2026, help='Ending year; 2026 means 2025-26')
    parser.add_argument('--team', default='BYU')
    parser.add_argument('--limit', type=int, default=3, help='Earliest completed games, 1-20')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.limit <= 20 or args.season < 2024:
        parser.error('Require limit 1-20 and season >= 2024 for lineup pilot')
    key = os.environ.get('CBBD_API_KEY')
    if not key:
        parser.error('Set CBBD_API_KEY securely in the environment; do not commit it')
    try:
        result = collect(args.season, args.team, args.limit, args.out, key)
    except (RuntimeError, ValueError, KeyError, OSError) as exc:
        parser.exit(1, f'{exc}\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
