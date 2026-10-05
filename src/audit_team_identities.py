"""Offline, reproducible cross-check of supported team identities and exports.

Run after rebuilding exports. Source evidence is checked in, so CI never relies
on a live third-party service. Unknown source schools are inventoried, not guessed.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from team_identity import canonical_name, normalize, registry

ROOT = Path(__file__).resolve().parent.parent


def read_js(path):
    text = path.read_text()
    match = re.search(r'=\s*(\{)', text)
    if not match:
        raise ValueError(f'No JSON payload in {path}')
    return json.JSONDecoder().raw_decode(text[match.start(1):])[0]


def audit(root=ROOT):
    rows, names = registry()
    by_id = {r['team_id']: r for r in rows}
    by_name = {r['canonical_name']: r for r in rows}
    counts, errors, outside = Counter(), [], defaultdict(Counter)

    def check(ok, message):
        if not ok:
            errors.append(message)

    def tid(value, context):
        check(int(value) in by_id, f'{context}: unknown internal team ID {value}')
        counts['internal_id_references'] += 1

    def school(value, context):
        name = canonical_name(value)
        if not name:
            outside[value][context] += 1
        return by_name.get(name, {}).get('team_id')

    directory = json.loads((root / 'data/reference/espn_team_directory.json').read_text())
    provider = {int(r['id']): r for r in directory['teams']}
    for row in rows:
        p = provider.get(row['provider_ids']['espn'])
        check(p is not None and p['displayName'] == row['espn_display_name'],
              f"Provider identity mismatch: {row['canonical_name']}")
        check(p is not None and canonical_name(p['displayName']) == row['canonical_name'],
              f"Provider display name unresolved: {row['canonical_name']}")
    counts['supported_teams'] = len(rows)
    counts['provider_directory_teams'] = len(provider)
    counts['verified_cfbd_scoreboard_ids'] = sum('cfbd_scoreboard' in r['provider_ids'] for r in rows)

    data = json.loads((root / 'ui/dashboard_data.json').read_text())
    check({t['id'] for t in data['teams']} == set(by_id), 'Dashboard/registry team sets differ')
    for t in data['teams']:
        r = by_id.get(t['id'], {})
        check((t['name'], t['slug']) == (r.get('canonical_name'), r.get('slug')),
              f"Dashboard ID/name/route mismatch: {t}")
    search = read_js(root / 'ui/data/search_index.js')
    for id_, name, slug, aliases in search['teams']:
        r = by_id.get(id_, {})
        check((name, slug) == (r.get('canonical_name'), r.get('slug')), f'Search identity mismatch: {name}')
        for alias in aliases:
            check(canonical_name(alias) == name, f'Search alias mismatch: {alias} -> {name}')

    source_games = {}
    for pattern in ['games_[0-9][0-9][0-9][0-9].csv', 'schedule_*.csv']:
        for path in sorted((root / 'data/raw').glob(pattern)):
            for game in csv.DictReader(path.open()):
                home = school(game['home_team'], path.name)
                away = school(game['away_team'], path.name)
                if home and away:
                    source_games[int(game['game_id'])] = (home, away)
    exports = {}
    for path in sorted((root / 'ui/data/games').glob('*.js')):
        payload = read_js(path)
        for values in payload['games']:
            g = dict(zip(payload['fields'], values))
            pair = (g['home_id'], g['away_id'])
            for value in pair:
                tid(value, f'game {g["game_id"]}')
            check(source_games.get(g['game_id']) == pair, f'Game/source identity mismatch: {g["game_id"]}')
            check(pair[0] != pair[1], f'Self-opponent: {g["game_id"]}')
            exports[g['game_id']] = pair
            counts['archived_and_scheduled_games'] += 1
    for game in data['upcoming']['games']:
        check(source_games.get(game[0]) == (game[5], game[6]), f'Prediction identity mismatch: {game[0]}')
        counts['predictions'] += 1
    for key in ['team_ratings_by_year', 'team_rolling_by_year', 'team_elo_by_year']:
        for season in data[key].values():
            for r in season:
                check(r['team'] in by_name, f'Rating identity mismatch: {r["team"]}')
                counts['rating_rows'] += 1

    team_pages = read_js(root / 'ui/data/team_pages.js')
    for game in team_pages['games']:
        check(source_games.get(game[0]) == (game[4], game[5]), f'Team-page game mismatch: {game[0]}')
    for game in team_pages['elo']:
        check(game[1] in source_games.get(game[0], ()), f'Elo/game team mismatch: {game[0]}/{game[1]}')
        counts['elo_game_rows'] += 1
    conf = read_js(root / 'ui/data/conference_pages.js')
    for seasons in conf['members'].values():
        for values in seasons.values():
            for row in values:
                tid(row[0], 'conference standings')
                counts['conference_member_rows'] += 1

    logos = json.loads((root / 'ui/logo_manifest.json').read_text())['teams']
    colors = json.loads((root / 'ui/team_brand_colors.json').read_text())
    check(set(logos) == set(by_name), 'Logo keys differ from supported schools')
    check(set(colors) == set(by_name), 'Brand color keys differ from supported schools')
    for name, entries in logos.items():
        for entry in entries:
            path = entry['src'].split('?')[0]
            check((root / 'ui' / path).is_file(), f'Missing logo asset: {name}/{path}')
            check('/teams/' + by_name[name]['slug'] + '/' in path, f'Logo path belongs to another school: {name}/{path}')
            counts['dated_logo_assets'] += 1

    seeds = {}
    for r in csv.DictReader((root / 'data/stadiums_2026.tsv').open(), delimiter='\t'):
        seeds[school(r['team_name'], 'stadium seed')] = r['stadium_name']
    for stadium in read_js(root / 'ui/data/stadiums.js')['stadiums']:
        for team in stadium['teams']:
            tid(team['id'], 'stadium host')
            check(by_id.get(team['id'], {}).get('canonical_name') == team['name'], f'Stadium host mismatch: {team}')
            check(team['id'] in seeds, f'Missing stadium relationship evidence: {team}')
            counts['stadium_host_links'] += 1
        for game in stadium.get('upcoming', []):
            check(exports.get(game['id']) == (game['home_id'], game['away_id']), f'Stadium game mismatch: {game["id"]}')

    for path in sorted((root / 'ui/data/people').glob('player_*.js')):
        for player in read_js(path).values():
            for season in player['seasons']:
                tid(season['team_id'], 'player season')
                counts['player_team_seasons'] += 1
    for path in sorted((root / 'ui/data/people').glob('roster_*.js')):
        payload = read_js(path)
        source = root / f'data/raw/rosters/{path.stem.split("_")[1]}.json'
        raw = json.loads(source.read_text()) if source.exists() else []
        source_names = {(school(r['team'], source.name), normalize(r['name'])) for r in raw}
        for team, players in payload['teams'].items():
            tid(team, 'roster')
            for player in players:
                check((int(team), normalize(player[1])) in source_names,
                      f'Roster/source identity mismatch: {path.name}/{team}/{player[1]}')
                counts['roster_rows'] += 1
    coaches_source = json.loads((root / 'data/raw/coaches/coaches.json').read_text())
    coach_evidence = {(normalize(c['name']), int(s['year']), school(s['school'], 'coach source'))
                      for c in coaches_source for s in c['seasons']}
    for coach in read_js(root / 'ui/data/people/coaches.js').values():
        for tenure in coach['tenures']:
            tid(tenure['team_id'], 'coach tenure')
            check((normalize(coach['display_name']), tenure['season_year'], tenure['team_id']) in coach_evidence,
                  f'Coach/source school mismatch: {coach["display_name"]}/{tenure}')
            counts['coach_tenures'] += 1

    for game in json.loads((root / 'ui/data/live_scores.json').read_text())['games']:
        for side in [game['home'], game['away']]:
            name = canonical_name(side['name'])
            if not name:
                outside[side['name']]['live scoreboard'] += 1
                check(side.get('classification') != 'fbs', f'Unresolved FBS scoreboard team: {side["name"]}')
            else:
                expected = by_name[name]['provider_ids'].get('cfbd_scoreboard')
                check(expected is None or expected == side['id'], f'CFBD ID/name mismatch: {side}')
                counts['live_team_references'] += 1
    return {'counts': dict(sorted(counts.items())), 'errors': sorted(set(errors)),
            'outside_supported_catalog': {k: dict(v) for k, v in sorted(outside.items())}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path)
    args = parser.parse_args()
    result = audit()
    if args.out:
        args.out.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'counts': result['counts'], 'errors': result['errors'][:30],
                      'error_count': len(result['errors']),
                      'outside_supported_catalog': len(result['outside_supported_catalog'])}, indent=2))
    raise SystemExit(bool(result['errors']))


if __name__ == '__main__':
    main()
