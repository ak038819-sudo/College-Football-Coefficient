#!/usr/bin/env python3
"""Report real archive coverage; absent source statistics are never zeroes."""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def audit(conn, player_dir):
    advanced = defaultdict(set)
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='game_team_advanced'").fetchone():
        for gid, tid in conn.execute('SELECT game_id, team_id FROM game_team_advanced '
                                     'WHERE off_success_rate IS NOT NULL'):
            advanced[gid].add(tid)
    stadium_col = 'stadium_id' in {row[1] for row in conn.execute('PRAGMA table_info(games)')}
    games = defaultdict(list)
    for gid, year, home, away, venue in conn.execute(
            'SELECT game_id, season_year, home_team_id, away_team_id, '
            + ('stadium_id' if stadium_col else 'NULL') + ' FROM games'):
        games[year].append((gid, home, away, venue))
    out = {}
    for year, season_games in sorted(games.items()):
        source = player_dir / f'{year}.json'
        players = json.loads(source.read_text()) if source.exists() else {}
        ids = {str(g[0]) for g in season_games}
        out[str(year)] = {
            'completed_fbs_games': len(season_games),
            'both_team_success_rates': sum({home, away} <= advanced[gid] for gid, home, away, _ in season_games),
            'verified_venue_id': sum(venue is not None for _, _, _, venue in season_games),
            'player_boxscores': sum(bool(lines) for gid, lines in players.items() if gid in ids),
            'player_source_supported': year >= 2004,
        }
    return {'seasons': out, 'note': 'Coverage counts are source availability, not zero-valued statistics.'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db', default=str(ROOT / 'db/league.db'))
    p.add_argument('--out', type=Path, default=ROOT / 'data/processed/game_coverage.json')
    args = p.parse_args()
    with sqlite3.connect(args.db) as conn:
        report = audit(conn, ROOT / 'data/raw/player_boxscores')
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')
    current = report['seasons'][max(report['seasons'], key=int)]
    print(f'Current season coverage: {current}')


if __name__ == '__main__':
    main()
