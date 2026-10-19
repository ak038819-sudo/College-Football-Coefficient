"""Public Merits tables use the existing CoE 2.0 rollups, without changing awards."""
import csv
from pathlib import Path


def build_team_merits(teams, version, data_dir=Path('data/processed')):
    names = {int(t['id']): t['name'] for t in teams}
    path = data_dir / 'team_coe2_by_season.csv'
    out = {}
    if not path.exists():
        return out
    with path.open(newline='') as f:
        for row in csv.DictReader(f):
            # Pre-coverage rollups contain zero-game placeholders, not measured zeros.
            if row['formula_version'] != version or int(row['games_counted']) <= 0:
                continue
            name = names.get(int(row['team_id']))
            if name is None:
                continue
            out.setdefault(row['season_year'], []).append({
                'team': name, 'merits': round(float(row['season_coe2']), 3),
            })
    for rows in out.values():
        rows.sort(key=lambda r: (-r['merits'], r['team']))
    return out
