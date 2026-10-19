import csv

from export_merits import build_team_merits


def test_merits_uses_versioned_totals_and_excludes_unmeasured_placeholders(tmp_path):
    with (tmp_path / 'team_coe2_by_season.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['team_id', 'season_year', 'season_coe2', 'games_counted', 'formula_version'])
        writer.writerows([
            [1, 1980, 0, 0, 'current'],
            [1, 2026, 99, 1, 'old'],
            [1, 2026, 3.5, 1, 'current'],
            [2, 2026, 3.5, 1, 'current'],
            [3, 2026, 0, 1, 'current'],
        ])
    teams = [{'id': 1, 'name': 'Zulu'}, {'id': 2, 'name': 'Alpha'}, {'id': 3, 'name': 'Zero'}]
    assert build_team_merits(teams, 'current', tmp_path) == {
        '2026': [{'team': 'Alpha', 'merits': 3.5}, {'team': 'Zulu', 'merits': 3.5}, {'team': 'Zero', 'merits': 0.0}]
    }


def test_missing_merits_never_falls_back_to_legacy(tmp_path):
    assert build_team_merits([], 'current', tmp_path) == {}
