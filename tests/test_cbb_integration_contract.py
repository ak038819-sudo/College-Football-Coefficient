"""Basketball identity and release boundary tests; no network or football DB."""

import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from analysis.cbb_player_impact.contract import entity_key, pilot_export, season_label


def game():
    return dict(id=10, season=2026, homeTeamId=1, awayTeamId=2,
                startDate='2025-11-01T00:00:00Z', status='scheduled',
                neutralSite=False, homePoints=None, awayPoints=None)


def export(games):
    return pilot_export(games, season=2026, team='BYU', retrieved_at='2026-10-20T00:00:00Z')


def test_namespaced_ids_and_ending_year():
    result = export([game()])
    assert result['season_label'] == '2025-26'
    assert result['games'][0]['id'] == 'ncaambb:cbbd:game:10'
    assert result['games'][0]['home_team_id'] == 'ncaambb:cbbd:team:1'
    assert entity_key('player', 1) != entity_key('team', 1)
    assert season_label(2027) == '2026-27'


def test_sample_never_implies_model_or_public_readiness():
    result = export([game()])
    assert result['scope'] == {'kind': 'team_sample', 'team': 'BYU'}
    assert not any(result['readiness'].values())
    assert result['games'][0]['home_score'] is None
    assert json.loads(json.dumps(result)) == result


@pytest.mark.parametrize('change', [dict(season=2025), dict(id=True),
                                  dict(status='complete'), dict(homeTeamId=2),
                                  dict(neutralSite='false'), dict(homePoints=-1),
                                  dict(status='final'), dict(startDate='2026-01-01')])
def test_invalid_provider_records_rejected(change):
    row = game()
    row.update(change)
    with pytest.raises(ValueError):
        export([row])


def test_duplicate_games_rejected():
    with pytest.raises(ValueError, match='Duplicate'):
        export([game(), game()])


def test_direct_cli_and_module_entry_points_work_without_key(tmp_path):
    # Running outside the repo catches imports that depend on working directory.
    result = subprocess.run([sys.executable, str(ROOT / 'analysis/cbb_player_impact/collect.py'), '--help'],
                            cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    result = subprocess.run([sys.executable, '-m', 'analysis.cbb_player_impact.collect', '--help'],
                            cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
