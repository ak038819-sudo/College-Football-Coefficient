"""Offline synthetic contract tests; these do not establish real-data coverage."""

import copy
import json
from pathlib import Path
import sys

import pytest

# The CI console entry point does not add the repository root like python -m
# pytest does. Resolve from this file so both invocations import the prototype.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.cbb_player_impact.collect import audit_lineups, collect
from analysis.cbb_player_impact.elo import Parameters, replay


def game(id=1, season=2025):
    return dict(id=id, season=season, startDate=f'{season}-01-01T12:00:00Z',
                homeTeamId=1, awayTeamId=2, homePoints=70, awayPoints=60,
                homePeriodPoints=[35, 35], awayPeriodPoints=[30, 30],
                neutralSite=True, status='final')


def lineups(g):
    return [dict(teamId=team, athletes=[{'id': team*10+i} for i in range(5)],
                 totalSeconds=2400, teamStats={'points': points, 'possessions': 65},
                 opponentStats={'points': against, 'possessions': 65})
            for team, points, against in [(1, g['homePoints'], g['awayPoints']),
                                          (2, g['awayPoints'], g['homePoints'])]]


def test_elo_chronology_conservation_and_future_independence():
    first, second = game(), game(2)
    second['startDate'] = '2025-01-02T12:00:00Z'
    before, _ = replay([first])
    history, ratings = replay([second, first])
    assert history[0] == before[0]
    assert history[1]['home_pre'] == 1510
    assert sum(ratings.values()) == pytest.approx(3000)
    assert history[1]['home_probability'] > .5


def test_neutral_court_and_offseason_regression():
    p = Parameters(home=100, retention=.5)
    first, next_year = game(), game(2, 2026)
    history, _ = replay([first, next_year], p)
    assert history[0]['home_probability'] == .5
    assert history[1]['home_pre'] == 1505
    first['neutralSite'] = False
    assert replay([first], p)[0][0]['home_probability'] > .5


def test_scheduled_games_do_not_change_ratings():
    scheduled = game(2)
    scheduled.update(status='scheduled', homePoints=None, awayPoints=None)
    assert replay([game(), scheduled]) == replay([game()])


@pytest.mark.parametrize('mutation', [dict(homePoints=None), dict(homePoints=60),
                                      dict(neutralSite='false'), dict(homeTeamId=2)])
def test_reject_invalid_final(mutation):
    g = game()
    g.update(mutation)
    with pytest.raises(ValueError):
        replay([g])


def test_reject_duplicate_game():
    with pytest.raises(ValueError, match='Duplicate'):
        replay([game(), game()])


def test_structural_pass_is_not_player_model_readiness():
    g = game()
    result = audit_lineups(g, lineups(g))
    assert result['structural_checks_passed']
    assert not result['player_model_ready']


@pytest.mark.parametrize('problem', ['missing_team', 'duplicate_player', 'score', 'duration', 'duplicate_lineup'])
def test_audit_catches_corrupt_lineups(problem):
    g = game()
    rows = lineups(g)
    if problem == 'missing_team':
        rows.pop()
    elif problem == 'duplicate_player':
        rows[0]['athletes'][1] = rows[0]['athletes'][0]
    elif problem == 'score':
        rows[0]['teamStats']['points'] -= 1
    elif problem == 'duration':
        rows[0]['totalSeconds'] -= 20
    else:
        rows.append(copy.deepcopy(rows[0]))
    assert not audit_lineups(g, rows)['structural_checks_passed']


def test_overtime_duration():
    g = game()
    g['homePeriodPoints'] = [30, 30, 10]
    g['awayPeriodPoints'] = [30, 30, 0]
    rows = lineups(g)
    assert not audit_lineups(g, rows)['structural_checks_passed']
    for row in rows:
        row['totalSeconds'] = 2700
    assert audit_lineups(g, rows)['structural_checks_passed']


def test_collection_manifest_and_failure_boundary(tmp_path):
    g = game()
    calls = []

    def getter(path, params, key):
        calls.append(path)
        if path == '/games':
            return [g]
        return lineups(g) if path.startswith('/lineups') else []

    output = tmp_path / 'success'
    result = collect(2025, 'BYU', 1, output, 'secret-not-to-persist', getter)
    assert len(calls) == 4
    assert len(result['files']) == 5
    assert 'secret-not-to-persist' not in (output / 'manifest.json').read_text()
    with pytest.raises(ValueError, match='already exists'):
        collect(2025, 'BYU', 1, output, 'unused', getter)

    def failing(path, params, key):
        if path == '/games':
            return [g]
        raise RuntimeError('Access unavailable')

    output = tmp_path / 'failed'
    with pytest.raises(RuntimeError):
        collect(2025, 'BYU', 1, output, 'unused', failing)
    assert json.loads((output / 'manifest.json').read_text())['status'].startswith('incomplete')


def test_provider_cap_is_not_silent_truncation(tmp_path):
    def getter(*args):
        return [game(i) for i in range(3000)]
    with pytest.raises(ValueError, match='cap'):
        collect(2025, 'BYU', 1, tmp_path / 'capped', 'unused', getter)
