"""Conference audit must not infer membership from missing export values."""
import csv
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "conference_audit", ROOT / "analysis/elo_conference_flow/audit.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def fixture_exports(monkeypatch, conferences, *, completed=True, paired=True):
    fields = ['game_id', 'completed', 'home_id', 'away_id', 'phase', 'week',
              'p_home', 'home_score', 'away_score']
    payload = {'fields': fields, 'conferences': conferences, 'games': [
        [1, True, 1, 2, None, 3, 0.5, 21, 7],
        [2, completed, 3, 4, None, 6, 0.75, 28, 14],
    ]}
    elo = [[1, 1, 0, 0, 0, 10], [1, 2, 0, 0, 0, -10],
           [2, 3, 0, 0, 0, 5]]
    if paired:
        elo.append([2, 4, 0, 0, 0, -5])
    monkeypatch.setattr(audit, 'read_export', lambda path:
                        {'elo': elo} if path.name == 'team_pages.js' else payload)


@pytest.mark.parametrize('membership', [None, '', '  ', 'absent'])
def test_missing_membership_excludes_entire_season(monkeypatch, membership):
    conferences = {'1': 'ACC', '2': 'ACC', '3': 'Sun Belt'}
    if membership != 'absent':
        conferences['4'] = membership
    fixture_exports(monkeypatch, conferences)
    rows, summaries = audit.audit([2026])
    assert rows == []  # even the earlier valid game must not leak into output
    assert summaries == [{'season': 2026, 'status': 'excluded',
                          'reason': 'missing_conference_membership',
                          'missing_team_ids': [4]}]


def test_valid_internal_games_and_independents(monkeypatch):
    fixture_exports(monkeypatch, {'1': 'ACC', '2': 'ACC',
                                 '3': 'FBS Independents', '4': 'FBS Independents'})
    rows, (summary,) = audit.audit([2025])
    pools = {r['conference']: r for r in rows}
    assert pools['ACC']['internal_games'] == 1
    assert pools['ACC']['internal_net'] == 0
    assert pools['FBS Independents / 3']['cross_net'] == 5
    assert pools['FBS Independents / 4']['cross_net'] == -5
    assert summary['internal_brier'] == 0.25
    assert summary['cross_brier'] == 0.0625
    assert summary['max_pair_residual'] == 0


@pytest.mark.parametrize('completed,paired', [(False, True), (True, False)])
def test_unrated_or_uncompleted_games_do_not_require_membership(monkeypatch, completed, paired):
    fixture_exports(monkeypatch, {'1': 'ACC', '2': 'ACC'},
                    completed=completed, paired=paired)
    rows, (summary,) = audit.audit([2025])
    assert len(rows) == 1
    assert summary['rated_games'] == 1
    assert 'status' not in summary


def test_reports_reproduce_frozen_exports(monkeypatch, tmp_path):
    """Exercise report writing without coupling CI to mutable dashboard data."""
    fixture_exports(monkeypatch, {'1': 'ACC', '2': 'ACC',
                                 '3': 'FBS Independents', '4': 'FBS Independents'})
    monkeypatch.setattr(audit, '__file__', str(tmp_path / 'audit.py'))
    audit.main()
    summaries = json.loads((tmp_path / 'season_summary.json').read_text())
    assert [s['season'] for s in summaries] == list(range(2018, 2027))
    for summary in summaries:
        assert summary['rated_games'] == 2
        assert summary['max_pair_residual'] == 0
        assert summary['internal_brier'] == 0.25
        assert summary['cross_brier'] == 0.0625
    with (tmp_path / 'conference_transfers.csv').open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 27
    for season in range(2018, 2027):
        pools = {r['conference']: r for r in rows if r['season'] == str(season)}
        assert set(pools) == {'ACC', 'FBS Independents / 3', 'FBS Independents / 4'}
        assert pools['ACC']['internal_net'] == '0.0'
        assert pools['ACC']['internal_games'] == '1.0'
        assert pools['FBS Independents / 3']['cross_net'] == '5.0'
        assert pools['FBS Independents / 4']['cross_net'] == '-5.0'
    first = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    audit.main()
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == first


def test_live_exports_preserve_audit_invariants():
    """Fresh exports may change totals or make a previously excluded season valid."""
    rows, summaries = audit.audit()
    assert summaries
    excluded = {s['season'] for s in summaries if s.get('status') == 'excluded'}
    assert not any(row['season'] in excluded for row in rows)
    assert not any(row['conference'].startswith('Unknown') for row in rows)
    for summary in summaries:
        if summary.get('status') == 'excluded':
            assert summary['reason'] == 'missing_conference_membership'
            assert summary['missing_team_ids']
            assert 'internal_brier' not in summary
            assert 'cross_brier' not in summary
        else:
            assert summary['max_pair_residual'] == pytest.approx(0, abs=1e-8)
            for key in ('internal_brier', 'cross_brier'):
                assert summary[key] is None or 0 <= summary[key] <= 1
    assert all(row.get('internal_net', 0) == pytest.approx(0, abs=1e-8)
               for row in rows)
