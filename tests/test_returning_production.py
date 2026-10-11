"""Returning-production ingestion: the guards that keep a silent drop out.

The fit that will consume this data cannot tell "CFBD reported 0% returning"
from "CFBD reported nothing", so the parsing rules here are load-bearing rather
than cosmetic.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import fetch_cfbd_returning_production as rp  # noqa: E402


def entry(team='Georgia', season=2024, **overrides):
    row = {'season': season, 'team': team, 'conference': 'SEC',
           'totalPPA': 120.5, 'totalPassingPPA': 60.1, 'totalReceivingPPA': 40.2,
           'totalRushingPPA': 20.2, 'percentPPA': 0.61, 'percentPassingPPA': 0.55,
           'percentReceivingPPA': 0.58, 'percentRushingPPA': 0.72,
           'usage': 0.63, 'passingUsage': 0.57, 'receivingUsage': 0.6,
           'rushingUsage': 0.71}
    row.update(overrides)
    return row


# ---- fields the endpoint may add --------------------------------------------

def test_an_unknown_field_fails_the_run_rather_than_being_dropped():
    """This repository's recurring fault is the silent drop. A metric CFBD adds
    later must stop the run and name itself, not vanish."""
    with pytest.raises(ValueError, match='no column for'):
        rp.returning_rows([entry(percentDefensivePPA=0.4)], 2024)


def test_unknown_keys_reports_every_new_field_across_rows():
    payload = [entry(somethingNew=1), entry(team='Alabama', anotherNew=2)]
    assert rp.unknown_keys(payload) == ['anotherNew', 'somethingNew']


def test_every_declared_field_has_a_csv_column():
    assert len(rp.CSV_FIELDS) == 3 + len(rp.FIELDS)
    for column, _key in rp.FIELDS:
        assert column in rp.CSV_FIELDS


# ---- the season convention ---------------------------------------------------

def test_a_mislabelled_season_fails_rather_than_being_renumbered():
    """CFBD's year is the season production returns INTO, and that is which
    offseason the number feeds. Quietly accepting a different label would shift
    every team's retention by a year."""
    with pytest.raises(ValueError, match='season 2023'):
        rp.returning_rows([entry(season=2023)], 2024)


def test_a_row_without_a_season_is_accepted_and_stamped():
    """The endpoint is queried one season at a time, so an absent season field
    is not ambiguous."""
    rows = rp.returning_rows([entry(season=None)], 2024)
    assert rows[0]['season_year'] == 2024


def test_the_csv_keeps_cfbds_own_season_numbering():
    rows = rp.returning_rows([entry(season=2024)], 2024)
    assert rows[0]['season_year'] == 2024


# ---- missing values ----------------------------------------------------------

def test_a_metric_cfbd_did_not_report_is_blank_not_zero():
    """0% returning production is a real value. Writing 0 for "not reported"
    would feed the fit a measurement nobody made."""
    rows = rp.returning_rows([entry(percentPPA=None, usage=None)], 2024)
    assert rows[0]['percent_ppa'] == ''
    assert rows[0]['usage'] == ''


def test_a_reported_zero_survives_as_zero():
    rows = rp.returning_rows([entry(percentPPA=0)], 2024)
    assert rows[0]['percent_ppa'] == 0


def test_a_missing_conference_is_blank():
    rows = rp.returning_rows([entry(conference=None)], 2024)
    assert rows[0]['conference'] == ''


# ---- malformed payloads ------------------------------------------------------

def test_a_row_without_a_team_fails():
    with pytest.raises(ValueError, match='no team'):
        rp.returning_rows([entry(team=None)], 2024)


def test_a_non_object_entry_fails():
    with pytest.raises(ValueError, match='not an object'):
        rp.returning_rows(['Georgia'], 2024)


def test_a_duplicated_team_fails():
    """Two rows for one team would double-weight it in any fit."""
    with pytest.raises(ValueError, match='duplicate team'):
        rp.returning_rows([entry(), entry()], 2024)


def test_an_empty_payload_is_not_an_error():
    """A season the endpoint does not cover is a fact about CFBD, not a fault."""
    assert rp.returning_rows([], 2024) == []
    assert rp.returning_rows(None, 2024) == []


# ---- writing -----------------------------------------------------------------

def test_a_season_with_no_rows_writes_no_file(tmp_path, monkeypatch):
    """A header-only file reads downstream as "every team returned nothing",
    which has the same shape as a real answer."""
    monkeypatch.setattr(rp, 'fetch_returning', lambda year, headers: [])
    assert rp.write_returning(2013, {}, out_dir=tmp_path) is None
    assert list(tmp_path.glob('*.csv')) == []


def test_a_season_with_rows_writes_the_declared_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(rp, 'fetch_returning',
                        lambda year, headers: [entry(), entry(team='Alabama')])
    path = rp.write_returning(2024, {}, out_dir=tmp_path)
    assert path == tmp_path / 'returning_production_2024.csv'
    rows = list(csv.DictReader(path.open(encoding='utf-8')))
    assert [r['team'] for r in rows] == ['Georgia', 'Alabama']
    assert list(rows[0]) == rp.CSV_FIELDS


# ---- the CLI -----------------------------------------------------------------

def test_a_season_before_play_by_play_is_refused(monkeypatch, capsys):
    """PPA needs play-by-play, so there is no honest answer for 1995 and
    nothing should be written that implies there is."""
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    assert rp.main(['1995']) == 2
    assert 'cannot predate' in capsys.readouterr().out


def test_a_missing_key_is_refused(monkeypatch):
    monkeypatch.delenv('CFBD_API_KEY', raising=False)
    monkeypatch.delenv('COLLEGEFOOTBALLDATA_API_KEY', raising=False)
    assert rp.main(['2024']) == 2


def test_probe_writes_nothing(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    monkeypatch.setattr(rp, 'fetch_returning', lambda year, headers: [entry()])
    monkeypatch.setattr(rp, 'OUT_DIR', tmp_path)
    assert rp.main(['--probe', '2024']) == 0
    assert list(tmp_path.glob('*.csv')) == []
    assert 'keys:' in capsys.readouterr().out


def test_a_range_fetches_every_season_in_it(monkeypatch, tmp_path):
    asked = []
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    monkeypatch.setattr(rp, 'fetch_returning',
                        lambda year, headers: asked.append(year) or [entry(season=year)])
    monkeypatch.setattr(rp, 'OUT_DIR', tmp_path)
    assert rp.main(['2014', '2016']) == 0
    assert asked == [2014, 2015, 2016]


def test_one_season_without_an_end_year(monkeypatch, tmp_path):
    asked = []
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    monkeypatch.setattr(rp, 'fetch_returning',
                        lambda year, headers: asked.append(year) or [entry(season=year)])
    monkeypatch.setattr(rp, 'OUT_DIR', tmp_path)
    assert rp.main(['2024']) == 0
    assert asked == [2024]


# ---- the data stays inert until a fit says otherwise -------------------------

def test_nothing_in_the_pipeline_reads_returning_production_yet():
    """Landing the data must change no published number. When a fit does start
    reading it, this test is the place to record that deliberately."""
    repo = Path(__file__).resolve().parents[1]
    readers = []
    for path in list((repo / 'src').rglob('*.py')) + [repo / 'build_dashboard.py']:
        if path.name == 'fetch_cfbd_returning_production.py' or not path.exists():
            continue
        if 'returning_production' in path.read_text(encoding='utf-8'):
            readers.append(path.relative_to(repo).as_posix())
    assert readers == [], f'returning production is now read by {readers}'


# ---- the backfill workflow ---------------------------------------------------

def _workflow():
    import yaml
    path = (Path(__file__).resolve().parents[1] / '.github' / 'workflows'
            / 'backfill-returning-production.yml')
    data = yaml.safe_load(path.read_text(encoding='utf-8'))
    return data, data.get('on', data.get(True))  # PyYAML reads the bare key `on` as True


def test_the_backfill_is_dispatch_only():
    """It walks every season at once and pushes to main; nothing should start
    it on a timer."""
    _, on = _workflow()
    assert set(on) == {'workflow_dispatch'}


def test_the_backfill_offers_a_probe_that_writes_nothing():
    data, on = _workflow()
    assert 'probe' in on['workflow_dispatch']['inputs']
    steps = data['jobs']['backfill']['steps']
    probing = [s for s in steps if 'inputs.probe' in str(s.get('if', ''))]
    assert len(probing) >= 2, 'the probe and the fetch must be mutually exclusive steps'
    writes = [s for s in steps if 'publish_to_main.sh' in str(s.get('run', ''))]
    assert len(writes) == 1
    assert '!inputs.probe' in str(writes[0]['if'])


def test_the_backfill_shares_the_lock_that_guards_data_raw():
    """The scheduled refresh rewrites data/raw too, and both push to main."""
    data, _ = _workflow()
    concurrency = data['jobs']['backfill']['concurrency']
    assert concurrency['group'] == 'rebuild-and-deploy-main'
    assert concurrency['cancel-in-progress'] is False


def test_every_season_is_checked_explicitly_in_the_loop():
    """`set -e` does not stop a shell loop, so a failed season has to be
    checked and halt the run rather than be committed over."""
    path = (Path(__file__).resolve().parents[1] / '.github' / 'workflows'
            / 'backfill-returning-production.yml')
    body = path.read_text(encoding='utf-8')
    assert 'if ! python3 src/fetch_cfbd_returning_production.py' in body
    assert 'if ! scripts/publish_to_main.sh' in body


def test_the_key_reaches_only_the_steps_that_call_cfbd():
    data, _ = _workflow()
    steps = data['jobs']['backfill']['steps']
    with_key = [s for s in steps if 'CFBD_API_KEY' in (s.get('env') or {})]
    assert len(with_key) == 2, 'the probe step and the fetch step, and nothing else'
    for step in with_key:
        assert 'fetch_cfbd_returning_production.py' in step['run']
