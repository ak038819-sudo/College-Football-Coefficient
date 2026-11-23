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


# ---- the data describes a roster and rates nothing ---------------------------

# The fit was run on 2026-10-11 and found nothing: returning production did not
# beat the flat 0.8 offseason regression already in the Elo configuration, by a
# held-out margin of 0.002% (docs/returning-production.md). The decision was to
# KEEP it as its own measurement, so it now has a table and a board -- and the
# gate below is what keeps that from drifting into the ratings.
ALLOWED_READERS = {
    "src/fetch_cfbd_returning_production.py",
    "src/calibrate_returning_retention.py",
    # Added when the archive was finally given a table and a page of its own.
    # The rejection said to KEEP it as its own measurement, so these three
    # carry it from the committed CSVs to a board a reader can look at and
    # nowhere else: the loader, the pipeline step that calls the loader, and
    # the exporter that writes the season payloads.
    "src/load_returning_production.py",
    "src/export_static_data.py",
    "run_pipeline.py",
}

# The ratings. A name appearing here that reads returning production would mean
# the feed had become a rating input after it was measured and rejected, which
# is a decision for a person and not a side effect of a refactor.
RATING_MODULES = (
    "src/build_elo.py", "src/calibrate_elo.py", "src/build_coefficients.py",
    "src/build_hybrid_coefficients.py", "src/build_coe2_rollups.py",
    "src/build_conference_coe2.py", "src/build_bowling_merits.py",
    "src/export_merits.py", "src/elo_timeline.py", "src/export_team_pages.py",
    "build_dashboard.py",
)


def test_only_the_named_files_read_returning_production():
    """Every reader is listed above on purpose, with a reason.

    The set is the record of what this feed is allowed to touch. Widening it
    is a deliberate edit; growing into the ratings by accident is what it
    exists to stop.
    """
    repo = Path(__file__).resolve().parents[1]
    candidates = list((repo / "src").rglob("*.py"))
    candidates += [repo / "build_dashboard.py", repo / "run_pipeline.py"]
    readers = []
    for path in candidates:
        if not path.exists():
            continue
        name = path.relative_to(repo).as_posix()
        if name in ALLOWED_READERS:
            continue
        if "returning_production" in path.read_text(encoding="utf-8"):
            readers.append(name)
    assert readers == [], f"returning production is now read by {readers}"


def test_no_rating_reads_it():
    """Named one by one, because the set above could be widened by habit."""
    repo = Path(__file__).resolve().parents[1]
    checked = 0
    for name in RATING_MODULES:
        path = repo / name
        if not path.exists():
            continue
        checked += 1
        assert "returning_production" not in path.read_text(encoding="utf-8"), name
    assert checked >= 8, "the rating modules were renamed; this list stopped checking them"


def test_the_pipeline_only_loads_it_and_never_rates_with_it():
    """run_pipeline.py is the ONE entry point allowed to mention it.

    It calls the loader so a fresh database carries the table, which is what
    makes the board reachable. It must not reach the Elo configuration.
    """
    repo = Path(__file__).resolve().parents[1]
    text = (repo / "run_pipeline.py").read_text(encoding="utf-8")
    assert "load_returning_production" in text, (
        "a fresh database would have no table and the board would be empty")
    # Every mention outside a comment must be that one step. A rating would
    # have to pass the table somewhere, and that shows up as a third line.
    code = [line.strip() for line in text.splitlines()
            if "returning" in line and not line.strip().startswith("#")]
    assert code == ['if list(Path("data/raw").glob("returning_production_*.csv")):',
                    'run([python, "src/load_returning_production.py", "--db", str(DB_PATH)],',
                    '"Load returning production")'], code
