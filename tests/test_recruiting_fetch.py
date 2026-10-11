"""The recruiting fetcher: the choices that would corrupt the data silently.

This file writes the input for coach recruiting records and the pipeline map.
Nothing reads it yet, which is exactly when a defect is cheapest to catch and
hardest to notice: a blank written as 0, a hometown quietly dropped, a class
renumbered into the wrong season. Each test here pins one of those.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import fetch_cfbd_recruiting as rec  # noqa: E402


def team(**over):
    row = {'year': 2024, 'rank': 1, 'team': 'Georgia', 'points': 310.5}
    row.update(over)
    return row


def recruit(**over):
    row = {'year': 2024, 'id': 7, 'athleteId': 4567, 'recruitType': 'HighSchool',
           'name': 'A Player', 'school': 'Central High', 'committedTo': 'Georgia',
           'position': 'DL', 'height': 76, 'weight': 280, 'stars': 5,
           'rating': 0.9912, 'ranking': 3, 'city': 'Macon',
           'stateProvince': 'GA', 'country': 'USA',
           'hometownInfo': {'latitude': 32.84, 'longitude': -83.63, 'fipsCode': '13021'}}
    row.update(over)
    return row


# ---- unknown fields stop the run -------------------------------------------

def test_a_new_team_field_fails_rather_than_being_dropped():
    with pytest.raises(ValueError, match='nilValuation'):
        rec.team_rows([team(nilValuation=400000)], 2024)


def test_a_new_recruit_field_fails_rather_than_being_dropped():
    with pytest.raises(ValueError, match='transferFrom'):
        rec.player_rows([recruit(transferFrom='Alabama')], 2024)


def test_a_new_hometown_field_fails_rather_than_being_dropped():
    """The nested object is the easiest place to lose something, because a
    top-level key check never looks inside it."""
    row = recruit()
    row['hometownInfo'] = dict(row['hometownInfo'], countyName='Bibb')
    with pytest.raises(ValueError, match='countyName'):
        rec.player_rows([row], 2024)


# ---- the season convention ---------------------------------------------------

@pytest.mark.parametrize('rows_fn,row', [(rec.team_rows, team(year=2023)),
                                         (rec.player_rows, recruit(year=2023))])
def test_a_row_labelled_another_season_fails(rows_fn, row):
    """A recruiting class belongs to the season it signs into. Renumbering one
    would attribute a class to the wrong coach."""
    with pytest.raises(ValueError, match='2023'):
        rows_fn([row], 2024)


def test_a_row_with_no_season_of_its_own_is_accepted():
    assert rec.team_rows([team(year=None)], 2024)[0]['season_year'] == 2024


# ---- blanks are blanks -------------------------------------------------------

def test_a_missing_metric_is_blank_and_never_zero():
    """0 points or 0 stars is a real and occasionally true value, so writing 0
    for "not reported" would feed a later fit a measurement nobody made."""
    assert rec.team_rows([team(points=None)], 2024)[0]['points'] == ''
    assert rec.player_rows([recruit(stars=None, rating=None)], 2024)[0]['stars'] == ''


def test_a_recruit_with_no_hometown_object_still_produces_a_row():
    row = rec.player_rows([recruit(hometownInfo=None)], 2024)[0]
    assert row['hometown_latitude'] == ''
    assert row['name'] == 'A Player'


def test_the_hometown_is_flattened_into_named_columns():
    """The coordinates are the reason recruits are fetched at all: a map cannot
    be drawn from a JSON blob every reader has to parse again."""
    row = rec.player_rows([recruit()], 2024)[0]
    assert (row['hometown_latitude'], row['hometown_longitude']) == (32.84, -83.63)
    assert row['hometown_county_fips'] == '13021'


def test_an_uncommitted_recruit_is_kept():
    """Dropping them would quietly change what a class average means for
    everyone else."""
    rows = rec.player_rows([recruit(committedTo=None)], 2024)
    assert len(rows) == 1 and rows[0]['committed_to'] == ''


# ---- integrity ---------------------------------------------------------------

def test_a_duplicate_team_fails():
    with pytest.raises(ValueError, match='duplicate'):
        rec.team_rows([team(), team(rank=2)], 2024)


def test_a_team_row_with_no_team_fails():
    with pytest.raises(ValueError, match='no team'):
        rec.team_rows([team(team=None)], 2024)


def test_every_column_the_header_declares_is_written():
    """A column in the header that no row fills writes a short row, and
    DictWriter would not complain."""
    assert set(rec.team_rows([team()], 2024)[0]) == set(rec.TEAM_CSV)
    assert set(rec.player_rows([recruit()], 2024)[0]) == set(rec.PLAYER_CSV)


# ---- writing -----------------------------------------------------------------

def test_the_output_directory_is_resolved_inside_the_function(tmp_path, monkeypatch):
    """A default argument binds at def time, so `out_dir=OUT_DIR` could not be
    patched and this test would pass while the code wrote into the real
    data/raw. That trap has bitten this repository four times."""
    monkeypatch.setattr(rec, 'OUT_DIR', tmp_path)
    monkeypatch.setattr(rec, 'fetch', lambda path, year, headers:
                        [team()] if path.endswith('teams') else [recruit()])
    written = rec.write_season(2024, {})
    assert {p.name for p in written} == {'recruiting_teams_2024.csv',
                                         'recruiting_players_2024.csv'}
    assert all(p.parent == tmp_path for p in written)
    assert not list(Path('data/raw').glob('recruiting_*_2024.csv'))


def test_a_season_with_no_rows_writes_no_file(tmp_path, monkeypatch):
    """A header-only file reads downstream as "nobody recruited anybody",
    which is the same shape as a real answer."""
    monkeypatch.setattr(rec, 'fetch', lambda path, year, headers: [])
    assert rec.write_season(2001, {}, out_dir=tmp_path) == []
    assert list(tmp_path.iterdir()) == []


def test_one_endpoint_empty_still_writes_the_other(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, 'fetch', lambda path, year, headers:
                        [] if path.endswith('teams') else [recruit()])
    written = rec.write_season(2024, {}, out_dir=tmp_path)
    assert [p.name for p in written] == ['recruiting_players_2024.csv']


def test_the_written_csv_round_trips(tmp_path, monkeypatch):
    import csv
    monkeypatch.setattr(rec, 'fetch', lambda path, year, headers:
                        [team()] if path.endswith('teams') else [recruit()])
    rec.write_season(2024, {}, out_dir=tmp_path)
    rows = list(csv.DictReader((tmp_path / 'recruiting_players_2024.csv')
                               .open(encoding='utf-8')))
    assert rows[0]['hometown_latitude'] == '32.84'
    assert rows[0]['season_year'] == '2024'


# ---- nothing published reads it yet -----------------------------------------

READERS = {'src/fetch_cfbd_recruiting.py'}


def test_no_published_number_depends_on_recruiting_yet():
    """Until a loader lands deliberately, this data must not reach the site.
    A file that quietly starts being read is how an unvalidated input becomes
    a published number without anyone deciding to."""
    root = Path(__file__).resolve().parents[1]
    offenders = []
    for path in list((root / 'src').rglob('*.py')) + [root / 'build_dashboard.py',
                                                      root / 'run_pipeline.py']:
        rel = path.relative_to(root).as_posix()
        if rel in READERS:
            continue
        if 'recruiting_' in path.read_text(encoding='utf-8'):
            offenders.append(rel)
    assert not offenders, (
        f"{offenders} reads the recruiting archive. Add it to READERS only "
        "with the change that deliberately starts using this data.")
