"""Season-by-season major-college eligibility.

The error this step exists to prevent is reading today's membership back
into 1952 -- and the subtler one of reading SILENCE as membership. So the
tests are about what the script refuses to conclude: unknown stays
unknown, a season decides only itself, and two labels for one season are
a conflict rather than an average.
"""

import csv
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / 'scripts/build_historical_eligibility.py'


def load():
    spec = importlib.util.spec_from_file_location('historical_eligibility',
                                                  SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


eligibility = load()


@pytest.fixture
def archive(tmp_path):
    path = tmp_path / 'historical_staging.db'
    db = sqlite3.connect(path)
    db.executescript("""
    CREATE TABLE archive_games (
        source TEXT NOT NULL, source_game_id TEXT NOT NULL,
        season INTEGER NOT NULL, season_type TEXT, start_date TEXT,
        home_name_raw TEXT, away_name_raw TEXT,
        home_score INTEGER, away_score INTEGER, neutral_site INTEGER,
        home_classification TEXT, away_classification TEXT,
        home_conference TEXT, away_conference TEXT, venue TEXT,
        source_payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
        PRIMARY KEY (source, source_game_id));
    """)
    db.commit()
    db.close()
    return path


def game(path, gid, home, away, season=1950, home_class='fbs',
         away_class='fbs'):
    db = sqlite3.connect(path)
    with db:
        db.execute('INSERT INTO archive_games VALUES '
                   '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   ('cfbd', str(gid), season, 'regular', None, home, away,
                    21, 0, 0, home_class, away_class, None, None, None,
                    '{}', 'x' * 64))
    db.close()


@pytest.fixture
def records(tmp_path):
    path = tmp_path / 'records.db'
    db = sqlite3.connect(path)
    db.executescript("""
    CREATE TABLE archive_team_records (
        source TEXT NOT NULL, season INTEGER NOT NULL, team_raw TEXT NOT NULL,
        team_source_id TEXT, classification TEXT, conference TEXT,
        division TEXT,
        regular_games INTEGER, regular_wins INTEGER, regular_losses INTEGER,
        regular_ties INTEGER, postseason_games INTEGER,
        postseason_wins INTEGER, postseason_losses INTEGER,
        postseason_ties INTEGER, total_games INTEGER, total_wins INTEGER,
        total_losses INTEGER, total_ties INTEGER,
        source_payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
        PRIMARY KEY (source, season, team_raw));
    """)
    db.commit()
    db.close()
    return path


def line(path, team, season=1950, classification='fbs'):
    db = sqlite3.connect(path)
    with db:
        db.execute('INSERT INTO archive_team_records VALUES '
                   '(' + ','.join('?' * 21) + ')',
                   ('cfbd', season, team, '7', classification, '', '',
                    1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '{}', 'y' * 64))
    db.close()


def build(archive_path, records_path, first=1950, last=1950):
    with sqlite3.connect(f'file:{archive_path}?mode=ro', uri=True) as a, \
            sqlite3.connect(f'file:{records_path}?mode=ro', uri=True) as r:
        return {(row['season'], row['team_raw']): row
                for row in eligibility.build(a, r, first, last)}


class TestWhatTheEvidenceDecides:
    def test_a_season_the_provider_called_major_is_major(self, archive,
                                                         records):
        game(archive, 1, 'Michigan', 'Army')
        rows = build(archive, records)
        assert rows[(1950, 'Michigan')]['eligibility'] == 'major_college'

    def test_a_lower_division_label_is_not_major(self, archive, records):
        game(archive, 1, 'Catawba', 'Newberry', home_class='ii',
             away_class='ii')
        rows = build(archive, records)
        assert rows[(1950, 'Catawba')]['eligibility'] == 'lower_division'

    def test_silence_is_unknown_and_never_major(self, archive, records):
        # THE SUBTLE ONE. 165 names in this archive carry no classification
        # at all, and reading that as the top tier would quietly admit every
        # one of them.
        game(archive, 1, 'Quantico Marines', 'Army', home_class=None,
             away_class=None)
        rows = build(archive, records)
        assert rows[(1950, 'Quantico Marines')]['eligibility'] == 'unknown'

    def test_a_record_line_can_settle_a_season_the_games_do_not(self, archive,
                                                                records):
        game(archive, 1, 'Michigan', 'Army', home_class=None, away_class=None)
        line(records, 'Michigan', classification='fbs')
        rows = build(archive, records)
        assert rows[(1950, 'Michigan')]['eligibility'] == 'major_college'

    def test_two_labels_for_one_season_are_a_conflict_not_an_average(
            self, archive, records):
        game(archive, 1, 'Villanova', 'Army', home_class='fbs')
        game(archive, 2, 'Villanova', 'Navy', home_class='ii')
        rows = build(archive, records)
        row = rows[(1950, 'Villanova')]
        assert row['eligibility'] == 'conflict'
        assert row['game_classifications'] == 'fbs;ii'

    def test_the_two_sources_disagreeing_is_also_a_conflict(self, archive,
                                                            records):
        game(archive, 1, 'Villanova', 'Army', home_class='fbs')
        line(records, 'Villanova', classification='fcs')
        assert build(archive, records)[(1950, 'Villanova')]['eligibility'] \
            == 'conflict'


class TestASeasonDecidesOnlyItself:
    def test_membership_in_one_season_does_not_carry_into_another(
            self, archive, records):
        # Membership moved. This is the whole reason the mapping is
        # season-specific rather than a list of schools.
        game(archive, 1, 'Villanova', 'Army', season=1950, home_class='fbs')
        game(archive, 2, 'Villanova', 'Navy', season=1951, home_class='ii')
        rows = build(archive, records, 1950, 1951)
        assert rows[(1950, 'Villanova')]['eligibility'] == 'major_college'
        assert rows[(1951, 'Villanova')]['eligibility'] == 'lower_division'

    def test_each_team_season_is_its_own_row(self, archive, records):
        game(archive, 1, 'Army', 'Navy', season=1950)
        game(archive, 2, 'Army', 'Navy', season=1951)
        rows = build(archive, records, 1950, 1951)
        assert len(rows) == 4


class TestItNeverConsultsThePresentDay:
    def test_the_modern_registry_is_not_read(self):
        source = SCRIPT.read_text(encoding='utf-8')
        code = '\n'.join(line for line in source.splitlines()
                         if not line.lstrip().startswith('#'))
        for forbidden in ('team_identity', 'team_identities',
                          'league.db"', 'build_elo', 'run_pipeline', 'ui/'):
            assert forbidden not in code, forbidden

    def test_a_present_day_power_gets_nothing_for_free(self, archive,
                                                       records):
        # Alabama is in the modern registry and in the top tier today. In a
        # season this archive says nothing about, it is unknown like
        # anybody else.
        game(archive, 1, 'Alabama', 'Quantico Marines', home_class=None,
             away_class=None)
        rows = build(archive, records)
        assert rows[(1950, 'Alabama')]['eligibility'] == 'unknown'


class TestTheSummary:
    def test_it_counts_the_major_teams_of_each_season(self):
        rows = [
            {'season': 1950, 'team_raw': 'Michigan',
             'eligibility': 'major_college', 'game_classifications': 'fbs',
             'record_classification': 'fbs'},
            {'season': 1950, 'team_raw': 'Catawba',
             'eligibility': 'lower_division', 'game_classifications': 'ii',
             'record_classification': ''},
            {'season': 1951, 'team_raw': 'Michigan',
             'eligibility': 'major_college', 'game_classifications': 'fbs',
             'record_classification': 'fbs'},
        ]
        summary = eligibility.summarize(rows, 1950, 1951)
        assert summary['team_seasons'] == 3
        assert summary['by_eligibility'] == {'lower_division': 1,
                                             'major_college': 2}
        assert summary['major_college_by_season'] == {'1950': 1, '1951': 1}
        assert summary['production_promotion_allowed'] is False

    def test_every_conflict_is_listed_with_what_disagreed(self):
        rows = [{'season': 1950, 'team_raw': 'Villanova',
                 'eligibility': 'conflict', 'game_classifications': 'fbs;ii',
                 'record_classification': 'fcs'}]
        conflicts = eligibility.summarize(rows, 1950, 1950)['conflicts']
        assert conflicts == [{'season': 1950, 'team_raw': 'Villanova',
                              'game_classifications': 'fbs;ii',
                              'record_classification': 'fcs'}]

    def test_the_note_says_unknown_is_not_no(self):
        note = eligibility.summarize([], 1946, 1979)['note']
        assert 'Unknown is not no' in note
        assert 'never read as yes' in note


class TestItTouchesNothingItShouldNot:
    def test_the_range_is_phase_three_and_nothing_wider(self, archive,
                                                        records):
        with sqlite3.connect(f'file:{archive}?mode=ro', uri=True) as a, \
                sqlite3.connect(f'file:{records}?mode=ro', uri=True) as r:
            for first, last in ((1945, 1950), (1950, 1980), (1960, 1950)):
                with pytest.raises(ValueError, match='1946-1979'):
                    eligibility.build(a, r, first, last)

    def test_both_databases_are_opened_read_only(self, archive, records,
                                                 tmp_path, monkeypatch):
        game(archive, 1, 'Michigan', 'Army')
        line(records, 'Michigan')
        opened = []
        real = eligibility.sqlite3.connect

        def spy(target, *args, **kwargs):
            connection = real(target, *args, **kwargs)
            opened.append((str(target), connection))
            return connection

        monkeypatch.setattr(eligibility.sqlite3, 'connect', spy)
        monkeypatch.setattr(sys, 'argv', [
            'build_historical_eligibility.py', '--staging', str(archive),
            '--records', str(records), '--out', str(tmp_path / 'report'),
            '--start', '1950', '--end', '1950'])
        eligibility.main()
        for name, table in ((archive.name, 'archive_games'),
                            (records.name, 'archive_team_records')):
            handles = [c for target, c in opened if name in target]
            assert handles, name
            for handle in handles:
                with pytest.raises(sqlite3.OperationalError,
                                   match='readonly|read-only'):
                    handle.execute(f'DELETE FROM {table}')

    def test_it_writes_both_reports(self, archive, records, tmp_path,
                                    monkeypatch):
        game(archive, 1, 'Michigan', 'Catawba', away_class='ii')
        out = tmp_path / 'report'
        monkeypatch.setattr(sys, 'argv', [
            'build_historical_eligibility.py', '--staging', str(archive),
            '--records', str(records), '--out', str(out),
            '--start', '1950', '--end', '1950'])
        eligibility.main()
        rows = list(csv.DictReader(
            (out / 'historical_eligibility.csv').open()))
        assert {row['team_raw'] for row in rows} == {'Michigan', 'Catawba'}
        summary = json.loads(
            (out / 'historical_eligibility_summary.json').read_text())
        assert summary['by_eligibility'] == {'lower_division': 1,
                                             'major_college': 1}

    def test_it_needs_nothing_installed(self):
        source = SCRIPT.read_text(encoding='utf-8')
        for third_party in ('import requests', 'import pandas', 'import yaml',
                            'import numpy'):
            assert third_party not in source, third_party


class TestTheWorkflowStepThatRunsIt:
    WORKFLOW = ROOT / '.github/workflows/historical-identity.yml'

    def test_it_runs_beside_the_identity_map_on_the_same_evidence(self):
        text = self.WORKFLOW.read_text(encoding='utf-8')
        assert 'scripts/build_historical_eligibility.py' in text
        body = text.split('Map season eligibility', 1)[1].split('- name:', 1)[0]
        assert 'set -o pipefail' in body
        for flag in ('--staging', '--records', '--out', '--start', '--end'):
            assert flag in body, flag

    def test_that_workflow_still_needs_no_credential(self):
        text = self.WORKFLOW.read_text(encoding='utf-8')
        code = '\n'.join(line for line in text.splitlines()
                         if not line.lstrip().startswith('#'))
        assert 'secrets.' not in code
