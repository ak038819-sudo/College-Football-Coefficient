"""The candidate identity map for the 1946-1979 schools.

The danger in this step is not a crash, it is a quiet merge: two schools
that look alike becoming one, or a defunct program being resolved into the
modern team that shares its name. So the assertions are mostly about what
the script REFUSES to decide, and about a suggestion staying a suggestion.
"""

import csv
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / 'scripts/build_historical_identity_map.py'


def load():
    spec = importlib.util.spec_from_file_location('historical_identity', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


identity = load()


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
         away_class=None, home_conf='Big Ten', away_conf=None):
    db = sqlite3.connect(path)
    with db:
        db.execute('INSERT INTO archive_games VALUES '
                   '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   ('cfbd', str(gid), season, 'regular', None, home, away,
                    21, 0, 0, home_class, away_class, home_conf, away_conf,
                    None, '{}', 'x' * 64))
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


def line(path, team, season=1950, team_id='7', classification='fbs'):
    db = sqlite3.connect(path)
    with db:
        db.execute(
            'INSERT INTO archive_team_records VALUES '
            '(' + ','.join('?' * 21) + ')',
            ('cfbd', season, team, team_id, classification, 'Big Ten', '',
             1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '{}', 'y' * 64))
    db.close()


def build(archive_path, records_path, first=1950, last=1950):
    with sqlite3.connect(f'file:{archive_path}?mode=ro', uri=True) as a, \
            sqlite3.connect(f'file:{records_path}?mode=ro', uri=True) as r:
        return {row['raw_name']: row
                for row in identity.build(a, r, first, last)}


class TestWhatItSortsWhere:
    def test_a_name_the_records_feed_carries_is_matched_to_its_id(
            self, archive, records):
        game(archive, 1, 'Michigan', 'Army')
        line(records, 'Michigan', team_id='130')
        rows = build(archive, records)
        assert rows['Michigan']['status'] == 'records_matched'
        assert rows['Michigan']['canonical_name'] == 'Michigan'
        assert rows['Michigan']['cfbd_team_ids'] == '130'

    def test_a_name_only_our_registry_knows_is_matched_to_the_registry(
            self, archive, records):
        # Alabama is in the reviewed registry; the records feed here is not
        # carrying it.
        game(archive, 1, 'Alabama', 'Quantico Marines')
        rows = build(archive, records)
        assert rows['Alabama']['status'] == 'registry_matched'
        assert rows['Alabama']['canonical_name'] == 'Alabama'

    def test_a_name_nobody_carries_is_left_for_a_person(self, archive,
                                                        records):
        game(archive, 1, 'Alabama', 'Quantico Marines')
        rows = build(archive, records)
        assert rows['Quantico Marines']['status'] == 'unresolved'
        assert rows['Quantico Marines']['canonical_name'] == ''

    def test_the_records_feed_wins_over_the_registry(self, archive, records):
        # Its id is the stronger evidence, and the one promotion will need.
        game(archive, 1, 'Alabama', 'Army')
        line(records, 'Alabama', team_id='333')
        rows = build(archive, records)
        assert rows['Alabama']['status'] == 'records_matched'
        assert rows['Alabama']['cfbd_team_ids'] == '333'


class TestTheEvidenceItGathers:
    def test_seasons_games_and_sides_are_counted(self, archive, records):
        game(archive, 1, 'Army', 'Navy', season=1950)
        game(archive, 2, 'Navy', 'Army', season=1951)
        game(archive, 3, 'Army', 'Yale', season=1951)
        rows = build(archive, records, 1950, 1951)
        army = rows['Army']
        assert army['first_season'] == 1950
        assert army['last_season'] == 1951
        assert army['seasons'] == 2
        assert army['games'] == 3
        assert army['as_home'] == 2
        assert army['as_away'] == 1
        assert army['distinct_opponents'] == 2

    def test_the_classifications_a_name_appears_under_are_kept_all_of_them(
            self, archive, records):
        # A school that reads as one division in one season and another in
        # the next is exactly what the eligibility gate needs to see.
        game(archive, 1, 'Villanova', 'Army', season=1950, home_class='fbs')
        game(archive, 2, 'Villanova', 'Navy', season=1951, home_class='fcs')
        rows = build(archive, records, 1950, 1951)
        assert rows['Villanova']['classifications'] == 'fbs;fcs'

    def test_a_name_in_no_season_of_the_range_is_absent(self, archive,
                                                        records):
        game(archive, 1, 'Army', 'Navy', season=1950)
        game(archive, 2, 'Rice', 'Baylor', season=1960)
        rows = build(archive, records, 1950, 1950)
        assert 'Rice' not in rows


class TestASuggestionIsNeverAResolution:
    def test_a_near_miss_is_offered_as_a_lead_only(self, archive, records):
        game(archive, 1, 'Alabam', 'Army')
        rows = build(archive, records)
        row = rows['Alabam']
        assert row['status'] == 'unresolved'
        assert row['canonical_name'] == ''
        assert row['suggestion'] == 'Alabama'
        assert row['suggestion_ratio'] > identity.SUGGESTION_FLOOR

    def test_a_name_like_nothing_known_gets_no_suggestion(self, archive,
                                                          records):
        game(archive, 1, 'Quantico Marines', 'Army')
        rows = build(archive, records)
        assert rows['Quantico Marines']['suggestion'] == ''

    def test_a_resolved_name_is_not_given_a_suggestion_to_second_guess_it(
            self, archive, records):
        game(archive, 1, 'Michigan', 'Army')
        line(records, 'Michigan')
        assert build(archive, records)['Michigan']['suggestion'] == ''

    def test_two_schools_that_merely_look_alike_stay_two_schools(
            self, archive, records):
        # The whole risk of this step in one test.
        game(archive, 1, 'Trinity (Texas)', 'Trinity (Connecticut)')
        rows = build(archive, records)
        assert len(rows) == 2
        assert all(row['status'] == 'unresolved' for row in rows.values())
        assert all(row['canonical_name'] == '' for row in rows.values())

    def test_every_row_leaves_the_verdict_columns_empty(self, archive,
                                                        records):
        game(archive, 1, 'Michigan', 'Quantico Marines')
        line(records, 'Michigan')
        for row in build(archive, records).values():
            assert row['verdict'] == ''
            assert row['verdict_note'] == ''


class TestWithoutRecords:
    def test_an_uncollected_records_database_sends_everything_to_review(
            self, archive, tmp_path):
        game(archive, 1, 'Quantico Marines', 'Camp Lejeune')
        empty = sqlite3.connect(':memory:')
        with sqlite3.connect(f'file:{archive}?mode=ro', uri=True) as a:
            rows = identity.build(a, empty, 1950, 1950)
        assert {row['status'] for row in rows} == {'unresolved'}


class TestTheSummary:
    def test_it_counts_by_status_and_ranks_the_unresolved(self):
        rows = [
            {'raw_name': 'Michigan', 'status': 'records_matched', 'games': 9,
             'seasons': 1, 'first_season': 1950, 'last_season': 1950,
             'suggestion': '', 'suggestion_ratio': 0.0},
            {'raw_name': 'Camp Lejeune', 'status': 'unresolved', 'games': 2,
             'seasons': 1, 'first_season': 1950, 'last_season': 1950,
             'suggestion': '', 'suggestion_ratio': 0.0},
            {'raw_name': 'Quantico Marines', 'status': 'unresolved',
             'games': 20, 'seasons': 5, 'first_season': 1946,
             'last_season': 1950, 'suggestion': '', 'suggestion_ratio': 0.0},
        ]
        summary = identity.summarize(rows, 1946, 1950)
        assert summary['schools'] == 3
        assert summary['by_status'] == {'records_matched': 1, 'unresolved': 2}
        assert summary['unresolved_games'] == 22
        # Most seasons first: that is the one worth a reviewer's time.
        assert [row['raw_name'] for row in summary['unresolved']] == [
            'Quantico Marines', 'Camp Lejeune']
        assert summary['production_promotion_allowed'] is False

    def test_the_note_says_a_suggestion_is_not_a_resolution(self):
        assert 'never a resolution' in identity.summarize([], 1946, 1979)['note']


class TestItTouchesNothingItShouldNot:
    def test_the_range_is_phase_three_and_nothing_wider(self, archive,
                                                        records):
        with sqlite3.connect(f'file:{archive}?mode=ro', uri=True) as a, \
                sqlite3.connect(f'file:{records}?mode=ro', uri=True) as r:
            for first, last in ((1945, 1950), (1950, 1980), (1960, 1950)):
                with pytest.raises(ValueError, match='1946-1979'):
                    identity.build(a, r, first, last)

    def test_both_databases_are_opened_read_only(self, archive, records,
                                                 tmp_path, monkeypatch):
        game(archive, 1, 'Michigan', 'Army')
        line(records, 'Michigan')
        opened = []
        real = identity.sqlite3.connect

        def spy(target, *args, **kwargs):
            connection = real(target, *args, **kwargs)
            opened.append((str(target), connection))
            return connection

        monkeypatch.setattr(identity.sqlite3, 'connect', spy)
        monkeypatch.setattr(sys, 'argv', [
            'build_historical_identity_map.py', '--staging', str(archive),
            '--records', str(records), '--out', str(tmp_path / 'report'),
            '--start', '1950', '--end', '1950'])
        identity.main()
        for name, table in ((archive.name, 'archive_games'),
                            (records.name, 'archive_team_records')):
            handles = [c for target, c in opened if name in target]
            assert handles, name
            for handle in handles:
                with pytest.raises(sqlite3.OperationalError,
                                   match='readonly|read-only'):
                    handle.execute(f'DELETE FROM {table}')

    def test_it_writes_both_files_and_decides_nothing(self, archive, records,
                                                      tmp_path, monkeypatch):
        game(archive, 1, 'Michigan', 'Quantico Marines')
        line(records, 'Michigan')
        out = tmp_path / 'report'
        monkeypatch.setattr(sys, 'argv', [
            'build_historical_identity_map.py', '--staging', str(archive),
            '--records', str(records), '--out', str(out),
            '--start', '1950', '--end', '1950'])
        identity.main()
        rows = list(csv.DictReader((out / 'historical_schools.csv').open()))
        assert {row['raw_name'] for row in rows} == {'Michigan',
                                                     'Quantico Marines'}
        summary = json.loads(
            (out / 'historical_schools_summary.json').read_text())
        assert summary['by_status'] == {'records_matched': 1,
                                        'unresolved': 1}

    def test_it_builds_no_rating_and_publishes_no_page(self):
        source = SCRIPT.read_text(encoding='utf-8')
        code = '\n'.join(l for l in source.splitlines()
                         if not l.lstrip().startswith('#'))
        for forbidden in ('build_elo', 'build_ratings', 'run_pipeline',
                          'export_static_data', 'ui/', 'INSERT INTO',
                          'UPDATE '):
            assert forbidden not in code, forbidden

    def test_it_needs_nothing_installed(self):
        source = SCRIPT.read_text(encoding='utf-8')
        for third_party in ('import requests', 'import pandas', 'import yaml',
                            'import numpy', 'rapidfuzz'):
            assert third_party not in source, third_party


WORKFLOW = ROOT / '.github/workflows/historical-identity.yml'


@pytest.fixture(scope='module')
def text():
    return WORKFLOW.read_text(encoding='utf-8')


@pytest.fixture(scope='module')
def workflow(text):
    yaml = pytest.importorskip('yaml')
    return yaml.safe_load(text)


@pytest.fixture(scope='module')
def code(text):
    return '\n'.join(line for line in text.splitlines()
                     if not line.lstrip().startswith('#'))


@pytest.fixture(scope='module')
def steps(workflow):
    return workflow['jobs']['identity']['steps']


class TestTheWorkflowThatBuildsIt:
    def test_it_only_runs_when_asked(self, workflow):
        assert set(workflow[True] if True in workflow
                   else workflow['on']) == {'workflow_dispatch'}

    def test_it_needs_no_credential_at_all(self, text, code):
        # It asks no provider for anything, so the key has no business here.
        assert 'CFBD_API_KEY' not in code
        assert 'secrets.' not in code

    def test_it_republishes_neither_chain(self, steps):
        for step in steps:
            if 'upload-artifact' in str(step.get('uses') or ''):
                assert step['with']['name'] not in (
                    'historical-staging-db', 'historical-records-db')

    def test_the_artifact_lookups_are_sorted(self, text):
        assert 'sort_by(.created_at)' in text
        assert 'per_page=1"' not in text

    def test_a_missing_staging_artifact_stops_the_run(self, text):
        assert 'No $name artifact. Run the backfill first' in text

    def test_missing_records_do_not_stop_it(self, text):
        # Without them every name falls to review, which is a worse report
        # but a true one.
        assert 'continuing without it' in text
        assert 'fetch "$ARTIFACT_RECORDS" "$RECORDS" optional' in text

    def test_the_names_to_rule_on_are_printed_not_only_filed(self, text):
        # The artifact store is not reachable from everywhere this is read.
        body = text.split('List every name still to be ruled on', 1)[1]
        assert "print(f\"TODO" in body
        assert 'GITHUB_STEP_SUMMARY' in body

    def test_it_can_read_the_repository_and_its_artifacts_only(self, workflow):
        assert workflow['permissions'] == {'contents': 'read',
                                           'actions': 'read'}

    def test_every_piped_step_sets_pipefail(self, steps):
        for step in steps:
            script = step.get('run') or ''
            if '| tee' in script:
                assert 'set -o pipefail' in script, step.get('name')

    def test_it_builds_no_rating_and_publishes_no_page(self, code):
        for forbidden in ('league.db', 'build_elo', 'build_ratings',
                          'run_pipeline', 'export_static_data', 'ui/',
                          'git commit', 'git push', 'contents: write'):
            assert forbidden not in code, forbidden

    def test_it_needs_no_dependency_install(self, code):
        assert 'pip install' not in code

    def test_it_calls_the_script_with_the_arguments_it_takes(self, text):
        assert 'scripts/build_historical_identity_map.py' in text
        source = SCRIPT.read_text(encoding='utf-8')
        for flag in ('--staging', '--records', '--out', '--start', '--end'):
            assert f"'{flag}'" in source, flag

    def test_the_whole_map_is_printed_to_the_log(self, text):
        # The artifact store is not reachable from every environment this
        # repository is worked in, and the review needs the evidence.
        body = text.split('Print the candidate map in full', 1)[1]
        assert 'cat "$report"' in body
        assert '--- historical_schools.csv ---' in body


MAP = ROOT / 'data/identity/historical_schools.csv'
# The vocabulary a reviewer fills `verdict` from. Everything but `renamed`
# means the name is never rated and only has to be a consistent opponent;
# `renamed` is the one that asserts two names are one programme, so it must
# say which programme.
VERDICTS = {'small_college', 'service_team', 'exhibition', 'renamed',
            'ambiguous'}


@pytest.fixture(scope='module')
def committed():
    return list(csv.DictReader(MAP.open(encoding='utf-8')))


class TestTheCommittedMap:
    """The evidence outlives the artifact it came from.

    The staging database expires 90 days after the run that built it. This
    file is the only durable record of what the archive actually contains,
    and the place the verdicts get written as they are made.
    """

    def test_it_holds_every_name_the_run_found(self, committed):
        assert len(committed) == 405
        assert len({row['raw_name'] for row in committed}) == 405

    def test_each_name_is_in_one_of_the_three_states(self, committed):
        assert {row['status'] for row in committed} == {
            'records_matched', 'registry_matched', 'unresolved'}

    def test_a_resolved_name_carries_what_resolved_it(self, committed):
        for row in committed:
            if row['status'] == 'records_matched':
                assert row['canonical_name'], row['raw_name']
                assert row['cfbd_team_ids'], row['raw_name']
            elif row['status'] == 'registry_matched':
                assert row['canonical_name'], row['raw_name']

    def test_an_unresolved_name_claims_no_identity(self, committed):
        for row in committed:
            if row['status'] == 'unresolved':
                assert row['canonical_name'] == '', row['raw_name']

    def test_a_verdict_is_one_of_the_words_that_mean_something(self,
                                                               committed):
        for row in committed:
            if row['verdict']:
                assert row['verdict'] in VERDICTS, row['raw_name']

    def test_calling_a_name_a_rename_says_what_it_was_renamed_to(self,
                                                                 committed):
        # The one verdict that asserts two names are one programme. Without
        # a canonical name it asserts it and does not say it, which is how
        # a wrong merge gets in unnoticed.
        for row in committed:
            if row['verdict'] == 'renamed':
                assert row['canonical_name'], row['raw_name']

    def test_the_suggestion_column_was_never_promoted_into_an_identity(
            self, committed):
        # Two of the suggestions in this very file are wrong -- Georgia
        # State for Emporia State, Northwestern for Northeastern. They are
        # leads, and this is what keeps them leads.
        for row in committed:
            if row['status'] == 'unresolved' and row['suggestion']:
                assert row['canonical_name'] == '', row['raw_name']
                assert row['canonical_name'] != row['suggestion']

    def test_the_evidence_columns_are_filled_for_every_name(self, committed):
        for row in committed:
            assert int(row['games']) > 0, row['raw_name']
            assert 1946 <= int(row['first_season']) <= int(row['last_season']) <= 1979
            assert int(row['as_home']) + int(row['as_away']) == int(row['games'])
