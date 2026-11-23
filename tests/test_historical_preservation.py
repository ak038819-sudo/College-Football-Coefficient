"""The preservation gate has to catch a rewritten column, so every test
here mutates a staged row and insists the audit notices.

A row is built payload-first: the payload is the source of truth and the
columns are written from it, exactly as the importer does. Each test then
damages one of them.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts' / 'audit_historical_preservation.py'
sys.path.insert(0, str(ROOT / 'scripts'))

import audit_historical_preservation as audit  # noqa: E402

SCHEMA = """
CREATE TABLE archive_games (
    source TEXT NOT NULL,
    source_game_id TEXT NOT NULL,
    season INTEGER NOT NULL,
    season_type TEXT,
    start_date TEXT,
    home_name_raw TEXT,
    away_name_raw TEXT,
    home_score INTEGER,
    away_score INTEGER,
    neutral_site INTEGER,
    home_classification TEXT,
    away_classification TEXT,
    home_conference TEXT,
    away_conference TEXT,
    venue TEXT,
    source_payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    PRIMARY KEY (source, source_game_id)
);
"""

COLUMNS = ['season_type', 'start_date', 'home_name_raw', 'away_name_raw',
           'home_score', 'away_score', 'neutral_site',
           'home_classification', 'away_classification',
           'home_conference', 'away_conference', 'venue']


def game(gid='1', season=1952, **overrides):
    """One provider response body, in the provider's own field names."""
    body = {
        'id': int(gid),
        'season': season,
        'seasonType': 'regular',
        'startDate': f'{season}-10-04T19:00:00.000Z',
        'homeTeam': 'Army',
        'awayTeam': 'Navy',
        'homePoints': 21,
        'awayPoints': 7,
        'neutralSite': False,
        'homeClassification': 'fbs',
        'awayClassification': 'fbs',
        'homeConference': 'FBS Independents',
        'awayConference': 'FBS Independents',
        'venue': 'Michie Stadium',
    }
    body.update(overrides)
    return body


def columns_from(body):
    """The columns the importer would write for this payload."""
    def pick(*keys):
        for key in keys:
            if key in body:
                return body[key]
        return None
    neutral = pick('neutralSite', 'neutral_site')
    return {
        'season_type': pick('seasonType', 'season_type'),
        'start_date': pick('startDate', 'start_date'),
        'home_name_raw': pick('homeTeam', 'home_team'),
        'away_name_raw': pick('awayTeam', 'away_team'),
        'home_score': pick('homePoints', 'home_points'),
        'away_score': pick('awayPoints', 'away_points'),
        'neutral_site': None if neutral is None else int(bool(neutral)),
        'home_classification': pick('homeClassification',
                                    'home_classification'),
        'away_classification': pick('awayClassification',
                                    'away_classification'),
        'home_conference': pick('homeConference', 'home_conference'),
        'away_conference': pick('awayConference', 'away_conference'),
        'venue': pick('venue'),
    }


def staged(tmp_path, bodies, damage=None, name='historical_staging.db'):
    """A staging database holding these payloads, optionally damaged.

    `damage` is {source_game_id: {column: value}} and is applied AFTER the
    columns are written from the payload, so the payload still says what it
    always said.
    """
    path = tmp_path / name
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    for body in bodies:
        gid = str(body['id'])
        raw = json.dumps(body, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'))
        cols = columns_from(body)
        # The importer stores the season it REQUESTED, which is why a
        # payload may omit one.
        cols['season'] = body.get('season', 1952)
        cols['source_game_id'] = gid
        for column, value in (damage or {}).get(gid, {}).items():
            cols[column] = value
        db.execute(
            'INSERT INTO archive_games VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'
            '?,?,?)',
            ('cfbd', cols['source_game_id'], cols['season'],
             *[cols[c] for c in COLUMNS], raw,
             hashlib.sha256(raw.encode()).hexdigest()))
    db.commit()
    db.close()
    return path


def run(path, start=1946, end=1979):
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as db:
        return audit.audit(db, start, end)


class TestAnUndamagedArchivePasses:
    def test_no_findings(self, tmp_path):
        result = run(staged(tmp_path, [game('1'), game('2', 1960)]))
        assert result['findings'] == []
        assert result['games'] == 2
        assert result['corrupt_payloads'] == []
        assert result['unreadable_payloads'] == []

    def test_an_empty_classification_is_not_a_disagreement(self, tmp_path):
        # The provider writes '' for a school it has no division for, and
        # the importer stores that ''. Neither asserts a division, so the
        # two agree.
        body = game('1', homeClassification='')
        result = run(staged(tmp_path, [body], damage={'1': {
            'home_classification': None}}))
        assert result['findings'] == []

    def test_a_silent_season_type_is_not_a_disagreement(self, tmp_path):
        # store() writes the season type that was REQUESTED, so a payload
        # that never mentions one cannot contradict it.
        body = game('1')
        del body['seasonType']
        result = run(staged(tmp_path, [body], damage={'1': {
            'season_type': 'postseason'}}))
        assert result['findings'] == []


class TestARewrittenColumnIsCaught:
    @pytest.mark.parametrize('column,value,payload_says', [
        ('home_score', None, '21'),
        ('start_date', None, '1952-10-04T19:00:00.000Z'),
        ('home_name_raw', None, 'Army'),
        ('neutral_site', None, '0'),
        ('home_classification', None, 'fbs'),
        ('venue', None, 'Michie Stadium'),
    ])
    def test_a_blanked_column_is_a_loss(self, tmp_path, column, value,
                                        payload_says):
        result = run(staged(tmp_path, [game('1')],
                            damage={'1': {column: value}}))
        assert len(result['findings']) == 1
        finding = result['findings'][0]
        assert finding['column'] == column
        assert finding['kind'] == 'loss'
        assert finding['payload'] == payload_says
        assert finding['stored'] == ''

    def test_a_column_the_payload_lacks_is_an_invention(self, tmp_path):
        body = game('1')
        del body['venue']
        result = run(staged(tmp_path, [body],
                            damage={'1': {'venue': 'Soldier Field'}}))
        assert [(f['column'], f['kind'], f['stored'])
                for f in result['findings']] == [
            ('venue', 'invention', 'Soldier Field')]

    @pytest.mark.parametrize('column,value', [
        ('home_score', 28),
        ('away_name_raw', 'Notre Dame'),
        ('neutral_site', 1),
        ('away_classification', 'fcs'),
        ('home_conference', 'Big Ten'),
    ])
    def test_a_changed_column_is_a_mismatch(self, tmp_path, column, value):
        result = run(staged(tmp_path, [game('1')],
                            damage={'1': {column: value}}))
        assert [(f['column'], f['kind']) for f in result['findings']] == [
            (column, 'mismatch')]

    def test_a_season_type_the_payload_contradicts_is_caught(self, tmp_path):
        result = run(staged(tmp_path, [game('1')],
                            damage={'1': {'season_type': 'postseason'}}))
        assert [(f['column'], f['kind']) for f in result['findings']] == [
            ('season_type', 'mismatch')]

    def test_a_tie_turned_into_a_win_is_caught(self, tmp_path):
        # The failure the plan names first: a tie quietly resolved.
        body = game('1', homePoints=7, awayPoints=7)
        result = run(staged(tmp_path, [body],
                            damage={'1': {'away_score': 6}}))
        assert [(f['column'], f['kind'], f['payload'])
                for f in result['findings']] == [
            ('away_score', 'mismatch', '7')]

    def test_several_damaged_rows_are_all_reported(self, tmp_path):
        result = run(staged(
            tmp_path, [game('1'), game('2', 1960), game('3', 1971)],
            damage={'1': {'venue': None}, '3': {'home_score': 99}}))
        assert sorted(f['source_game_id'] for f in result['findings']) == \
            ['1', '3']


class TestProvenanceIsCheckedToo:
    """The season decides which team-season every later gate credits a
    game to, so a season that drifted moves evidence between years without
    touching a score."""

    def test_a_drifted_season_is_caught(self, tmp_path):
        result = run(staged(tmp_path, [game('1', 1952)],
                            damage={'1': {'season': 1960}}))
        assert [(f['column'], f['kind'], f['stored'], f['payload'])
                for f in result['findings']] == [
            ('season', 'mismatch', '1960', '1952')]

    def test_a_payload_silent_about_the_season_is_not_a_disagreement(
            self, tmp_path):
        # normalized() writes the year that was REQUESTED.
        body = game('1', 1952)
        del body['season']
        result = run(staged(tmp_path, [body], damage={'1': {'season': 1960}}))
        assert result['findings'] == []

    def test_a_drifted_game_id_is_caught(self, tmp_path):
        result = run(staged(tmp_path, [game('1')],
                            damage={'1': {'source_game_id': '4242'}}))
        assert [(f['column'], f['kind'], f['payload'])
                for f in result['findings']] == [
            ('source_game_id', 'mismatch', '1')]


class TestTheCensusReadsThePayloadNotTheColumns:
    """The columns are what this audit is checking. Letting them answer a
    question about the source would hand the corruption the last word."""

    def test_a_tie_hidden_by_a_drifted_score_is_still_counted(self,
                                                              tmp_path):
        body = game('1', homePoints=7, awayPoints=7)
        result = run(staged(tmp_path, [body],
                            damage={'1': {'away_score': 6}}))
        summary = audit.summarize(result, 1946, 1979)
        assert summary['preserved']['ties'] == 1
        assert summary['column_findings']['total'] == 1

    def test_an_unknown_neutral_site_hidden_by_a_drifted_column_survives(
            self, tmp_path):
        # Otherwise the report would announce that the provider is never
        # uncertain about a neutral site, on the strength of the very
        # column it just reported as drifted.
        body = game('1', neutralSite=None)
        result = run(staged(tmp_path, [body],
                            damage={'1': {'neutral_site': 0}}))
        summary = audit.summarize(result, 1946, 1979)
        assert summary['neutral_site'] == {'true': 0, 'false': 0,
                                           'unknown': 1}
        assert summary['neutral_site_is_three_valued'] is True

    def test_a_classification_invented_in_a_column_is_not_counted_as_known(
            self, tmp_path):
        body = game('1', homeClassification='')
        result = run(staged(tmp_path, [body],
                            damage={'1': {'home_classification': 'fbs'}}))
        summary = audit.summarize(result, 1946, 1979)
        assert summary['preserved']['home_classification_unknown'] == 1

    def test_unverified_payloads_are_left_out_of_the_census(self, tmp_path):
        path = staged(tmp_path, [game('1'), game('2', 1960)])
        db = sqlite3.connect(path)
        db.execute("UPDATE archive_games SET source_payload_json='{}'"
                   " WHERE source_game_id='1'")
        db.commit()
        db.close()
        summary = audit.summarize(run(path), 1946, 1979)
        assert summary['games'] == 2
        assert summary['census_games'] == 1


class TestADamagedPayloadIsNotTrusted:
    def test_a_payload_that_fails_its_digest_is_reported(self, tmp_path):
        path = staged(tmp_path, [game('1')])
        db = sqlite3.connect(path)
        db.execute("UPDATE archive_games SET source_payload_json = ?",
                   (json.dumps(game('1', homePoints=99)),))
        db.commit()
        db.close()
        result = run(path)
        assert len(result['corrupt_payloads']) == 1
        # ...and it is not then read as if it were evidence.
        assert result['findings'] == []

    def test_a_payload_that_is_not_json_is_reported(self, tmp_path):
        path = staged(tmp_path, [game('1')])
        raw = 'not json at all'
        db = sqlite3.connect(path)
        db.execute('UPDATE archive_games SET source_payload_json = ?,'
                   ' payload_sha256 = ?',
                   (raw, hashlib.sha256(raw.encode()).hexdigest()))
        db.commit()
        db.close()
        result = run(path)
        assert len(result['unreadable_payloads']) == 1
        assert result['findings'] == []

    def test_a_payload_that_is_not_an_object_is_reported(self, tmp_path):
        path = staged(tmp_path, [game('1')])
        raw = '[1, 2, 3]'
        db = sqlite3.connect(path)
        db.execute('UPDATE archive_games SET source_payload_json = ?,'
                   ' payload_sha256 = ?',
                   (raw, hashlib.sha256(raw.encode()).hexdigest()))
        db.commit()
        db.close()
        result = run(path)
        assert len(result['unreadable_payloads']) == 1
        assert result['findings'] == []


class TestTheCensusCountsWhatIsUncertain:
    def test_ties_missing_scores_and_dates(self, tmp_path):
        bodies = [
            game('1', homePoints=7, awayPoints=7),
            game('2', homePoints=None),
            game('3', startDate=None),
            game('4'),
        ]
        result = run(staged(tmp_path, bodies))
        summary = audit.summarize(result, 1946, 1979)
        assert summary['preserved']['ties'] == 1
        assert summary['preserved']['missing_score'] == 1
        assert summary['preserved']['missing_date'] == 1

    def test_a_missing_score_is_never_counted_as_a_tie(self, tmp_path):
        # Two absent scores are not 0-0, and not level either.
        bodies = [game('1', homePoints=None, awayPoints=None)]
        summary = audit.summarize(run(staged(tmp_path, bodies)), 1946, 1979)
        assert summary['preserved']['missing_score'] == 1
        assert summary['preserved']['ties'] == 0

    def test_unknown_classification_is_counted_per_side(self, tmp_path):
        bodies = [
            game('1', homeClassification='', awayClassification='fbs'),
            game('2', homeClassification=None, awayClassification=None),
        ]
        summary = audit.summarize(run(staged(tmp_path, bodies)), 1946, 1979)
        assert summary['preserved']['home_classification_unknown'] == 2
        assert summary['preserved']['away_classification_unknown'] == 1

    def test_neutral_site_is_counted_three_ways(self, tmp_path):
        bodies = [
            game('1', neutralSite=True),
            game('2', neutralSite=False),
            game('3', neutralSite=None),
            game('4', neutralSite=False),
        ]
        summary = audit.summarize(run(staged(tmp_path, bodies)), 1946, 1979)
        assert summary['neutral_site'] == {'true': 1, 'false': 2,
                                           'unknown': 1}
        assert summary['neutral_site_is_three_valued'] is True

    def test_an_archive_that_never_says_unknown_is_two_valued(self,
                                                              tmp_path):
        # The finding that matters if it comes back: every 'false' would
        # then be the provider's assertion, not our inference.
        bodies = [game('1', neutralSite=True), game('2', neutralSite=False)]
        summary = audit.summarize(run(staged(tmp_path, bodies)), 1946, 1979)
        assert summary['neutral_site']['unknown'] == 0
        assert summary['neutral_site_is_three_valued'] is False


class TestTheSummaryCountsHonestly:
    def test_kinds_and_columns_are_tallied(self, tmp_path):
        result = run(staged(
            tmp_path, [game('1'), game('2', 1960)],
            damage={'1': {'venue': None, 'home_score': 99},
                    '2': {'away_score': None}}))
        summary = audit.summarize(result, 1946, 1979)
        assert summary['column_findings']['total'] == 3
        assert summary['column_findings']['by_kind'] == {'loss': 2,
                                                         'mismatch': 1}
        assert summary['column_findings']['by_column'] == {
            'away_score': 1, 'home_score': 1, 'venue': 1}

    def test_verified_payloads_exclude_the_damaged_ones(self, tmp_path):
        path = staged(tmp_path, [game('1'), game('2', 1960)])
        db = sqlite3.connect(path)
        db.execute("UPDATE archive_games SET source_payload_json='{}'"
                   " WHERE source_game_id='1'")
        db.commit()
        db.close()
        summary = audit.summarize(run(path), 1946, 1979)
        assert summary['games'] == 2
        assert summary['payloads'] == {'corrupt': 1, 'unreadable': 0,
                                       'verified': 1}

    def test_promotion_is_never_allowed(self, tmp_path):
        summary = audit.summarize(run(staged(tmp_path, [game('1')])),
                                  1946, 1979)
        assert summary['production_promotion_allowed'] is False


class TestTheRangeIsTheWholeArchive:
    @pytest.mark.parametrize('start,end', [(1868, 1979), (1869, 1980),
                                           (1960, 1950)])
    def test_outside_the_range_is_refused(self, tmp_path, start, end):
        path = staged(tmp_path, [game('1')])
        with pytest.raises(ValueError):
            run(path, start, end)

    def test_a_season_outside_the_window_is_not_read(self, tmp_path):
        result = run(staged(tmp_path, [game('1', 1952), game('2', 1971)]),
                     1946, 1960)
        assert result['games'] == 1


class TestItOpensNothingItShouldNot:
    def test_production_is_refused_by_name(self, tmp_path, monkeypatch,
                                           capsys):
        path = tmp_path / 'league.db'
        path.touch()
        monkeypatch.setattr(sys, 'argv', [
            'audit', '--staging', str(path), '--out', str(tmp_path / 'out')])
        with pytest.raises(SystemExit):
            audit.main()
        assert 'Refusing production database' in capsys.readouterr().err

    def test_the_connection_it_opens_cannot_write(self, tmp_path,
                                                  monkeypatch):
        # Exercising the capability is the only proof. 'Nothing changed
        # afterwards' passes under mode=rw too.
        path = staged(tmp_path, [game('1')])
        opened = []
        real = sqlite3.connect

        def spy(*args, **kwargs):
            db = real(*args, **kwargs)
            opened.append(db)
            return db

        monkeypatch.setattr(sqlite3, 'connect', spy)
        monkeypatch.setattr(sys, 'argv', [
            'audit', '--staging', str(path), '--out', str(tmp_path / 'out')])
        audit.main()
        assert opened, 'main() opened no database'
        for db in opened:
            with pytest.raises(sqlite3.OperationalError):
                db.execute("INSERT INTO archive_games (source,"
                           " source_game_id, season, source_payload_json,"
                           " payload_sha256) VALUES ('x','x',1952,'{}','x')")

    def test_it_does_not_import_the_importer(self):
        # Reading the payload back with the function that wrote the row
        # would only prove that function is deterministic.
        source = SCRIPT.read_text()
        assert 'import_historical_archive' not in source
        assert not re.search(r'^\s*(import|from)\s+\w*import_historical',
                             source, re.M)


class TestTheReportIsWritten:
    def test_main_writes_both_files(self, tmp_path, monkeypatch, capsys):
        path = staged(tmp_path, [game('1'), game('2', 1960)],
                      damage={'2': {'venue': None}})
        out = tmp_path / 'report'
        monkeypatch.setattr(sys, 'argv', [
            'audit', '--staging', str(path), '--out', str(out)])
        audit.main()
        summary = json.loads(
            (out / 'historical_preservation_summary.json').read_text())
        assert summary['column_findings']['total'] == 1
        rows = (out / 'historical_preservation_findings.csv'
                ).read_text().strip().splitlines()
        assert rows[0].startswith('source_game_id,season,column,kind')
        assert len(rows) == 2
        assert 'venue' in rows[1]
        assert '"games": 2' in capsys.readouterr().out

    def test_an_empty_range_is_an_error_not_a_pass(self, tmp_path,
                                                   monkeypatch):
        path = staged(tmp_path, [game('1', 1952)])
        monkeypatch.setattr(sys, 'argv', [
            'audit', '--staging', str(path), '--out', str(tmp_path / 'out'),
            '--start', '1970', '--end', '1979'])
        with pytest.raises(SystemExit) as exc:
            audit.main()
        assert 'Nothing staged' in str(exc.value)

    def test_it_runs_as_a_program(self, tmp_path):
        path = staged(tmp_path, [game('1')])
        out = tmp_path / 'report'
        done = subprocess.run(
            [sys.executable, str(SCRIPT), '--staging', str(path),
             '--out', str(out)], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
        assert json.loads(done.stdout)['games'] == 1


WORKFLOW = ROOT / '.github' / 'workflows' / 'historical-preservation.yml'


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
    return workflow['jobs']['preservation']['steps']


class TestTheWorkflowThatRunsIt:
    def test_it_only_runs_when_asked(self, workflow):
        assert set(workflow[True] if True in workflow
                   else workflow['on']) == {'workflow_dispatch'}

    def test_it_needs_no_credential_at_all(self, code):
        # It asks no provider for anything, so the key has no business here.
        assert 'CFBD_API_KEY' not in code
        assert 'secrets.' not in code

    def test_it_can_read_the_repository_and_its_artifacts_only(self,
                                                               workflow):
        assert workflow['permissions'] == {'contents': 'read',
                                           'actions': 'read'}

    def test_it_never_republishes_the_staging_chain(self, steps):
        uploads = [step for step in steps
                   if 'upload-artifact' in str(step.get('uses') or '')]
        assert uploads, 'the report is never published'
        for step in uploads:
            assert step['with']['name'] != 'historical-staging-db'

    def test_the_artifact_lookup_is_sorted(self, text):
        # The endpoint promises no order; row one once dropped a batch.
        assert 'sort_by(.created_at)' in text
        assert 'per_page=1"' not in text

    def test_a_missing_staging_artifact_stops_the_run(self, text):
        assert 'Run the backfill first' in text

    def test_it_runs_the_audit_and_nothing_that_writes(self, code):
        assert 'scripts/audit_historical_preservation.py' in code
        for forbidden in ('run_pipeline.py', 'build_elo', 'league.db',
                          'export_static_data', 'import_historical_archive'):
            assert forbidden not in code

    def test_the_verdict_is_printed_not_only_filed(self, text):
        # The artifact store is not reachable from everywhere this is read.
        body = text.split('Say plainly whether anything drifted', 1)[1]
        assert 'GITHUB_STEP_SUMMARY' in body
        assert "print('\\n'.join(lines))" in body

    def test_it_is_not_cancelled_by_the_next_dispatch(self, workflow):
        assert workflow['concurrency']['cancel-in-progress'] is False
