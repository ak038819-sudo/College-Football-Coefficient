"""The 1946-1979 source reconciliation, driven rather than read.

What this script is for is catching a season that looks complete because
every partition answered and is missing games the provider itself believes
were played. So the tests build an archive that disagrees with the records
feed in each of the ways that can happen, and assert the disagreement is
named -- not smoothed over, and not inferred away.

The script asks the provider for records, so every test here passes its own
fetcher. Nothing in this file reaches the network.
"""

import csv
import importlib.util
import json
import re
import sys
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
SCRIPT = ROOT / 'scripts/reconcile_historical_sources.py'


def load():
    spec = importlib.util.spec_from_file_location('reconcile_historical', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reconciler = load()


WORKFLOW = ROOT / '.github/workflows/historical-reconcile.yml'


@pytest.fixture(scope='module')
def text():
    return WORKFLOW.read_text(encoding='utf-8')


@pytest.fixture(scope='module')
def workflow(text):
    yaml = pytest.importorskip('yaml')
    return yaml.safe_load(text)


@pytest.fixture(scope='module')
def code(text):
    """The workflow with its comments stripped.

    They name what the file must not do, so a search of the whole text
    finds the promise instead of a breach of it.
    """
    return '\n'.join(line for line in text.splitlines()
                     if not line.lstrip().startswith('#'))


@pytest.fixture(scope='module')
def steps(workflow):
    return workflow['jobs']['reconcile']['steps']


def record(team, regular=(0, 0, 0, 0), postseason=(0, 0, 0, 0), year=1950,
           conference='Independent', classification=None, team_id=1):
    def block(counts):
        return dict(zip(('games', 'wins', 'losses', 'ties'), counts))
    return {'year': year, 'teamId': team_id, 'team': team,
            'classification': classification, 'conference': conference,
            'division': '', 'expectedWins': None,
            'total': block(tuple(a + b for a, b in zip(regular, postseason))),
            'conferenceGames': block((0, 0, 0, 0)),
            'homeGames': block((0, 0, 0, 0)),
            'awayGames': block((0, 0, 0, 0)),
            'neutralSiteGames': block((0, 0, 0, 0)),
            'regularSeason': block(regular), 'postseason': block(postseason)}


@pytest.fixture
def archive(tmp_path):
    """A staged archive, in Phase 2's schema, written once and then read."""
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
    CREATE TABLE archive_fetches (
        season INTEGER NOT NULL, season_type TEXT NOT NULL,
        status TEXT NOT NULL, fetched_at TEXT NOT NULL,
        record_count INTEGER NOT NULL, error TEXT,
        PRIMARY KEY (season, season_type));
    """)
    db.commit()
    db.close()
    return path


def game(path, gid, home, away, home_score, away_score, season=1950,
         kind='regular'):
    db = sqlite3.connect(path)
    with db:
        db.execute(
            'INSERT INTO archive_games VALUES '
            '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('cfbd', str(gid), season, kind, f'{season}-10-01T00:00:00.000Z',
             home, away, home_score, away_score, 0, None, None, None, None,
             None, '{}', 'x' * 64))
    db.close()


def run(archive_path, tmp_path, payloads, start=1950, end=1950):
    """Collect the given records and reconcile, as main() would."""
    records_path = tmp_path / 'records.db'
    records = sqlite3.connect(records_path)
    reconciler.collect(records, start, end, 'key', pause=0,
                       fetcher=lambda year, key: payloads.get(year, []))
    with sqlite3.connect(f'file:{archive_path}?mode=ro', uri=True) as read_only:
        rows = reconciler.reconcile(read_only, records, start, end)
    records.close()
    return rows


def by_team(rows):
    return {row['team_raw']: row for row in rows}


def main_offline(archive_path, tmp_path, monkeypatch, start=1950, end=1950):
    """The entry point the workflow calls, asking the provider for nothing.

    A module a workflow invokes needs a test that invokes it: this
    repository has twice shipped a working library under a broken main().
    """
    out = tmp_path / 'report'
    monkeypatch.setattr(sys, 'argv', [
        'reconcile_historical_sources.py',
        '--staging', str(archive_path),
        '--db', str(tmp_path / 'records.db'),
        '--out', str(out),
        '--start', str(start), '--end', str(end), '--offline'])
    reconciler.main()
    return out


class TestTheTalliesComeFromTheGames:
    def test_a_win_a_loss_and_a_tie_are_counted_on_both_sides(self, archive):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        game(archive, 2, 'Army', 'Yale', 0, 7)
        game(archive, 3, 'Army', 'Penn', 14, 14)
        db = sqlite3.connect(f'file:{archive}?mode=ro', uri=True)
        tally = reconciler.staged(db, 1950)
        assert tally['Army'] == {'regular_games': 3, 'regular_wins': 1,
                                 'regular_losses': 1, 'regular_ties': 1}
        assert tally['Navy']['regular_losses'] == 1
        assert tally['Yale']['regular_wins'] == 1
        assert tally['Penn']['regular_ties'] == 1

    def test_a_missing_score_is_undecided_and_never_a_tie(self, archive):
        # A tie is a result the archive would be claiming. It does not have
        # one; it has a hole.
        game(archive, 1, 'Army', 'Navy', None, None)
        db = sqlite3.connect(f'file:{archive}?mode=ro', uri=True)
        tally = reconciler.staged(db, 1950)
        assert tally['Army']['regular_games'] == 1
        assert tally['Army']['regular_undecided'] == 1
        assert 'regular_ties' not in tally['Army']

    def test_the_two_season_types_are_tallied_apart(self, archive):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        game(archive, 2, 'Army', 'Rice', 7, 6, kind='postseason')
        db = sqlite3.connect(f'file:{archive}?mode=ro', uri=True)
        tally = reconciler.staged(db, 1950)
        assert tally['Army']['regular_wins'] == 1
        assert tally['Army']['postseason_wins'] == 1


class TestWhatItCatches:
    def test_an_agreeing_season_is_reconciled(self, archive, tmp_path):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Army', regular=(1, 1, 0, 0)),
            record('Navy', regular=(1, 0, 1, 0))]}))
        assert rows['Army']['findings'] == ''
        assert rows['Army']['reconciled'] is True
        assert rows['Navy']['reconciled'] is True

    def test_a_game_the_provider_believes_in_but_we_did_not_stage(
            self, archive, tmp_path):
        # THE POINT OF THE WHOLE SCRIPT. Nine games staged, ten in the
        # record: the partition answered, the count was consistent, and a
        # game is still missing.
        for index in range(9):
            game(archive, index, 'Army', f'Opponent {index}', 20, 0)
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Army', regular=(10, 10, 0, 0))]}))
        assert rows['Army']['findings'] == 'regular_game_count_mismatch'
        assert rows['Army']['delta_regular_games'] == -1
        assert rows['Army']['staged_regular_games'] == 9
        assert rows['Army']['reference_regular_games'] == 10

    def test_the_same_count_with_different_results_is_named_differently(
            self, archive, tmp_path):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Army', regular=(1, 0, 1, 0))]}))
        assert rows['Army']['findings'] == 'regular_result_mismatch'
        assert rows['Army']['delta_regular_wins'] == 1
        assert rows['Army']['delta_regular_losses'] == -1

    def test_a_team_with_a_record_and_no_staged_games_at_all(
            self, archive, tmp_path):
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Centre', regular=(8, 5, 3, 0))]}))
        assert rows['Centre']['findings'] == 'reference_has_games_none_staged'

    def test_a_team_we_staged_that_the_records_feed_never_mentions(
            self, archive, tmp_path):
        # Reported, not dropped: an unmatched name is what the identity map
        # is built from.
        game(archive, 1, 'Army', 'Camp Lejeune', 42, 0)
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Army', regular=(1, 1, 0, 0))]}))
        assert rows['Camp Lejeune']['findings'] == 'no_reference_line'
        assert rows['Camp Lejeune']['staged_regular_losses'] == 1
        assert rows['Camp Lejeune']['reference_regular_games'] is None

    def test_an_absent_reference_number_is_not_treated_as_a_zero(
            self, archive, tmp_path):
        game(archive, 1, 'Army', 'Camp Lejeune', 42, 0)
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Army', regular=(1, 1, 0, 0))]}))
        # A delta against nothing would read as "one game too many".
        assert rows['Camp Lejeune']['delta_regular_games'] is None

    def test_a_postseason_disagreement_is_named_as_postseason(
            self, archive, tmp_path):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        game(archive, 2, 'Army', 'Rice', 7, 6, kind='postseason')
        rows = by_team(run(archive, tmp_path, {1950: [
            record('Army', regular=(1, 1, 0, 0), postseason=(0, 0, 0, 0))]}))
        assert rows['Army']['findings'] == 'postseason_game_count_mismatch'

    def test_a_season_whose_records_were_never_collected_says_so(
            self, archive, tmp_path):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        records_path = tmp_path / 'records.db'
        records = sqlite3.connect(records_path)
        reconciler.setup(records)
        with sqlite3.connect(f'file:{archive}?mode=ro', uri=True) as read_only:
            rows = by_team(reconciler.reconcile(read_only, records, 1950, 1950))
        records.close()
        assert 'records_not_collected' in rows['Army']['findings']


class TestTheFeedIsStoredFaithfully:
    def test_the_payload_and_its_digest_are_kept(self, tmp_path):
        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.collect(records, 1950, 1950, 'key', pause=0,
                           fetcher=lambda year, key: [
                               record('Army', regular=(9, 8, 1, 0))])
        raw, digest = records.execute(
            'SELECT source_payload_json, payload_sha256 FROM '
            'archive_team_records').fetchone()
        assert json.loads(raw)['team'] == 'Army'
        assert len(digest) == 64
        records.close()

    def test_a_season_already_collected_is_not_asked_for_again(self, tmp_path):
        asked = []

        def fetcher(year, key):
            asked.append(year)
            return [record('Army', regular=(9, 8, 1, 0))]

        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.collect(records, 1950, 1950, 'key', pause=0, fetcher=fetcher)
        reconciler.collect(records, 1950, 1950, 'key', pause=0, fetcher=fetcher)
        assert asked == [1950]
        reconciler.collect(records, 1950, 1950, 'key', pause=0,
                           fetcher=fetcher, force=True)
        assert asked == [1950, 1950]
        records.close()

    def test_a_failed_season_is_checkpointed_as_failed_and_raises(
            self, tmp_path):
        records = sqlite3.connect(tmp_path / 'records.db')

        def fetcher(year, key):
            raise RuntimeError('provider said no')

        with pytest.raises(RuntimeError):
            reconciler.collect(records, 1950, 1950, 'key', pause=0,
                               fetcher=fetcher)
        status, error = records.execute(
            'SELECT status, error FROM archive_record_fetches').fetchone()
        assert status == 'failed'
        assert 'provider said no' in error
        records.close()

    def test_two_lines_for_one_team_in_one_season_are_refused(self, tmp_path):
        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.setup(records)
        with pytest.raises(ValueError, match='Duplicate team names'):
            reconciler.store(records, 1950, [record('Army'), record('Army')])
        records.close()

    def test_a_line_for_the_wrong_season_is_refused(self, tmp_path):
        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.setup(records)
        with pytest.raises(ValueError, match='Unexpected records season'):
            reconciler.store(records, 1950, [record('Army', year=1951)])
        records.close()

    def test_a_line_with_no_team_is_refused(self, tmp_path):
        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.setup(records)
        with pytest.raises(ValueError, match='no team name'):
            reconciler.store(records, 1950, [record('')])
        records.close()

    def test_both_spellings_of_the_regular_season_block_are_read(self):
        snake = {'year': 1950, 'team': 'Army',
                 'regular_season': {'games': 9, 'wins': 8, 'losses': 1,
                                    'ties': 0}}
        assert reconciler.split(
            snake, *reconciler.REFERENCE_SPLITS['regular']) == (9, 8, 1, 0)

    def test_a_missing_block_reads_as_nothing_rather_than_zeroes(self):
        assert reconciler.split({}, 'regularSeason') == (None,) * 4


class TestItTouchesNothingItShouldNot:
    def test_the_staging_archive_is_opened_read_only(
            self, archive, tmp_path, monkeypatch):
        # Proven by writing through THE CONNECTION main() OPENED. Opening a
        # second read-only handle and finding it read-only tests sqlite,
        # and passes just as well when the script asked for read-write.
        game(archive, 1, 'Army', 'Navy', 21, 0)
        opened = []
        real = reconciler.sqlite3.connect

        def spy(target, *args, **kwargs):
            connection = real(target, *args, **kwargs)
            opened.append((str(target), connection))
            return connection

        monkeypatch.setattr(reconciler.sqlite3, 'connect', spy)
        main_offline(archive, tmp_path, monkeypatch)
        staging = [connection for target, connection in opened
                   if archive.name in target]
        assert staging, 'main() never opened the staging archive'
        for connection in staging:
            with pytest.raises(sqlite3.OperationalError,
                               match='readonly|read-only'):
                connection.execute('DELETE FROM archive_games')

    def test_the_records_never_go_into_the_staging_database(self, archive,
                                                            tmp_path):
        # The staging artifact chain has ONE writer. A second script writing
        # into it is how a chain starts losing batches.
        run(archive, tmp_path, {1950: []})
        tables = {row[0] for row in sqlite3.connect(archive).execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert 'archive_team_records' not in tables

    def test_the_range_is_phase_three_and_nothing_wider(self, archive,
                                                        tmp_path):
        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.setup(records)
        with sqlite3.connect(f'file:{archive}?mode=ro', uri=True) as db:
            for first, last in ((1945, 1950), (1950, 1980), (1960, 1950)):
                with pytest.raises(ValueError, match='1946-1979'):
                    reconciler.reconcile(db, records, first, last)
        records.close()

    def test_it_builds_no_rating_and_publishes_no_page(self):
        source = SCRIPT.read_text(encoding='utf-8')
        code = '\n'.join(line for line in source.splitlines()
                         if not line.lstrip().startswith('#'))
        for forbidden in ('build_elo', 'build_ratings', 'run_pipeline',
                          'export_static_data', 'ui/'):
            assert forbidden not in code, forbidden

    def test_it_opens_no_database_it_was_not_handed(self):
        # Rather than searching for the production file's name, which the
        # refusal that rejects it also contains.
        source = SCRIPT.read_text(encoding='utf-8')
        opens = re.findall(r'sqlite3\.connect\((.*?)\)', source, re.S)
        assert opens, 'the script opens nothing at all'
        for opened in opens:
            assert 'args.db' in opened or 'args.staging' in opened, opened

    def test_it_refuses_the_production_database_by_name(self):
        assert 'league.db' in SCRIPT.read_text(encoding='utf-8')
        assert 'Refusing production database' in SCRIPT.read_text(
            encoding='utf-8')

    def test_it_needs_nothing_installed(self):
        source = SCRIPT.read_text(encoding='utf-8')
        for third_party in ('import requests', 'import pandas', 'import yaml',
                            'import numpy'):
            assert third_party not in source, third_party


class TestTheSummary:
    def test_it_counts_every_finding_and_the_seasons_they_fall_in(self):
        rows = [
            {'season': 1950, 'team_raw': 'Army', 'findings': '',
             'reconciled': True},
            {'season': 1950, 'team_raw': 'Navy',
             'findings': 'regular_game_count_mismatch', 'reconciled': False},
            {'season': 1951, 'team_raw': 'Yale',
             'findings': 'no_reference_line;records_not_collected',
             'reconciled': False},
        ]
        summary = reconciler.summarize(rows, 1950, 1951)
        assert summary['team_seasons'] == 3
        assert summary['reconciled'] == 1
        assert summary['with_findings'] == 2
        assert summary['findings'] == {'no_reference_line': 1,
                                       'records_not_collected': 1,
                                       'regular_game_count_mismatch': 1}
        assert summary['seasons_with_findings'] == {'1950': 1, '1951': 1}
        assert summary['production_promotion_allowed'] is False

    def test_it_says_the_two_feeds_are_the_same_provider(self):
        summary = reconciler.summarize([], 1946, 1979)
        assert 'collegefootballdata.com' in summary['note']


class TestTheEntryPoint:
    def test_offline_it_writes_both_reports_from_what_is_stored(
            self, archive, tmp_path, monkeypatch, capsys):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        records = sqlite3.connect(tmp_path / 'records.db')
        reconciler.collect(records, 1950, 1950, 'key', pause=0,
                           fetcher=lambda year, key: [
                               record('Army', regular=(1, 1, 0, 0)),
                               record('Navy', regular=(1, 0, 1, 0))])
        records.close()
        out = main_offline(archive, tmp_path, monkeypatch)
        rows = list(csv.DictReader(
            (out / 'source_reconciliation.csv').open()))
        assert {row['team_raw'] for row in rows} == {'Army', 'Navy'}
        assert all(row['findings'] == '' for row in rows)
        summary = json.loads(
            (out / 'source_reconciliation_summary.json').read_text())
        assert summary['reconciled'] == 2
        assert summary['production_promotion_allowed'] is False
        assert json.loads(capsys.readouterr().out.split('Offline')[1]
                          .split('\n', 1)[1])['team_seasons'] == 2

    def test_offline_it_asks_the_provider_for_nothing(
            self, archive, tmp_path, monkeypatch):
        game(archive, 1, 'Army', 'Navy', 21, 0)
        monkeypatch.delenv('CFBD_API_KEY', raising=False)

        def refuse(*args, **kwargs):
            raise AssertionError('offline must not reach the provider')

        monkeypatch.setattr(reconciler, 'fetch', refuse)
        out = main_offline(archive, tmp_path, monkeypatch)
        rows = list(csv.DictReader(
            (out / 'source_reconciliation.csv').open()))
        # No records collected, so every line says exactly that.
        assert all('records_not_collected' in row['findings'] for row in rows)

    def test_the_records_may_not_be_written_into_the_staging_archive(
            self, archive, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, 'argv', [
            'reconcile_historical_sources.py', '--staging', str(archive),
            '--db', str(archive), '--out', str(tmp_path / 'report'),
            '--offline'])
        with pytest.raises(SystemExit):
            reconciler.main()

    def test_a_staging_archive_that_is_not_there_is_an_error(
            self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, 'argv', [
            'reconcile_historical_sources.py',
            '--staging', str(tmp_path / 'nope.db'),
            '--db', str(tmp_path / 'records.db'),
            '--out', str(tmp_path / 'report'), '--offline'])
        with pytest.raises(SystemExit):
            reconciler.main()

    def test_without_offline_it_requires_the_key(self, archive, tmp_path,
                                                 monkeypatch):
        monkeypatch.delenv('CFBD_API_KEY', raising=False)
        monkeypatch.setattr(sys, 'argv', [
            'reconcile_historical_sources.py', '--staging', str(archive),
            '--db', str(tmp_path / 'records.db'),
            '--out', str(tmp_path / 'report')])
        with pytest.raises(SystemExit):
            reconciler.main()


class TestTheWorkflowThatRunsIt:
    """Read as data, like the backfill workflow's own tests."""

    STAGING_CHAIN = 'historical-staging-db'
    RECORDS_CHAIN = 'historical-records-db'

    def step(self, steps, fragment):
        for one in steps:
            if fragment.lower() in (one.get('name') or '').lower():
                return one
        raise AssertionError(f'no step named like {fragment!r}')

    def body(self, text, name):
        return text.split(name, 1)[1].split('- name:', 1)[0]

    def test_it_only_runs_when_asked(self, workflow):
        assert set(workflow[True] if True in workflow
                   else workflow['on']) == {'workflow_dispatch'}

    def test_a_queued_run_does_not_cancel_a_running_one(self, workflow):
        assert workflow['concurrency']['cancel-in-progress'] is False

    def test_it_does_not_share_the_backfill_concurrency_group(self, workflow):
        # The two can run at once: this one only reads the backfill's chain.
        assert workflow['concurrency']['group'] != 'historical-backfill'

    def test_it_never_republishes_the_staging_chain(self, steps):
        # The backfill is that chain's only writer. A second writer is how a
        # chain starts losing batches.
        for one in steps:
            if 'upload-artifact' in str(one.get('uses') or ''):
                assert one['with']['name'] != self.STAGING_CHAIN

    def test_the_records_are_a_chain_of_their_own_kept_on_failure(self, steps):
        keep = self.step(steps, 'keep the records')
        assert keep['if'] == 'always()'
        assert keep['with']['name'] == self.RECORDS_CHAIN
        assert keep['with']['if-no-files-found'] == 'error'

    def test_both_carries_sort_the_candidates(self, text):
        for name in ('Fetch the staged archive',
                     'Carry the collected records forward'):
            body = self.body(text, name)
            assert 'sort_by(.created_at)' in body, name
            assert 'per_page=1"' not in body, name
            assert 'per_page=1&' not in body, name

    def test_a_missing_staging_artifact_stops_the_run(self, text):
        body = self.body(text, 'Fetch the staged archive')
        assert 'No staging database artifact' in body
        # And it is guarded rather than chained, as in the backfill.
        assert 'if ! gh api' in body

    def test_the_reports_are_published_even_after_a_failure(self, steps):
        assert self.step(steps, 'publish the reconciliation')['if'] \
            == 'always()'
        assert self.step(steps, 'summarise')['if'] == 'always()'

    def test_every_piped_step_sets_pipefail(self, steps):
        for one in steps:
            script = one.get('run') or ''
            if '| tee' in script:
                assert 'set -o pipefail' in script, one.get('name')

    def test_it_can_read_the_repository_and_its_artifacts_only(self, workflow):
        assert workflow['permissions'] == {'contents': 'read',
                                           'actions': 'read'}

    def test_it_builds_no_rating_and_publishes_no_page(self, code):
        for forbidden in ('league.db', 'build_ratings', 'build_elo',
                          'run_pipeline', 'export_static_data', 'ui/',
                          'git commit', 'git push', 'contents: write'):
            assert forbidden not in code, forbidden

    def test_it_needs_no_dependency_install(self, code):
        assert 'pip install' not in code

    def test_offline_runs_without_the_key(self, text):
        body = self.body(text, 'Check the credential and the range')
        assert '"$OFFLINE" != "true"' in body
        assert 'CFBD_API_KEY repository secret is missing' in body

    def test_the_key_is_never_echoed(self, text):
        assert 'echo "$CFBD_API_KEY"' not in text
        assert text.count('secrets.CFBD_API_KEY') == 1

    def test_the_range_is_checked_before_the_provider_is_asked(self, steps,
                                                               text):
        names = [(one.get('name') or '').lower() for one in steps]
        check = next(i for i, name in enumerate(names) if 'range' in name)
        work = next(i for i, name in enumerate(names) if 'reconcile the' in name)
        assert check < work
        assert '1946 <= first <= last <= 1979' in self.body(
            text, 'Check the credential and the range')

    def test_it_calls_the_script_with_the_arguments_it_takes(self, text):
        assert 'scripts/reconcile_historical_sources.py' in text
        source = SCRIPT.read_text(encoding='utf-8')
        for flag in ('--staging', '--db', '--out', '--start', '--end',
                     '--pause', '--force', '--offline'):
            assert f'"{flag}"' in source, flag
