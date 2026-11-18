"""The probe's job is to tell three states apart: a season with games, a
season the provider answered empty, and a season the provider failed on.

Collapsing any two of those is the failure these tests exist to catch,
because an outage read as a gap would send someone looking for records
that were never missing.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts' / 'probe_early_football_coverage.py'
sys.path.insert(0, str(ROOT / 'scripts'))

import probe_early_football_coverage as probe  # noqa: E402


def game(gid=1, season=1890, **overrides):
    body = {
        'id': gid,
        'season': season,
        'seasonType': 'regular',
        'startDate': f'{season}-11-01T15:00:00.000Z',
        'homeTeam': 'Yale',
        'awayTeam': 'Princeton',
        'homePoints': 10,
        'awayPoints': 6,
        'neutralSite': False,
        'homeClassification': 'fbs',
        'awayClassification': 'fbs',
        'homeConference': 'Ivy',
        'awayConference': 'Ivy',
        'venue': None,
    }
    body.update(overrides)
    return body


def feed(answers):
    """A fetcher driven by {(season, season_type): games or Exception}."""
    def fetcher(year, kind, key, **_):
        answer = answers.get((year, kind), [])
        if isinstance(answer, Exception):
            raise answer
        return answer
    return fetcher


def run(answers, first=1890, last=1890):
    return probe.probe(first, last, key='k', fetcher=feed(answers),
                       pause=0, sleeper=lambda _: None)


class TestItTellsTheThreeStatesApart:
    def test_a_season_with_games_is_reported_as_such(self):
        rows, *_ = run({(1890, 'regular'): [game(), game(2)]})
        summary = probe.summarize(*run(
            {(1890, 'regular'): [game(), game(2)]}), 1890, 1890)
        assert summary['seasons'] == {'requested': 1, 'with_games': 1,
                                      'empty': 0, 'incomplete': 0}
        assert summary['games'] == 2
        assert all(row['status'] == 'complete' for row in rows)

    def test_an_empty_season_is_a_gap_to_confirm_not_a_season_without_football(
            self):
        summary = probe.summarize(*run({}), 1890, 1890)
        assert summary['empty_seasons'] == [1890]
        assert summary['seasons']['empty'] == 1
        assert 'not evidence that no games were played' in summary['note']

    def test_a_failed_season_is_neither_empty_nor_present(self):
        answers = {(1890, 'regular'): RuntimeError('503'),
                   (1890, 'postseason'): []}
        summary = probe.summarize(*run(answers), 1890, 1890)
        assert summary['incomplete_seasons'] == [1890]
        # The half-answer must not make it look like a confirmed gap.
        assert summary['empty_seasons'] == []
        assert summary['seasons']['empty'] == 0
        assert summary['requests']['failed'] == 1

    def test_one_failure_does_not_stop_the_other_seasons(self):
        answers = {(1890, 'regular'): RuntimeError('boom'),
                   (1891, 'regular'): [game(1, 1891)]}
        rows, *_ = run(answers, 1890, 1891)
        assert len(rows) == 4
        summary = probe.summarize(*run(answers, 1890, 1891), 1890, 1891)
        assert summary['games'] == 1
        assert summary['incomplete_seasons'] == [1890]
        assert summary['empty_seasons'] == [1891] or \
            summary['first_season_with_games'] == 1891

    def test_a_failure_records_its_error(self):
        rows, *_ = run({(1890, 'regular'): RuntimeError('rate limited')})
        failed = [row for row in rows if row['status'] == 'failed']
        assert len(failed) == 1
        assert 'rate limited' in failed[0]['error']
        assert failed[0]['records'] == 0


class TestItCountsTheShapeOfAnAnswer:
    def test_scores_dates_and_ties(self):
        games = [
            game(1, homePoints=6, awayPoints=6),
            game(2, homePoints=None),
            game(3, startDate=None),
        ]
        counts, *_ = probe.shape(games, 1890)
        assert counts['records'] == 3
        assert counts['with_both_scores'] == 2
        assert counts['ties'] == 1
        assert counts['with_date'] == 2

    def test_a_missing_score_is_not_a_tie(self):
        counts, *_ = probe.shape(
            [game(1, homePoints=None, awayPoints=None)], 1890)
        assert counts['ties'] == 0
        assert counts['with_both_scores'] == 0

    def test_neutral_site_is_counted_three_ways(self):
        games = [game(1, neutralSite=True), game(2, neutralSite=False),
                 game(3, neutralSite=None)]
        counts, *_ = probe.shape(games, 1890)
        assert (counts['neutral_true'], counts['neutral_false'],
                counts['neutral_unknown']) == (1, 1, 1)

    def test_venues_and_classifications(self):
        games = [game(1, venue='Yale Field'),
                 game(2, homeClassification='', awayClassification=None)]
        counts, *_ = probe.shape(games, 1890)
        assert counts['with_venue'] == 1
        assert counts['with_home_classification'] == 1
        assert counts['with_away_classification'] == 1

    def test_schools_are_collected_from_both_sides(self):
        games = [game(1), game(2, homeTeam='Harvard', awayTeam='Yale')]
        counts, names, labels, leagues = probe.shape(games, 1890)
        assert names == {'Yale', 'Princeton', 'Harvard'}
        assert counts['distinct_schools'] == 3
        assert labels == {'fbs'}
        assert leagues == {'Ivy'}

    def test_duplicate_and_missing_ids_are_counted(self):
        games = [game(1), game(1), game(None), {'season': 1890}]
        counts, *_ = probe.shape(games, 1890)
        assert counts['duplicate_ids'] == 1
        assert counts['missing_id'] == 2

    def test_a_season_the_payload_disagrees_with_is_counted(self):
        counts, *_ = probe.shape([game(1, season=1999)], 1890)
        assert counts['season_disagrees'] == 1

    def test_a_payload_silent_about_the_season_is_not_a_disagreement(self):
        body = game(1)
        del body['season']
        counts, *_ = probe.shape([body], 1890)
        assert counts['season_disagrees'] == 0


class TestTheSummaryAddsUp:
    def test_totals_come_only_from_complete_requests(self):
        answers = {(1890, 'regular'): [game(1), game(2)],
                   (1891, 'regular'): RuntimeError('boom')}
        summary = probe.summarize(*run(answers, 1890, 1891), 1890, 1891)
        assert summary['games'] == 2
        assert summary['requests'] == {'made': 4, 'complete': 3, 'failed': 1}

    def test_the_first_and_last_seasons_with_games_are_reported(self):
        answers = {(1891, 'regular'): [game(1, 1891)],
                   (1893, 'regular'): [game(2, 1893)]}
        summary = probe.summarize(*run(answers, 1890, 1893), 1890, 1893)
        assert summary['first_season_with_games'] == 1891
        assert summary['last_season_with_games'] == 1893
        assert summary['empty_seasons'] == [1890, 1892]

    def test_nothing_is_staged_or_promoted(self):
        summary = probe.summarize(*run({}), 1890, 1890)
        assert summary['staged'] is False
        assert summary['production_promotion_allowed'] is False

    def test_an_archive_with_no_games_at_all_says_so_without_guessing(self):
        summary = probe.summarize(*run({}, 1890, 1892), 1890, 1892)
        assert summary['first_season_with_games'] is None
        assert summary['games'] == 0
        assert summary['empty_seasons'] == [1890, 1891, 1892]


class TestTheRangeIsPhaseFours:
    @pytest.mark.parametrize('first,last', [(1868, 1945), (1869, 1946),
                                            (1900, 1890)])
    def test_outside_the_range_is_refused(self, first, last):
        with pytest.raises(ValueError):
            run({}, first, last)

    def test_it_stops_short_of_phase_threes_archive(self):
        # 1946 onward is already staged and audited; re-probing it here
        # would invite two answers to one question.
        assert probe.LAST == 1945


class TestItWritesItsReportAndRefusesToLie:
    def test_main_writes_three_files(self, tmp_path, monkeypatch, capsys):
        answers = {(1890, 'regular'): [game(1), game(2, homeTeam='Harvard')]}
        monkeypatch.setenv('CFBD_API_KEY', 'k')
        monkeypatch.setattr(probe, 'fetch', feed(answers))
        out = tmp_path / 'report'
        monkeypatch.setattr(sys, 'argv', [
            'probe', '--out', str(out), '--start', '1890', '--end', '1890',
            '--pause', '0'])
        probe.main()
        summary = json.loads((out / 'early_coverage_summary.json').read_text())
        assert summary['games'] == 2
        rows = list(csv.DictReader((out / 'early_coverage.csv').open()))
        assert len(rows) == 2
        assert {row['season_type'] for row in rows} == {'regular',
                                                        'postseason'}
        schools = (out / 'early_schools.csv').read_text().splitlines()
        assert schools[0] == 'raw_name'
        assert sorted(schools[1:]) == ['Harvard', 'Princeton', 'Yale']
        assert '"staged": false' in capsys.readouterr().out

    def test_a_partial_probe_exits_nonzero_but_still_reports(self, tmp_path,
                                                             monkeypatch):
        monkeypatch.setenv('CFBD_API_KEY', 'k')
        monkeypatch.setattr(probe, 'fetch',
                            feed({(1890, 'regular'): RuntimeError('503')}))
        out = tmp_path / 'report'
        monkeypatch.setattr(sys, 'argv', [
            'probe', '--out', str(out), '--start', '1890', '--end', '1890',
            '--pause', '0'])
        with pytest.raises(SystemExit) as exc:
            probe.main()
        assert 'partial' in str(exc.value)
        # The report is written anyway: a failed probe is still evidence.
        assert (out / 'early_coverage_summary.json').exists()

    def test_it_refuses_without_a_key(self, tmp_path, monkeypatch, capsys):
        monkeypatch.delenv('CFBD_API_KEY', raising=False)
        monkeypatch.setattr(sys, 'argv', [
            'probe', '--out', str(tmp_path / 'out')])
        with pytest.raises(SystemExit):
            probe.main()
        assert 'CFBD_API_KEY is required' in capsys.readouterr().err

    def test_it_never_opens_a_database(self):
        source = SCRIPT.read_text()
        for forbidden in ('sqlite3', 'league.db', 'archive_games',
                          'build_elo', 'run_pipeline', 'ui/'):
            assert forbidden not in source


WORKFLOW = ROOT / '.github' / 'workflows' / 'early-football-probe.yml'


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
    return workflow['jobs']['probe']['steps']


class TestTheWorkflowThatRunsIt:
    def test_it_only_runs_when_asked(self, workflow):
        # 77 seasons of requests is not something to run on a schedule.
        assert set(workflow[True] if True in workflow
                   else workflow['on']) == {'workflow_dispatch'}

    def test_the_key_reaches_only_the_probe_step(self, steps):
        asked = [step for step in steps
                 if 'CFBD_API_KEY' in str(step.get('env') or {})]
        assert len(asked) == 1
        assert 'probe_early_football_coverage.py' in asked[0]['run']

    def test_the_key_is_never_printed(self, code):
        assert 'echo "$CFBD_API_KEY"' not in code
        assert '${{ secrets.CFBD_API_KEY }}' in code
        # One reference, on the one step that calls the provider.
        assert code.count('secrets.CFBD_API_KEY') == 1

    def test_it_can_only_read_the_repository(self, workflow):
        assert workflow['permissions'] == {'contents': 'read'}

    def test_it_stages_nothing_and_builds_nothing(self, code):
        for forbidden in ('historical-staging-db', 'import_historical_archive',
                          'run_pipeline.py', 'build_elo', 'league.db',
                          'export_static_data', 'git push'):
            assert forbidden not in code

    def test_the_range_is_checked_before_the_provider_is_asked(self, steps):
        names = [step.get('name', '') for step in steps]
        assert names.index('Check the range') < \
            names.index('Ask the provider what it has')

    def test_the_coverage_is_printed_not_only_filed(self, text):
        body = text.split('Lay the coverage out season by season', 1)[1]
        assert 'GITHUB_STEP_SUMMARY' in body
        assert "print('\\n'.join(lines))" in body

    def test_an_empty_season_is_never_called_a_season_without_football(
            self, text):
        assert 'to confirm against another' in text
        assert 'never answered, so' in text

    def test_the_report_survives_a_failed_probe(self, steps):
        # The probe exits non-zero on a partial run; the report is still
        # the evidence of how far it got.
        for step in steps:
            if 'upload-artifact' in str(step.get('uses') or ''):
                assert step.get('if') == 'always()'

    def test_it_is_not_cancelled_by_the_next_dispatch(self, workflow):
        assert workflow['concurrency']['cancel-in-progress'] is False
