"""The Phase 3 backfill workflow, read as data rather than trusted as prose.

The thing this workflow can get wrong is not a syntax error, it is losing
work: an import that stops on 1954 has already asked the provider for
1946-1953, and the only record of that is a staging database living on a
runner that is about to be deleted. So the assertions here are mostly about
what survives a failure, plus the standing rules this repository learned by
breaking them -- a `cmd | tee` without pipefail passes a failed step, and a
cancelled run loses whatever it had not checkpointed.

Nothing here runs the workflow. Its scripts have their own tests; this is
about the wiring between them.
"""

from pathlib import Path

import pytest

yaml = pytest.importorskip('yaml')

ROOT = Path(__file__).parent.parent
WORKFLOW = ROOT / '.github/workflows/historical-backfill.yml'
ARTIFACT = 'historical-staging-db'


@pytest.fixture(scope='module')
def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding='utf-8'))


@pytest.fixture(scope='module')
def text():
    return WORKFLOW.read_text(encoding='utf-8')


@pytest.fixture(scope='module')
def code(text):
    """The workflow with its comments stripped.

    The comments name the things this file must not do -- "no pip install",
    "`ui/` is untouched" -- so a test searching the whole text for those
    words finds the promise instead of a breach of it.
    """
    return '\n'.join(line for line in text.splitlines()
                     if not line.lstrip().startswith('#'))


@pytest.fixture(scope='module')
def steps(workflow):
    return workflow['jobs']['backfill']['steps']


def named(steps, fragment):
    for step in steps:
        if fragment.lower() in (step.get('name') or '').lower():
            return step
    raise AssertionError(f'no step named like {fragment!r}')


def uploads(steps):
    return [step for step in steps
            if 'upload-artifact' in str(step.get('uses') or '')]


class TestItOnlyRunsWhenAsked:
    def test_there_is_no_trigger_but_a_dispatch(self, workflow):
        # Thirty-four seasons of somebody else's API allowance is not
        # something to run on a push or a timer.
        assert set(workflow[True] if True in workflow
                   else workflow['on']) == {'workflow_dispatch'}

    def test_a_batch_is_dispatched_by_its_years(self, workflow):
        inputs = (workflow[True] if True in workflow
                  else workflow['on'])['workflow_dispatch']['inputs']
        assert {'start', 'end'} <= set(inputs)
        assert inputs['start']['default'] == '1948'
        assert inputs['end']['default'] == '1959'

    def test_a_queued_run_is_not_cancelled_by_the_next_one(self, workflow):
        # A cancelled import loses every season fetched since its last
        # artifact upload, and the provider has already served them.
        assert workflow['concurrency']['cancel-in-progress'] is False

    def test_it_does_not_share_the_deploy_concurrency_group(self, workflow):
        assert 'deploy' not in workflow['concurrency']['group']


class TestWhatSurvivesAFailure:
    def test_the_staging_database_is_uploaded_even_when_a_step_failed(
            self, steps):
        # THE WHOLE POINT. The run where this matters is the one that broke
        # halfway, and `if: always()` is the only thing that makes the next
        # dispatch a resume rather than a restart.
        keep = named(steps, 'keep the staging database')
        assert keep['if'] == 'always()'
        assert keep['with']['name'] == ARTIFACT

    def test_an_empty_upload_is_an_error_and_not_a_shrug(self, steps):
        keep = named(steps, 'keep the staging database')
        assert keep['with']['if-no-files-found'] == 'error'

    def test_the_audit_runs_on_what_was_staged_even_after_a_failure(
            self, steps):
        assert named(steps, 'audit')['if'] == 'always()'

    def test_the_restored_database_is_checked_rather_than_assumed(self, text):
        # A download that failed quietly looks exactly like a first run, and
        # a first run re-fetches every season the archive already holds.
        carry = 'Carry the staging database forward'
        body = text.split(carry, 1)[1].split('- name:', 1)[0]
        assert 'if ! gh api' in body
        assert 'holds no historical_staging.db' in body

    def test_every_piped_step_sets_pipefail(self, steps):
        for step in steps:
            script = step.get('run') or ''
            if '| tee' in script:
                assert 'set -o pipefail' in script, step.get('name')


class TestItCannotTouchProduction:
    def test_it_writes_no_production_database(self, code):
        assert 'league.db' not in code
        # The importer's own default lives in the repository; this workflow
        # stages inside the workspace so nothing can be committed by habit.
        assert 'db/historical_staging.db' not in code

    def test_it_commits_nothing_and_pushes_nothing(self, code):
        for forbidden in ('git commit', 'git push', 'contents: write'):
            assert forbidden not in code, forbidden

    def test_it_can_read_the_repository_and_its_own_artifacts_only(
            self, workflow):
        assert workflow['permissions'] == {'contents': 'read',
                                           'actions': 'read'}

    def test_it_builds_no_rating_and_publishes_no_page(self, code):
        for forbidden in ('build_ratings', 'build_elo', 'run_pipeline',
                          'export_static_data', 'ui/'):
            assert forbidden not in code, forbidden


class TestTheRangeIsCheckedBeforeTheProviderIs:
    def test_the_check_happens_before_the_import(self, steps):
        names = [(step.get('name') or '').lower() for step in steps]
        check = next(i for i, name in enumerate(names) if 'range' in name)
        import_at = next(i for i, name in enumerate(names)
                         if 'import the batch' in name)
        assert check < import_at

    def test_phase_three_is_the_only_range_it_will_take(self, text):
        body = text.split('Check the credential and the range', 1)[1]
        body = body.split('- name:', 1)[0]
        assert '1946 <= first <= last <= 1979' in body

    def test_a_missing_key_fails_the_run_rather_than_the_request(self, text):
        assert 'CFBD_API_KEY repository secret is missing' in text

    def test_the_key_is_never_echoed(self, text):
        # It reaches the importer through the environment and nowhere else.
        assert 'echo "$CFBD_API_KEY"' not in text
        assert text.count('secrets.CFBD_API_KEY') == 1


class TestItMatchesTheScriptsItCalls:
    def test_both_scripts_exist_and_take_the_arguments_it_passes(self, text):
        for script in ('scripts/import_historical_archive.py',
                       'scripts/audit_historical_backfill.py'):
            assert script in text
            source = (ROOT / script).read_text(encoding='utf-8')
            for flag in ('--db', '--start', '--end'):
                assert f'"{flag}"' in source, (script, flag)
        assert '"--out"' in (ROOT / 'scripts/audit_historical_backfill.py'
                             ).read_text(encoding='utf-8')
        assert '"--force"' in (ROOT / 'scripts/import_historical_archive.py'
                               ).read_text(encoding='utf-8')

    def test_it_needs_no_dependency_install(self, code, text):
        # Both scripts are standard library only, which is what lets this
        # run without the pipeline's bootstrap. An install step appearing
        # here means one of them grew an import that the runner would have
        # to be told about.
        assert 'pip install' not in code
        for script in ('scripts/import_historical_archive.py',
                       'scripts/audit_historical_backfill.py'):
            source = (ROOT / script).read_text(encoding='utf-8')
            for third_party in ('import requests', 'import pandas',
                                'import yaml', 'import pyarrow'):
                assert third_party not in source, (script, third_party)
