"""The NFL refresh workflow, read as data rather than trusted as prose.

Each assertion here is a rule this repository learned by breaking it:
a refresh that shares the deploy's concurrency group gets cancelled, a
`cmd | tee` without pipefail passes a failed step, a push chained behind a
merge sends a conflicted tree, and a refresh that commits more than its own
output eventually carries a code change with it.
"""

from pathlib import Path

import pytest

yaml = pytest.importorskip('yaml')

WORKFLOW = Path(__file__).parent.parent / '.github/workflows/nfl-refresh.yml'


@pytest.fixture(scope='module')
def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding='utf-8'))


@pytest.fixture(scope='module')
def text():
    return WORKFLOW.read_text(encoding='utf-8')


@pytest.fixture(scope='module')
def steps(workflow):
    return workflow['jobs']['refresh']['steps']


def test_it_does_not_share_the_deploy_concurrency_group(workflow):
    group = workflow['concurrency']['group']
    assert group == 'nfl-refresh'
    assert 'deploy' not in group


def test_a_queued_run_is_not_cancelled_by_the_next_one(workflow):
    assert workflow['concurrency']['cancel-in-progress'] is False


def test_every_piped_step_sets_pipefail(steps):
    for step in steps:
        script = step.get('run') or ''
        if '| tee' in script:
            assert 'set -o pipefail' in script, step.get('name')


def test_the_model_is_never_refitted_on_a_schedule(steps):
    # The comments may name fit_elo -- they explain why it is absent. No step
    # may run it.
    for step in steps:
        assert 'fit_elo' not in (step.get('run') or ''), step.get('name')


def test_the_export_is_built_somewhere_temporary_first(steps):
    build = next(step for step in steps
                 if 'src.nfl.export' in (step.get('run') or ''))
    assert 'RUNNER_TEMP' in build['run']


def test_the_publish_step_can_fail_the_run(steps):
    publish = next(step for step in steps if step.get('id') == 'publish')
    assert 'sys.exit(0 if' in publish['run']


def test_the_commit_step_names_only_the_export(steps):
    commit = next(step for step in steps
                  if (step.get('name') or '').startswith('Commit'))
    assert 'PATHS="ui/data/nfl"' in commit['run']
    assert 'git add $PATHS' in commit['run']


def test_the_push_is_never_chained_behind_a_merge(steps):
    commit = next(step for step in steps
                  if (step.get('name') or '').startswith('Commit'))
    script = commit['run']
    assert 'git rebase FETCH_HEAD' in script
    assert '&& git push' not in script


def test_a_fetch_failure_is_checked_explicitly(steps):
    # `set -e` does not stop a loop in a GitHub Actions shell. Every fetch and
    # push in the retry loop is therefore checked with an explicit `if !`.
    commit = next(step for step in steps
                  if (step.get('name') or '').startswith('Commit'))
    assert 'if ! git fetch' in commit['run']


def test_the_health_light_is_scoped_to_this_workflow(workflow):
    for job in ('alert-failure', 'alert-recovery'):
        script = workflow['jobs'][job]['steps'][-1]['run']
        assert '--scope nfl-refresh' in script


def test_it_needs_no_provider_secret(text):
    # The whole point of the nflverse source: no key, no allowance, nothing
    # to leak. A secret appearing here means the source changed.
    assert 'secrets.' not in text.replace('secrets.GITHUB_TOKEN', '')


def test_the_tests_run_before_anything_is_published(steps):
    names = [step.get('name') or '' for step in steps]
    validate = next(index for index, name in enumerate(names)
                    if name.startswith('Validate'))
    publish = next(index for index, name in enumerate(names)
                   if name.startswith('Decide whether'))
    assert validate < publish
