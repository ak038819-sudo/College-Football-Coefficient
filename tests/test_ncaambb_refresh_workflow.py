"""The basketball refresh, as configuration rather than as prose.

The plan's rule is that a basketball provider failure must not stop the
football refresh or replace valid data with empty files. Most of that is
enforced by the workflow's own wiring, so these tests read the parsed
workflow: which concurrency group it takes, which health light it lights, and
whether it could ever refit the model on a schedule.
"""

from pathlib import Path
import sys

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[1]
REFRESH = ROOT / '.github/workflows/cbb-refresh.yml'
DEPLOY = ROOT / '.github/workflows/ci-and-deploy.yml'


def workflow(path=REFRESH):
    return yaml.safe_load(path.read_text(encoding='utf-8'))


def steps():
    return workflow()['jobs']['refresh']['steps']


def script():
    return '\n'.join(step.get('run', '') for step in steps())


def test_the_refresh_does_not_share_the_deploys_concurrency_group():
    """A refresh that shared it would queue behind a deploy and then be
    cancelled by the next one. That is what happened to live-scores.yml."""
    deploy = workflow(DEPLOY)['jobs']['rebuild-and-deploy']
    assert workflow()['concurrency']['group'] != deploy['concurrency']['group']
    assert workflow()['concurrency']['cancel-in-progress'] is False


def test_the_refresh_touches_no_football_job():
    text = REFRESH.read_text(encoding='utf-8')
    for football in ('ci-and-deploy', 'run_pipeline', 'build_dashboard',
                     'CFBD_API_KEY', 'rebuild-and-deploy-main'):
        assert football not in text, f'{football} must not appear'


def test_the_model_is_never_refitted_on_a_schedule():
    """fit_elo chooses the parameters and a person commits them. A refresh
    that refitted would change what the ratings mean on a schedule nobody is
    watching."""
    assert 'fit_elo' not in script()
    assert 'build_ratings' in script()


def test_the_export_is_built_somewhere_other_than_the_published_directory():
    """Building straight into ui/data/ncaambb would destroy the live export
    before knowing whether the new one is fit to publish."""
    build = next(step for step in steps()
                 if 'src.ncaambb.export' in step.get('run', ''))
    assert '--out' in build['run']
    assert 'ui/data/ncaambb' not in build['run']


def test_the_publish_decision_runs_the_real_module():
    decide = next(step for step in steps() if step.get('id') == 'publish')
    assert 'from src.ncaambb import publish' in decide['run']
    assert 'publish.plan(' in decide['run'] and 'publish.apply(' in decide['run']


def test_a_refused_export_fails_the_run_rather_than_passing_quietly():
    decide = next(step for step in steps() if step.get('id') == 'publish')
    assert "sys.exit(0 if decision['publish'] else 1)" in decide['run']


def test_the_commit_step_only_runs_when_something_was_published():
    commit = next(step for step in steps()
                  if step.get('name', '').startswith('Commit'))
    assert commit['if'] == "steps.publish.outputs.published == 'true'"


def test_the_commit_adds_only_the_export():
    commit = next(step for step in steps()
                  if step.get('name', '').startswith('Commit'))
    assert 'git add ui/data/ncaambb' in commit['run']
    assert 'git add -A' not in commit['run']
    assert 'git add .' not in commit['run']


def test_the_refresh_pushes_back_to_the_branch_it_ran_on():
    """A refresh that always pushed to main could not be rehearsed on a
    branch, and rehearsing it is how it was proved before release."""
    commit = next(step for step in steps()
                  if step.get('name', '').startswith('Commit'))
    assert 'GITHUB_REF_NAME' in commit['run']
    assert 'HEAD:main' not in commit['run']


def test_the_push_is_never_chained_behind_a_rebase():
    """`git rebase && git push` sends a conflicted tree, because the push
    still runs after the rebase fails. This repository has shipped that."""
    commit = next(step for step in steps()
                  if step.get('name', '').startswith('Commit'))
    assert 'rebase' in commit['run'] and '&& git push' not in commit['run']
    assert 'git rebase --abort' in commit['run']


def test_every_pipeline_step_turns_on_pipefail():
    """`cmd | tee` reports tee's status. A crash in the ingestion passed its
    step exactly once because of this."""
    piped = [step for step in steps() if '| tee' in step.get('run', '')]
    assert piped, 'the summary steps are the ones this protects'
    for step in piped:
        assert 'set -o pipefail' in step['run'], step.get('name')


def test_the_api_key_comes_only_from_the_repository_secret():
    text = REFRESH.read_text(encoding='utf-8')
    assert 'secrets.CBBD_API_KEY' in text
    # No echo, no printing, no writing it anywhere.
    assert 'echo $CBBD' not in text and 'echo "$CBBD' not in text


# --- the health light -------------------------------------------------------

def test_the_basketball_alert_is_scoped_to_itself():
    """A basketball failure that closed football's alert would hide a site
    that is not publishing."""
    for job in ('alert-failure', 'alert-recovery'):
        run = '\n'.join(step.get('run', '')
                        for step in workflow()['jobs'][job]['steps'])
        assert '--scope cbb-refresh' in run
        assert 'alert_pipeline_state.py' in run


def test_the_failure_alert_fires_only_on_failure_and_the_recovery_on_success():
    jobs = workflow()['jobs']
    assert "needs.refresh.result == 'failure'" in jobs['alert-failure']['if']
    assert "needs.refresh.result == 'success'" in jobs['alert-recovery']['if']
    # always(), or a failed refresh would skip the job meant to report it.
    for job in ('alert-failure', 'alert-recovery'):
        assert 'always()' in jobs[job]['if']


def test_the_failure_alert_says_football_is_unaffected():
    run = '\n'.join(step.get('run', '')
                    for step in workflow()['jobs']['alert-failure']['steps'])
    assert 'Football is unaffected' in run


def test_the_workflow_may_write_issues_and_contents():
    permissions = workflow()['permissions']
    assert permissions['issues'] == 'write'
    assert permissions['contents'] == 'write'


# --- the season it refreshes ------------------------------------------------

@pytest.mark.parametrize('month,year,expected', [
    (11, 2026, 2027), (12, 2026, 2027), (7, 2026, 2027),
    (1, 2027, 2027), (4, 2027, 2027), (6, 2027, 2027),
])
def test_the_latest_season_is_the_ending_year(month, year, expected):
    """A season tipping off in November 2026 is 2027 everywhere: the label,
    the route and the database. Nothing infers it from the calendar year."""
    from datetime import datetime, timezone
    from src.ncaambb.identity import current_season, seasons_through
    now = datetime(year, month, 15, tzinfo=timezone.utc)
    assert current_season(now) == expected
    assert seasons_through(now)[-1] == expected
    assert seasons_through(now)[0] == 2015


def test_the_refresh_reads_the_season_rule_from_the_shared_helper():
    """A copy of the rule written into the workflow is a copy no test can
    reach, and this project has been bitten by a season rule that drifted."""
    step = next(s for s in steps() if s.get('id') == 'seasons')
    assert 'from src.ncaambb.identity import seasons_through' in step['run']
    assert 'now.month' not in step['run']
