"""The NBA live scoreboard workflow, read as data rather than trusted as prose.

Every assertion here is a rule this repository learned by breaking it.
"""

import os
from pathlib import Path

import pytest

yaml = pytest.importorskip('yaml')

ROOT = Path(__file__).parent.parent
WORKFLOW = ROOT / '.github/workflows/nba-live-scores.yml'
REFRESH = ROOT / '.github/workflows/nba-refresh.yml'


@pytest.fixture(scope='module')
def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding='utf-8'))


@pytest.fixture(scope='module')
def job(workflow):
    return workflow['jobs']['refresh']


def test_it_shares_no_other_pipeline_concurrency_group(job):
    group = job['concurrency']['group']
    assert group == 'nba-live-scores'
    # Not the deploy's, which would queue this behind a six-minute deploy;
    # not the nightly refresh's, which would swallow an evening of polls.
    assert group not in ('rebuild-and-deploy-main', 'nba-refresh')


def test_a_queued_poll_is_not_cancelled_by_the_next_one(job):
    assert job['concurrency']['cancel-in-progress'] is False


def test_the_job_is_capped_well_inside_its_own_interval(job):
    # It runs off a ten-minute cron. A job allowed to run longer would block
    # the polls behind it.
    assert job['timeout-minutes'] <= 5


def test_every_piped_step_sets_pipefail(job):
    # `cmd | tee` reports tee's status, so without this a crash passes.
    for step in job['steps']:
        script = step.get('run') or ''
        if '| tee' in script:
            assert 'set -o pipefail' in script, step.get('name')


def test_no_push_is_chained_behind_a_merge(job):
    # `git merge && git push` pushes a CONFLICTED tree: the push still runs
    # after the merge fails.
    for step in job['steps']:
        script = step.get('run') or ''
        assert 'merge' not in script or '&&' not in script, step.get('name')


def test_it_publishes_only_its_own_file(job):
    scripts = ' '.join(step.get('run') or '' for step in job['steps'])
    assert 'ui/data/nba_live.json' in scripts
    # A refresh must never carry a code change along with it.
    assert 'git add -A' not in scripts
    assert 'git add .' not in scripts


def test_it_does_not_write_inside_the_directory_the_refresh_replaces(job):
    scripts = ' '.join(step.get('run') or '' for step in job['steps'])
    assert 'ui/data/nba/' not in scripts


def test_it_needs_no_secret(workflow):
    assert 'secrets.' not in WORKFLOW.read_text(encoding='utf-8')


def test_it_runs_through_the_nba_season_on_both_sides_of_midnight_utc(workflow):
    crons = [entry['cron'] for entry in workflow[True]['schedule']]
    months = {entry.split()[3] for entry in crons}
    assert months == {'10,11,12,1,2,3,4,5,6'}
    hours = ' '.join(entry.split()[1] for entry in crons)
    # The evening slate runs from about 23:00 UTC to about 06:00 UTC.
    assert '22,23' in hours and '0-7' in hours


def test_the_nightly_refresh_has_more_than_one_chance_a_day():
    # GitHub defers scheduled runs under load; this one has started between
    # six and ten hours after its cron on every run. One cron a day means a
    # skipped run loses a whole day of results.
    refresh = yaml.safe_load(REFRESH.read_text(encoding='utf-8'))
    assert len(refresh[True]['schedule']) >= 2


def test_the_first_snapshot_publishes_although_git_has_never_seen_it(tmp_path):
    """The bootstrap, driven rather than asserted on the workflow's prose.

    `ui/data/nba_live.json` is UNTRACKED until the first run commits it, and
    `git diff` compares against the INDEX: it reports no change for a file
    git has never seen. A `git diff --quiet` guard in front of the publish
    therefore exits 0 on every scheduled run and the feed never bootstraps.
    This drives `publish_to_main.sh` on exactly that case.
    """
    import subprocess

    def git(*args, cwd):
        return subprocess.run(('git',) + args, cwd=cwd, check=True,
                              capture_output=True, text=True)

    remote = tmp_path / 'remote.git'
    subprocess.run(['git', 'init', '--bare', '-b', 'main', str(remote)],
                   check=True, capture_output=True)
    work = tmp_path / 'work'
    work.mkdir()
    git('init', '-b', 'main', cwd=work)
    git('config', 'user.email', 't@example.com', cwd=work)
    git('config', 'user.name', 'T', cwd=work)
    (work / 'seed').write_text('x', encoding='utf-8')
    git('add', 'seed', cwd=work)
    git('commit', '-m', 'seed', cwd=work)
    git('remote', 'add', 'origin', str(remote), cwd=work)
    git('push', 'origin', 'main', cwd=work)

    (work / 'ui' / 'data').mkdir(parents=True)
    snapshot = work / 'ui' / 'data' / 'nba_live.json'
    snapshot.write_text('{"games": []}', encoding='utf-8')
    # Exactly the state a scheduled run is in: the file exists on disk and
    # git has never heard of it.
    assert 'nba_live.json' in git('status', '--porcelain',
                                  '--untracked-files=all', cwd=work).stdout
    # And THE TRAP itself: `git diff` says nothing changed about a file it
    # has never seen, so a guard built on it would exit 0 right here and the
    # feed would never bootstrap.
    quiet = subprocess.run(['git', 'diff', '--quiet', '--',
                            'ui/data/nba_live.json'], cwd=work)
    assert quiet.returncode == 0

    script = str(ROOT / 'scripts' / 'publish_to_main.sh')
    env = dict(os.environ, PUBLISH_BRANCH='main', PUBLISH_ATTEMPTS='1')
    done = subprocess.run(['bash', script, 'Refresh [skip ci]',
                           'ui/data/nba_live.json'],
                          cwd=work, env=env, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    # It reached the remote, which is the only thing that publishes it.
    listed = subprocess.run(['git', 'ls-tree', '--name-only', '-r',
                             'main', 'ui/data/'], cwd=remote,
                            capture_output=True, text=True, check=True)
    assert 'ui/data/nba_live.json' in listed.stdout

    # And a second run with nothing changed commits nothing, which is what
    # the removed guard was there for.
    again = subprocess.run(['bash', script, 'Refresh [skip ci]',
                            'ui/data/nba_live.json'],
                           cwd=work, env=env, capture_output=True, text=True)
    assert again.returncode == 0, again.stderr
    assert 'Nothing changed' in again.stdout
