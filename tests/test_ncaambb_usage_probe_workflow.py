"""The usage probe, as configuration rather than as prose.

Basketball cannot publish until the CBBD allowance recovers, and from outside
"the allowance is spent" and "the key is wrong" are the same HTTP 429. This
workflow is the one cheap way to tell them apart, so what matters is that it
stays cheap and stays unable to write anything: a diagnostic that could commit,
or that spent a season's quota, would not get dispatched when it is needed.
"""

from pathlib import Path
import subprocess
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / '.github/workflows/cbb-usage-probe.yml'
REFRESH = ROOT / '.github/workflows/cbb-refresh.yml'
DEPLOY = ROOT / '.github/workflows/ci-and-deploy.yml'


def workflow(path=PROBE):
    return yaml.safe_load(path.read_text(encoding='utf-8'))


def job():
    return workflow()['jobs']['usage']


def steps():
    return job()['steps']


def script():
    return '\n'.join(step.get('run', '') for step in steps())


def test_the_probe_cannot_write_anything():
    """It reads the provider and prints. No commit, no push, no data path."""
    assert workflow()['permissions'] == {'contents': 'read'}
    text = script()
    for forbidden in ('git commit', 'git push', 'git add', '--refetch'):
        assert forbidden not in text, forbidden
    checkout = next(s for s in steps() if str(s.get('uses', '')).startswith('actions/checkout'))
    assert checkout['with']['persist-credentials'] is False


def test_the_probe_asks_for_usage_and_nothing_else():
    """--usage-only is three requests. Without it the same script spends a
    burst, a cooldown and a paced run, which is the opposite of a probe you can
    afford to run while the allowance is already spent."""
    assert '--usage-only' in script()
    assert 'probe_cbbd_limits.py' in script()


def test_the_flag_the_workflow_passes_exists():
    """A workflow naming a flag the script does not have fails only when
    someone dispatches it, which is the one moment nobody wants a surprise."""
    out = subprocess.run([sys.executable, 'scripts/probe_cbbd_limits.py', '--help'],
                         cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert '--usage-only' in out.stdout


def test_the_probe_runs_in_its_own_concurrency_group():
    """A pending run that joins the deploy's or the refresh's group is
    cancelled by the next run to enter it, which is how a dispatched basketball
    job was lost once already."""
    groups = {workflow(REFRESH)['concurrency']['group'],
              workflow(DEPLOY)['jobs']['rebuild-and-deploy']['concurrency']['group']}
    assert job()['concurrency']['group'] not in groups
    assert job()['concurrency']['cancel-in-progress'] is False


def test_the_probe_is_only_ever_dispatched_by_hand():
    """It spends real requests, so it must not be on a schedule or a push.

    YAML reads a bare `on` as the boolean true, which is why the key is looked
    up as True rather than as a string -- the same shape every workflow here
    parses to."""
    assert set(workflow()[True]) == {'workflow_dispatch'}


def test_the_probe_never_prints_the_key():
    text = PROBE.read_text(encoding='utf-8')
    assert 'CBBD_API_KEY: ${{ secrets.CBBD_API_KEY }}' in text
    assert 'echo' not in script(), 'nothing in this job echoes, least of all the env'
