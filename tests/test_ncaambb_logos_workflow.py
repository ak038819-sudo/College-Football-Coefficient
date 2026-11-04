"""The club-marks workflow, as configuration rather than as prose.

This is the only place the mark fetch can run: the CDN it reads is blocked
from the development container. What matters is that it stays off the
provider's API allowance, stays off a schedule, and cannot publish over main
from a rehearsal on a branch.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MARKS = ROOT / '.github/workflows/cbb-logos.yml'
REFRESH = ROOT / '.github/workflows/cbb-refresh.yml'


def workflow():
    return yaml.safe_load(MARKS.read_text(encoding='utf-8'))


def steps():
    return workflow()['jobs']['marks']['steps']


def script():
    return '\n'.join(step.get('run', '') for step in steps())


def test_it_only_runs_when_a_person_asks():
    # yaml reads a bare `on:` as the boolean True, which is why this is not
    # spelled 'on'.
    triggers = workflow()[True]
    assert list(triggers) == ['workflow_dispatch']


def test_it_never_spends_the_provider_s_api_allowance():
    """The database is loaded from the committed cache, and nothing else.

    The marks live on a CDN that is not metered; the team feed behind them
    is, and this workflow has no business asking for it. The allowance is
    monthly and one people ingest is a third of it.
    """
    body = script()
    assert '--refetch ' not in body and not body.endswith('--refetch')
    assert 'CBBD_API_KEY' not in MARKS.read_text(encoding='utf-8')
    assert 'src.ncaambb.ingest --schedule-only' in body


def test_a_rehearsal_on_a_branch_stays_on_that_branch():
    # scripts/publish_to_main.sh pushes HEAD:main; this must not, or a
    # dispatch from a feature branch would land its unmerged source on main.
    body = script()
    assert 'HEAD:$GITHUB_REF_NAME' in body
    assert 'HEAD:main' not in body


def test_it_commits_only_the_marks():
    body = script()
    assert 'git add ui/assets/ncaambb' in body
    assert 'git add -A' not in body


def test_the_commit_skips_ci():
    # Hundreds of PNG files cannot change any test's answer, and the deploy
    # rebuilds and republishes the whole site.
    assert '[skip ci]' in script()


def test_the_fetch_is_the_module_and_not_a_copied_url():
    """A URL shape written into the workflow is a copy no test can reach."""
    assert 'python -m src.ncaambb.fetch_logos' in script()
    assert 'cdn.collegefootballdata.com' not in script()


# --- the refresh banks what it does not have --------------------------------
#
# A workflow_dispatch workflow can only be dispatched once it is on the
# DEFAULT branch, so cbb-logos.yml cannot run until this merges. The refresh
# banking anything missing is what gets the marks onto the site, and what
# keeps a newly Division I school from going unmarked until somebody
# remembers to dispatch.

def refresh():
    return yaml.safe_load(REFRESH.read_text(encoding='utf-8'))


def refresh_steps():
    return refresh()['jobs']['refresh']['steps']


def test_the_refresh_banks_any_mark_it_does_not_have():
    assert any('src.ncaambb.fetch_logos' in step.get('run', '')
               for step in refresh_steps())


def test_a_logo_cdn_having_a_bad_minute_cannot_stop_the_ratings():
    """Best effort, exactly as the people ingestion is.

    Before the people step was made best effort, a 429 on one season's box
    scores skipped the replay, the export and the publish. A logo is a
    smaller thing than that and must not cost more.
    """
    marks = [step for step in refresh_steps()
             if 'src.ncaambb.fetch_logos' in step.get('run', '')]
    assert marks and marks[0]['continue-on-error'] is True
    assert any(step.get('if', '').startswith("steps.marks.outcome")
               for step in refresh_steps()), 'and it says so when it fails'


def test_a_banked_mark_is_committed():
    """Or every run refetches the same 730 files for ever."""
    body = '\n'.join(step.get('run', '') for step in refresh_steps())
    assert 'ui/assets/ncaambb' in body
