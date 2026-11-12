import datetime as dt
import re
from pathlib import Path

import yaml

from measure_live_delivery import wait_for_snapshot


def test_wait_ignores_old_pages_snapshot_until_new_one_is_visible():
    snapshots = iter([{'updated_at': 'old'}, {'updated_at': 'old'}, {'updated_at': 'new'}])
    clock = [0.0]
    def sleep(seconds):
        clock[0] += seconds
    visible, attempts = wait_for_snapshot('new', lambda: next(snapshots),
        lambda: dt.datetime(2026, 9, 28, tzinfo=dt.timezone.utc),
        lambda: clock[0], sleep, 20)
    assert attempts == 3
    assert clock[0] == 10
    assert visible.tzinfo == dt.timezone.utc


def test_wait_times_out_when_pages_keeps_old_snapshot():
    clock = [0.0]
    def sleep(seconds):
        clock[0] += seconds
    try:
        wait_for_snapshot('new', lambda: {'updated_at': 'old'},
                          lambda: dt.datetime.now(dt.timezone.utc), lambda: clock[0], sleep, 11)
    except TimeoutError:
        pass
    else:
        raise AssertionError('a stale Pages file was treated as published')
    assert clock[0] == 11


def test_live_workflow_covers_thursday_and_weekday_postseason_without_key_in_measurement():
    path = Path(__file__).resolve().parents[1] / '.github/workflows/live-scores.yml'
    workflow = yaml.safe_load(path.read_text(encoding='utf-8'))
    schedule = workflow.get('on', workflow.get(True))['schedule']
    crons = [item['cron'] for item in schedule]
    assert '2/5 19-23 * 8-11 4' in crons
    assert '7/15 15-23 * 12,1 *' in crons
    assert '7/15 0-7 * 12,1 *' in crons
    steps = workflow['jobs']['refresh']['steps']
    measure = next(s for s in steps if s.get('name') == 'Measure delivery to GitHub Pages')
    assert measure['continue-on-error'] is True
    assert 'secrets.CFBD_API_KEY' not in str(measure)


def _measure_step(workflow_name):
    path = Path(__file__).resolve().parents[1] / '.github/workflows' / workflow_name
    workflow = yaml.safe_load(path.read_text(encoding='utf-8'))
    job = next(iter(workflow['jobs'].values()))
    step = next(s for s in job['steps'] if s.get('name') == 'Measure delivery to GitHub Pages')
    return job, step


def _timeout_of(step):
    run = step['run']
    return float(run.split('--timeout')[1].split()[0])


def _dispatch_duration_cap():
    """The longest session a workflow_dispatch can actually obtain.

    Read out of the clamp in the workflow rather than hard-coded, so raising
    the clamp without re-balancing the job timeout fails this test.
    """
    path = Path(__file__).resolve().parents[1] / '.github/workflows/live-score-session.yml'
    text = path.read_text(encoding='utf-8')
    clamp = re.search(r'-gt (\d+) \]; then DURATION=(\d+); fi', text)
    assert clamp, 'the dispatch duration clamp is no longer recognisable'
    assert clamp.group(1) == clamp.group(2), 'the clamp bound and the value it assigns disagree'
    return int(clamp.group(1))


def test_session_measurement_window_clears_the_observed_publish_leg():
    """The 120s window sat inside the distribution it measured; see the Phase 1 audit.

    Pinned to literals rather than to a constant, so moving the constant cannot
    satisfy this on its own.
    """
    job, step = _measure_step('live-score-session.yml')
    timeout = _timeout_of(step)
    assert timeout >= 300, f'window {timeout}s cannot distinguish a slow publish from a failed one'
    # The job must be able to sit through the wait it asks for: the longest
    # session a dispatch can request, plus the measurement. GitHub kills a job
    # at 360 minutes, so the budget is balanced by capping the dispatch, not by
    # raising the job timeout past that ceiling.
    assert job['timeout-minutes'] <= 360, 'GitHub kills a job at six hours'
    longest_dispatch = _dispatch_duration_cap()
    assert job['timeout-minutes'] * 60 >= longest_dispatch * 60 + timeout, (
        'a maximum-duration dispatch would be killed at the job cap mid-measurement')


def test_five_minute_refresh_keeps_a_window_its_three_minute_job_can_finish():
    job, step = _measure_step('live-scores.yml')
    timeout = _timeout_of(step)
    assert job['timeout-minutes'] == 3
    assert timeout < job['timeout-minutes'] * 60, (
        'the measurement cannot outlast the job it runs in, or the run fails instead of reporting')


def test_timed_out_observation_is_not_reported_as_a_delivery_time(tmp_path, capsys, monkeypatch):
    """A timeout must record waited_s and leave visible_after_s null.

    Reading "we stopped looking after N seconds" as a delivery time is how the
    68%-unconfirmed sessions would turn into a falsely healthy p95.
    """
    import json as _json
    import measure_live_delivery as mld

    snapshot = tmp_path / 'live_scores.json'
    snapshot.write_text(_json.dumps({'updated_at': '2026-11-08T07:15:11.395792Z'}), encoding='utf-8')
    monkeypatch.setattr('sys.argv', ['measure_live_delivery.py', '--url', 'https://example.test/x.json',
                                     '--snapshot', str(snapshot), '--timeout', '1'])
    monkeypatch.setattr(mld, 'wait_for_snapshot',
                        lambda *a, **k: (_ for _ in ()).throw(TimeoutError('not visible')))
    mld.main()
    records = [line for line in capsys.readouterr().out.splitlines() if line.startswith('LIVE_DELIVERY ')]
    assert len(records) == 1
    record = _json.loads(records[0][len('LIVE_DELIVERY '):])
    assert record['visible_after_s'] is None
    assert record['waited_s'] == 1
