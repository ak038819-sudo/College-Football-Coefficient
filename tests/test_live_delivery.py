import datetime as dt
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
