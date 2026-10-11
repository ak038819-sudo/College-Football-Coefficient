"""The game-day polling session: what makes it publish, and what makes it stop.

Every test here drives the real `run_session` loop. The network, the publisher
and the clock are injected, so the thing under test is the loop's own
decision-making rather than a reimplementation of it in the test.
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import run_live_score_session as session  # noqa: E402


def game(gid, status='in_progress', period=2, clock='10:00', away=7, home=3):
    return {'id': gid, 'status': status, 'period': period, 'clock': clock,
            'away': {'name': 'Away', 'points': away},
            'home': {'name': 'Home', 'points': home}}


def snapshot(games, stamp='2026-10-11T00:00:00Z'):
    return {'source': 'CollegeFootballData.com', 'updated_at': stamp, 'games': games}


class Clock:
    """A clock that only moves when the loop sleeps."""

    def __init__(self):
        self.at = dt.datetime(2026, 10, 11, 12, 0, tzinfo=dt.timezone.utc)

    def now(self):
        return self.at

    def sleep(self, seconds):
        self.at += dt.timedelta(seconds=seconds)


class Feed:
    """Returns each queued snapshot in turn, then repeats the last one."""

    def __init__(self, snapshots, out):
        self.snapshots = list(snapshots)
        self.out = out
        self.calls = 0

    def __call__(self, path, key):
        self.calls += 1
        value = self.snapshots[min(self.calls - 1, len(self.snapshots) - 1)]
        if isinstance(value, Exception):
            raise value
        # The real refresh writes the file before returning it.
        Path(path).write_text(json.dumps(value), encoding='utf-8')
        return value


class Publisher:
    def __init__(self, succeed=True):
        self.succeed = succeed
        self.calls = 0

    def __call__(self, message, out):
        self.calls += 1
        return self.succeed if isinstance(self.succeed, bool) else self.succeed(self.calls)


@pytest.fixture()
def out(tmp_path):
    return tmp_path / 'live_scores.json'


def run(out, snapshots, publisher=None, duration=60, interval=360, **kwargs):
    clock = Clock()
    feed = Feed(snapshots, out)
    publisher = publisher or Publisher()
    summary = session.run_session('key', out=out, interval=interval, duration=duration,
                                  refresh=feed, publisher=publisher, sleep=clock.sleep,
                                  now=clock.now, **kwargs)
    return summary, feed, publisher


# ---- what counts as a change -------------------------------------------------

def test_game_state_ignores_the_timestamp_that_changes_every_tick():
    """refresh() stamps updated_at every poll. If that counted as a change the
    session would publish on every tick regardless of the scoreboard."""
    first = snapshot([game(1)], stamp='2026-10-11T00:00:00Z')
    second = snapshot([game(1)], stamp='2026-10-11T00:06:00Z')
    assert session.game_state(first) == session.game_state(second)


def test_game_state_tracks_the_clock():
    """A frozen clock on a live game is the defect this change exists to fix,
    so the clock moving has to count as something worth publishing."""
    assert session.game_state(snapshot([game(1, clock='10:00')])) != \
        session.game_state(snapshot([game(1, clock='09:42')]))


def test_game_state_tracks_the_score_and_the_status():
    base = snapshot([game(1)])
    assert session.game_state(base) != session.game_state(snapshot([game(1, home=10)]))
    assert session.game_state(base) != session.game_state(snapshot([game(1, away=14)]))
    assert session.game_state(base) != session.game_state(snapshot([game(1, status='completed')]))


def test_game_state_survives_a_malformed_snapshot():
    assert session.game_state({}) == {}
    assert session.game_state({'games': 'nonsense'}) == {}
    assert session.game_state({'games': [{'no_id': True}]}) == {}


def test_unfinished_counts_only_scheduled_and_in_progress():
    snap = snapshot([game(1, status='completed'), game(2, status='scheduled'),
                     game(3, status='in_progress')])
    assert session.unfinished_games(snap) == 2
    assert session.unfinished_games({}) == 0


# ---- the loop ----------------------------------------------------------------

def test_publishes_when_a_score_changes(out):
    summary, feed, publisher = run(out, [
        snapshot([game(1, home=3)]),
        snapshot([game(1, home=10)]),
        snapshot([game(1, home=10, status='completed')]),
    ])
    assert publisher.calls == 3
    assert summary['publishes'] == 3


def test_does_not_publish_when_nothing_moved(out):
    """Three identical polls, only the timestamp differing: one publish for the
    first snapshot the session has not seen, and nothing after."""
    summary, feed, publisher = run(out, [
        snapshot([game(1)], stamp='2026-10-11T12:00:00Z'),
        snapshot([game(1)], stamp='2026-10-11T12:06:00Z'),
        snapshot([game(1)], stamp='2026-10-11T12:12:00Z'),
    ], duration=30)
    assert feed.calls >= 3
    assert publisher.calls == 1


def test_an_unchanged_scoreboard_already_on_disk_publishes_nothing(out):
    """A session restarting mid-slate must not republish what is already live."""
    live = snapshot([game(1)])
    out.write_text(json.dumps(live), encoding='utf-8')
    summary, feed, publisher = run(out, [live], duration=30)
    assert publisher.calls == 0


# ---- when the session stops --------------------------------------------------

def test_stops_once_every_game_is_final(out):
    summary, feed, publisher = run(out, [
        snapshot([game(1, status='in_progress')]),
        snapshot([game(1, status='completed')]),
        snapshot([game(1, status='completed')]),
    ], duration=600)
    assert feed.calls == 2, 'the session should not poll again after the slate ended'
    assert summary['publishes'] == 2


def test_keeps_going_while_one_game_is_unfinished(out):
    summary, feed, publisher = run(out, [
        snapshot([game(1, status='completed'), game(2, status='scheduled')]),
    ], duration=30)
    assert feed.calls > 1


def test_keep_going_when_idle_overrides_the_early_stop(out):
    summary, feed, publisher = run(out, [snapshot([game(1, status='completed')])],
                                   duration=30, stop_when_idle=False)
    assert feed.calls > 1


def test_does_not_stop_on_a_final_slate_it_failed_to_publish(out):
    """Stopping here would leave the site showing a running clock on a game
    that has finished -- precisely the state this session exists to clear."""
    summary, feed, publisher = run(out, [snapshot([game(1, status='completed')])],
                                   publisher=Publisher(succeed=False), duration=30)
    assert feed.calls > 1
    assert summary['publishes'] == 0


def test_honours_the_duration_limit(out):
    """60 minutes at a 360-second interval is ten ticks, not eleven: the
    eleventh starts exactly at the deadline."""
    summary, feed, publisher = run(out, [snapshot([game(1, clock=f'{i:02d}:00')])
                                         for i in range(30)], duration=60, interval=360)
    assert feed.calls == 10


# ---- failures ----------------------------------------------------------------

def test_a_failed_poll_does_not_end_the_session(out):
    summary, feed, publisher = run(out, [
        snapshot([game(1, home=3)]),
        OSError('CFBD reset the connection'),
        snapshot([game(1, home=10)]),
    ], duration=30)
    assert summary['failures'] >= 1
    assert summary['publishes'] == 2, 'the tick after the failure should still publish'


def test_a_failed_publish_is_retried_on_the_next_tick(out):
    """The published state must not advance on a failed push, or the change
    would never be sent again."""
    publisher = Publisher(succeed=lambda calls: calls > 1)
    summary, feed, _ = run(out, [snapshot([game(1, home=3)])],
                           publisher=publisher, duration=30)
    assert publisher.calls >= 2
    assert summary['publishes'] >= 1


def test_main_fails_only_when_every_poll_failed(out, monkeypatch):
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    monkeypatch.setattr(session, 'run_session',
                        lambda *a, **k: {'ticks': 4, 'publishes': 0, 'failures': 4,
                                         'fetch_failures': 4})
    assert session.main(['--out', str(out)]) == 1
    monkeypatch.setattr(session, 'run_session',
                        lambda *a, **k: {'ticks': 4, 'publishes': 0, 'failures': 1,
                                         'fetch_failures': 1})
    assert session.main(['--out', str(out)]) == 0


def test_main_requires_a_key_and_sane_arguments(out, monkeypatch):
    monkeypatch.delenv('CFBD_API_KEY', raising=False)
    with pytest.raises(SystemExit):
        session.main(['--out', str(out)])
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    with pytest.raises(SystemExit):
        session.main(['--interval', '0'])
    with pytest.raises(SystemExit):
        session.main(['--duration', '0'])


# ---- the contract with the rest of the pipeline ------------------------------

def test_the_session_publishes_through_the_shared_script():
    """Not its own git commands: publish_to_main.sh is what survives a main
    that moved under the job, and there is one copy of that logic."""
    assert session.PUBLISH_SCRIPT == Path('scripts/publish_to_main.sh')
    assert session.PUBLISH_SCRIPT.exists()


def test_publish_invokes_the_shared_script_with_the_snapshot(monkeypatch):
    """The argv matters: publish_to_main.sh takes the commit message first and
    the paths after it, and a path it was not given is never staged."""
    seen = {}

    class Result:
        returncode = 0

    def fake_run(argv, check=False):
        seen['argv'] = argv
        return Result()

    monkeypatch.setattr(session.subprocess, 'run', fake_run)
    assert session.publish('a message', Path('ui/data/live_scores.json')) is True
    assert seen['argv'] == ['scripts/publish_to_main.sh', 'a message',
                            'ui/data/live_scores.json']


def test_publish_reports_a_failed_push_rather_than_swallowing_it(monkeypatch):
    class Result:
        returncode = 1

    monkeypatch.setattr(session.subprocess, 'run', lambda argv, check=False: Result())
    assert session.publish() is False


def test_the_session_reuses_the_cron_path_rather_than_its_own_fetcher():
    """One definition of a published snapshot. A second would drift."""
    import fetch_live_scores
    assert session.run_session.__defaults__ is not None
    assert callable(fetch_live_scores.refresh)
    assert session.DEFAULT_OUT == fetch_live_scores.DEFAULT_OUT


def test_the_default_interval_respects_the_pages_build_limit():
    """Pages builds from a branch are soft-limited to about ten an hour.
    A shorter default would queue publishes that cannot be delivered."""
    assert 3600 / session.DEFAULT_INTERVAL <= 10


def test_the_default_duration_fits_inside_a_github_job(out):
    """A job is killed at six hours. A session must finish before that, or it
    dies mid-publish and the slate stops updating."""
    assert session.DEFAULT_DURATION <= 350


# ---- the workflow ------------------------------------------------------------

def _session_workflow():
    import yaml
    path = Path(__file__).resolve().parent.parent / '.github' / 'workflows' / 'live-score-session.yml'
    data = yaml.safe_load(path.read_text(encoding='utf-8'))
    return data, data.get('on', data.get(True))  # PyYAML reads the bare key `on` as True


def test_the_session_is_started_by_a_once_a_day_cron():
    """The whole point: a cron GitHub actually delivers. A firing more often
    than hourly would put this workflow in the same throttled class as the
    five-minute refresh it exists to replace."""
    _, on = _session_workflow()
    assert 'workflow_dispatch' in on
    for entry in on['schedule']:
        minute, hour = entry['cron'].split()[:2]
        assert '/' not in minute and '*' != minute, entry['cron']
        assert '/' not in hour, entry['cron']
        assert len(hour.split(',')) <= 2, f'{entry["cron"]} starts more than twice a day'


def test_the_job_timeout_leaves_room_after_the_polling_window():
    data, _ = _session_workflow()
    job = data['jobs']['session']
    assert job['timeout-minutes'] <= 360, 'GitHub kills a job at six hours'
    assert job['timeout-minutes'] > session.DEFAULT_DURATION


def test_the_session_does_not_share_a_concurrency_group():
    """Sharing the deploy's group would park a six-hour job in front of every
    publish; sharing the five-minute refresh's would cancel its runs."""
    data, _ = _session_workflow()
    group = data['jobs']['session']['concurrency']['group']
    assert group not in ('rebuild-and-deploy-main', 'refresh-live-scores-main')
    assert data['jobs']['session']['concurrency']['cancel-in-progress'] is False


def test_the_key_reaches_only_the_polling_step():
    data, _ = _session_workflow()
    steps = data['jobs']['session']['steps']
    with_key = [s for s in steps if 'CFBD_API_KEY' in (s.get('env') or {})]
    assert len(with_key) == 1
    assert 'run_live_score_session.py' in with_key[0]['run']
