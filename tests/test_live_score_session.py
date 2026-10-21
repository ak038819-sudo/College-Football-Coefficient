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

    def __init__(self, snapshots, out, clock=None):
        self.snapshots = list(snapshots)
        self.out = out
        self.calls = 0
        self.clock = clock
        self.polled_at = []

    def __call__(self, path, key):
        self.calls += 1
        self.polled_at.append(self.clock.at if self.clock else None)
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


def scheduled(gid, start, **kwargs):
    return dict(game(gid, status='scheduled', period=None, clock=None,
                     away=None, home=None), start_date=start, **kwargs)


def run(out, snapshots, publisher=None, duration=60, interval=360, **kwargs):
    clock = Clock()
    feed = Feed(snapshots, out, clock)
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
    """60 minutes at a 360-second interval is ten ticks plus the one at the
    deadline: minute 0 through 54, then minute 60, and nothing after it."""
    summary, feed, publisher = run(out, [snapshot([game(1, clock=f'{i:02d}:00')])
                                         for i in range(30)], duration=60, interval=360)
    assert feed.calls == 11


def test_the_last_poll_lands_on_the_deadline(out):
    """The session that follows this one cannot cover the tail: it waits out
    this job's concurrency slot and its own checkout before its first poll. So
    a session must not exit holding a snapshot a whole interval old -- it cuts
    its last wait short and polls again as it finishes. Measured by the clock
    rather than by counting ticks, because the count is what a changed interval
    would alter and the deadline is what matters."""
    clock = Clock()
    feed = Feed([snapshot([game(1, clock=f'{i:02d}:00')]) for i in range(30)], out, clock)
    start = clock.at
    session.run_session('key', out=out, interval=360, duration=60, refresh=feed,
                        publisher=Publisher(), sleep=clock.sleep, now=clock.now)
    assert (clock.at - start).total_seconds() == 60 * 60, 'the session must run its full duration'
    assert feed.polled_at[-1] == start + dt.timedelta(minutes=60)
    assert feed.polled_at[-2] == start + dt.timedelta(minutes=54), 'the normal cadence before it'


def test_a_tick_slower_than_the_window_ends_the_session(out):
    """The deadline poll must not become a licence to over-run. A tick that
    takes longer than the time left has already passed the deadline, and the
    session stops rather than starting work the job may be killed inside."""
    clock = Clock()
    start = clock.at

    def slow(path, key):
        clock.at += dt.timedelta(minutes=70)
        Path(path).write_text(json.dumps(snapshot([game(1)])), encoding='utf-8')
        return snapshot([game(1)])

    summary = session.run_session('key', out=out, interval=360, duration=60, refresh=slow,
                                  publisher=Publisher(), sleep=clock.sleep, now=clock.now)
    assert summary['ticks'] == 1, 'the over-running tick is the last one'
    assert (clock.at - start).total_seconds() == 70 * 60, 'no sleep, and no second poll'


# ---- how often it polls ------------------------------------------------------

NOON = dt.datetime(2026, 10, 21, 12, 0, tzinfo=dt.timezone.utc)


def test_a_live_game_is_polled_at_the_fast_interval():
    assert session.next_interval(snapshot([game(1)]), NOON, 360, 1200) == 360


def test_a_finished_slate_is_polled_at_the_idle_interval():
    """A Tuesday in October: last week's finals and nothing else. Each tick is
    three CFBD requests spent on bytes that cannot have changed."""
    assert session.next_interval(snapshot([game(1, status='completed')]),
                                 NOON, 360, 1200) == 1200


def test_a_kickoff_inside_the_idle_window_pulls_the_cadence_back_up():
    """The cadence has to speed up BEFORE the kickoff. Deciding on the next
    tick would mean finding the game twenty minutes into the first quarter."""
    soon = scheduled(1, '2026-10-21T12:10:00.000Z')
    assert session.next_interval(snapshot([soon]), NOON, 360, 1200) == 360


def test_a_kickoff_beyond_the_idle_window_is_still_idle():
    later = scheduled(1, '2026-10-21T15:00:00.000Z')
    assert session.next_interval(snapshot([later]), NOON, 360, 1200) == 1200


def test_a_kickoff_time_that_cannot_be_read_counts_as_imminent():
    """Being wrong this way polls at the normal rate; being wrong the other way
    is a slate that silently never speeds up."""
    for value in (None, 'tomorrow', '', 2026):
        assert session.next_interval(snapshot([scheduled(1, value)]),
                                     NOON, 360, 1200) == 360, value


def test_a_naive_kickoff_is_read_as_utc_rather_than_crashing():
    assert session.next_interval(snapshot([scheduled(1, '2026-10-21T12:10:00')]),
                                 NOON, 360, 1200) == 360


def test_an_idle_interval_no_longer_than_the_fast_one_changes_nothing():
    assert session.next_interval(snapshot([game(1, status='completed')]),
                                 NOON, 360, 360) == 360
    assert session.next_interval(snapshot([game(1, status='completed')]),
                                 NOON, 360, 10) == 360


def test_a_malformed_snapshot_is_polled_at_the_fast_interval():
    for value in ({}, {'games': None}, None, {'games': [None, 'x']}):
        assert session.next_interval(value, NOON, 360, 1200) == 360, value


def test_the_loop_slows_down_over_a_finished_slate_it_keeps_watching(out):
    """Driven through the real loop, not the helper alone: a session told to
    keep going over a final slate must spend the idle interval between polls,
    which is what makes tiling an empty day affordable."""
    summary, feed, _ = run(out, [snapshot([game(1, status='completed')])],
                           duration=60, interval=360, idle_interval=1200,
                           stop_when_idle=False)
    assert feed.calls == 4, 'an hour at the idle interval is three polls plus the deadline'


def test_the_loop_stays_fast_while_a_game_is_live(out):
    summary, feed, _ = run(out, [snapshot([game(1, clock=f'{i:02d}:00')])
                                 for i in range(30)],
                           duration=60, interval=360, idle_interval=1200)
    assert feed.calls == 11


def test_a_failed_poll_is_retried_at_the_fast_interval(out):
    """A tick with no snapshot cannot know whether anything is live, and the
    thing to do about a failure is try again soon."""
    summary, feed, _ = run(out, [OSError('CFBD reset the connection')],
                           duration=60, interval=360, idle_interval=1200)
    assert feed.calls == 11


def test_the_defaults_pay_for_tiling_the_whole_season():
    """The idle interval has to be slower than the fast one or it buys nothing,
    and the fast one still has to respect the Pages build limit."""
    assert session.DEFAULT_IDLE_INTERVAL > session.DEFAULT_INTERVAL
    assert 3600 / session.DEFAULT_INTERVAL <= 10


def test_an_idle_interval_shorter_than_the_interval_is_refused(out, monkeypatch):
    """`run_session` is stubbed, and `--out` is given, for a reason measured
    here: without both, a build that stopped refusing the pair reached the real
    CFBD endpoint and the test HUNG for the length of the session instead of
    failing. A test that catches a mutation by never finishing is a test whose
    next reader deletes it."""
    monkeypatch.setenv('CFBD_API_KEY', 'test-key')
    started = []

    def stub(*args, **kwargs):
        started.append(kwargs)
        return {'ticks': 1, 'publishes': 0, 'failures': 0, 'fetch_failures': 0}

    monkeypatch.setattr(session, 'run_session', stub)
    with pytest.raises(SystemExit):
        session.main(['--out', str(out), '--interval', '600', '--idle-interval', '300'])
    assert started == [], 'an inverted pair must not reach the loop'
    session.main(['--out', str(out), '--interval', '600', '--idle-interval', '900'])
    assert started[0]['idle_interval'] == 900, 'the value must reach the loop'


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


def _session_starts():
    """Every UTC hour:minute a scheduled session starts, and on which days."""
    _, on = _session_workflow()
    starts = []
    for entry in on['schedule']:
        minute, hours, dom, months, dow = entry['cron'].split()
        assert '/' not in minute and minute != '*', entry['cron']
        assert '/' not in hours, entry['cron']
        for hour in hours.split(','):
            starts.append((int(hour), int(minute), months, dom, dow))
    return sorted(starts)


def test_the_session_is_started_by_a_cron_github_delivers():
    """A cron GitHub actually delivers. A firing more often than hourly would
    put this workflow in the same throttled class as the five-minute refresh it
    exists to replace -- but once a day was too rare in the other direction: a
    dropped firing cost the whole day's coverage. Four hours apart is both."""
    _, on = _session_workflow()
    assert 'workflow_dispatch' in on
    minutes = [hour * 60 + minute for hour, minute, *_ in _session_starts()]
    assert len(minutes) == len(set(minutes)), 'two sessions start at the same time'
    gaps = [b - a for a, b in zip(minutes, minutes[1:])]
    assert min(gaps) >= 60, 'starting more than hourly is the throttled class'


def test_the_sessions_tile_the_day_with_no_gap_and_no_overlap():
    """The defect this schedule replaced was a hole, not an error: games on a
    day no cron covered simply never reached the site, while last week's finals
    sat there looking correct. A session's duration is therefore exactly its
    spacing -- shorter leaves a hole, longer queues a run that GitHub cancels
    when the next one is already pending."""
    starts = _session_starts()
    minutes = [hour * 60 + minute for hour, minute, *_ in starts]
    spacing = [b - a for a, b in zip(minutes, minutes[1:])]
    spacing.append(24 * 60 - minutes[-1] + minutes[0])  # the wrap past midnight
    assert set(spacing) == {session.DEFAULT_DURATION}, (
        f'{sorted(set(spacing))} between starts, {session.DEFAULT_DURATION}-minute sessions')
    assert sum(spacing) == 24 * 60, 'the starts do not add up to a whole day'


def test_every_day_of_the_season_is_covered():
    """54 of the 761 kickoffs in 2026 are on a Monday, Tuesday, Wednesday or a
    Friday lunchtime, and the first version of this workflow ran Thursday
    through Sunday. Bowl season then puts games on every weekday. No row here
    may name days or hours of its own."""
    for hour, minute, months, dom, dow in _session_starts():
        assert dow == '*', f'{hour:02d}:{minute:02d} runs only on {dow}'
        assert dom == '*', f'{hour:02d}:{minute:02d} runs only on day {dom}'
        assert months == '8-12,1', f'{hour:02d}:{minute:02d} covers {months}'


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
