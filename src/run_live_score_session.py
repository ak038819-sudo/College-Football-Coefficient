#!/usr/bin/env python3
"""Keep the published scoreboard fresh from a chain of long-running jobs.

Why this exists
---------------
`live-scores.yml` asks GitHub for a refresh every five minutes across the game
windows. Measured over two weeks, GitHub created 22 of the 699 firings those
crons specify -- 3.1% (docs/live-data-options.md). The workflow is correct and
the job takes about 100 seconds; high-frequency scheduled workflows are simply
dropped under load. Nothing inside a workflow file can raise that rate.

A loop inside ONE job is not scheduled by GitHub at all. A run polling
in-process publishes on its own clock, and GitHub only has to start the job.
`live-score-session.yml` asks it to every four hours, all season, so the
sessions tile the day end to end.

Why tile the day rather than draw windows around the slate
----------------------------------------------------------
Because the slate's shape is not what anyone assumes it is. The first version
of this workflow ran on Thursday, Friday, Saturday and Sunday; 54 of the 761
kickoffs in 2026 are on other days, and a hand-drawn table that covered 2026
exactly covered 2023 badly, bowl season having games every weekday from early
afternoon. A session that no longer has to know when football is played cannot
be wrong about it.

What makes tiling an empty day cheap
------------------------------------
Two things, both here rather than in the schedule. A session whose whole
scoreboard is final exits after its first tick, which is most of a Tuesday in
October. A session watching games that have not kicked off yet polls at
`--idle-interval` instead of `--interval`: there is nothing to be fresh about
until somebody kicks off, and each tick costs three CFBD requests.

What this does NOT fix
----------------------
Publishing to `main` is still how the site updates, and a Pages site built from
a branch has a documented soft limit of about ten builds an hour. So the floor
on staleness is roughly six minutes, not six seconds, and `--interval` defaults
accordingly. The win is not seconds: it is going from a snapshot hours old,
with finished games still showing a running clock, to one that is minutes old
all day. Anything faster than Pages means moving off Pages, which is the
decision recorded in docs/live-data-options.md and not this script's business.

Each tick takes the same path a cron run takes, through
`fetch_live_scores.refresh`, so there is one definition of a published
snapshot. A tick that fails is logged and the session continues: the last good
snapshot stays published, and one bad response should not end the day.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import fetch_live_scores

DEFAULT_OUT = Path('ui/data/live_scores.json')
PUBLISH_SCRIPT = Path('scripts/publish_to_main.sh')
COMMIT_MESSAGE = 'Refresh live scoreboard [skip ci]'
# Pages builds from a branch are soft-limited to roughly ten an hour, so a
# shorter interval would spend publishes it cannot deliver.
DEFAULT_INTERVAL = 360
# Nothing on the scoreboard is moving, so this is not a staleness the page can
# show: it is how long a kickoff can go unnoticed, and the kickoff itself is
# anticipated rather than waited for (see `next_interval`). Three CFBD requests
# a tick is what makes it worth slowing down at all.
DEFAULT_IDLE_INTERVAL = 1200
# Four hours, and `live-score-session.yml` starts a session every four hours.
# The two numbers are one decision: sessions that do not meet leave a hole, and
# sessions that overlap queue behind each other, where GitHub keeps only one
# pending run per concurrency group and cancels the rest.
DEFAULT_DURATION = 240
# A game that has ended keeps its final score for the rest of the session; what
# decides whether there is still anything to watch is an unfinished game.
UNFINISHED = ('scheduled', 'in_progress')


def game_state(snapshot):
    """The parts of a snapshot a reader would notice changing.

    `updated_at` is deliberately excluded: `refresh` stamps it every tick, so a
    file comparison always differs and would publish on every poll regardless
    of whether a single score had moved. The clock IS included -- a running
    clock is the difference between a live game and a frozen one, which is the
    defect this whole change exists to fix.
    """
    games = snapshot.get('games') if isinstance(snapshot, dict) else None
    if not isinstance(games, list):
        return {}
    state = {}
    for game in games:
        if not isinstance(game, dict) or game.get('id') is None:
            continue
        state[str(game['id'])] = [
            game.get('status'), game.get('period'), game.get('clock'),
            (game.get('away') or {}).get('points'),
            (game.get('home') or {}).get('points'),
        ]
    return state


def unfinished_games(snapshot):
    """How many games are still scheduled or in progress."""
    games = snapshot.get('games') if isinstance(snapshot, dict) else None
    if not isinstance(games, list):
        return 0
    return sum(1 for g in games if isinstance(g, dict) and g.get('status') in UNFINISHED)


def next_interval(snapshot, now, interval=DEFAULT_INTERVAL, idle_interval=DEFAULT_IDLE_INTERVAL):
    """How long to wait before the next poll.

    Fast while a game is in progress, because that is the only time a reader
    would see the difference. Fast as well once a kickoff is close enough that
    an idle wait would swallow it: a game that starts in eight minutes must not
    be found twenty minutes into the first quarter, so a scheduled game whose
    start is inside the idle window pulls the cadence back up before it kicks
    off rather than after.

    A scheduled game whose start time cannot be read counts as imminent. The
    cost of being wrong that way is polling at the normal rate, which is what
    this function existed to slow down and therefore no worse than before it;
    the cost of the other way is a slate that silently never speeds up.
    """
    games = snapshot.get('games') if isinstance(snapshot, dict) else None
    if not isinstance(games, list):
        return interval
    if idle_interval <= interval:
        return interval
    horizon = now + dt.timedelta(seconds=idle_interval)
    for game in games:
        # Anything unreadable is treated the way an unreadable kickoff is: as a
        # reason to keep polling at the normal rate rather than to slow down on
        # the strength of a snapshot this function could not understand.
        if not isinstance(game, dict):
            return interval
        status = game.get('status')
        if status == 'in_progress':
            return interval
        if status != 'scheduled':
            continue
        # A kickoff that is absent, blank or not a timestamp all land in the
        # same place: `fromisoformat` refuses them and the except below treats
        # the game as imminent. There is no separate guard for None, because a
        # guard whose removal changes no answer is one no test can keep honest.
        start = game.get('start_date')
        try:
            kickoff = dt.datetime.fromisoformat(str(start).replace('Z', '+00:00'))
        except (TypeError, ValueError):
            return interval
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=dt.timezone.utc)
        if kickoff <= horizon:
            return interval
    return idle_interval


def publish(message=COMMIT_MESSAGE, out=DEFAULT_OUT):
    """Commit and push the snapshot, returning True when it was published."""
    result = subprocess.run([str(PUBLISH_SCRIPT), message, str(out)], check=False)
    return result.returncode == 0


def read_snapshot(out):
    try:
        return json.loads(Path(out).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def run_session(key, out=DEFAULT_OUT, interval=DEFAULT_INTERVAL,
                duration=DEFAULT_DURATION, refresh=None, publisher=None,
                sleep=None, now=None, stop_when_idle=True,
                idle_interval=DEFAULT_IDLE_INTERVAL):
    """Poll and publish until the slate is over or `duration` minutes elapse.

    `refresh`, `publisher`, `sleep` and `now` are injected so a test can drive
    the real loop without a network, a git remote or a wall clock. They are
    resolved here rather than in the signature's defaults: a default binds at
    definition time, so `sleep=time.sleep` could not be patched and a test that
    patched the module attribute would pass while waiting out the real delay.
    """
    refresh = refresh or fetch_live_scores.refresh
    publisher = publisher or publish
    sleep = sleep or time.sleep
    now = now or (lambda: dt.datetime.now(dt.timezone.utc))

    deadline = now() + dt.timedelta(minutes=duration)
    published = game_state(read_snapshot(out))
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))

    ticks = publishes = failures = fetch_failures = 0
    while not stopping:
        started = now()
        if started >= deadline:
            print(f'Session reached its {duration}-minute limit.')
            break
        ticks += 1
        try:
            snapshot = refresh(Path(out), key)
        except Exception as exc:  # noqa: BLE001 - one bad poll must not end the day
            failures += 1
            fetch_failures += 1
            print(f'Tick {ticks} failed, keeping the last good snapshot: {exc!r}')
            snapshot = None

        if snapshot is not None:
            state = game_state(snapshot)
            left = unfinished_games(snapshot)
            if state == published:
                print(f'Tick {ticks}: no change; {left} game(s) still unfinished.')
            elif publisher(COMMIT_MESSAGE, Path(out)):
                publishes += 1
                published = state
                print(f'Tick {ticks}: published; {left} game(s) still unfinished.')
            else:
                failures += 1
                print(f'Tick {ticks}: publish failed; will retry on the next tick.')

            # Only once the published snapshot agrees that nothing is left.
            # Stopping on a snapshot we failed to publish would leave the site
            # showing a running clock on a finished game -- the exact defect.
            if stop_when_idle and left == 0 and state == published:
                print('Every game on the scoreboard is final; ending the session.')
                break

        wait = next_interval(snapshot, started, interval, idle_interval) \
            if snapshot is not None else interval
        if wait != interval:
            print(f'Nothing live and no kickoff within {idle_interval}s; '
                  f'next poll in {wait}s.')
        remaining = wait - (now() - started).total_seconds()
        if remaining > 0 and not stopping:
            sleep(remaining)

    if stopping:
        print('Received SIGTERM; ending the session after the current tick.')
    print(f'Session finished: {ticks} tick(s), {publishes} publish(es), {failures} failure(s).')
    return {'ticks': ticks, 'publishes': publishes, 'failures': failures,
            'fetch_failures': fetch_failures}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--out', type=Path, default=DEFAULT_OUT)
    parser.add_argument('--interval', type=int, default=DEFAULT_INTERVAL,
                        help=f'seconds between polls while a game is live '
                             f'(default {DEFAULT_INTERVAL})')
    parser.add_argument('--idle-interval', type=int, default=DEFAULT_IDLE_INTERVAL,
                        help=f'seconds between polls when no game is live or about '
                             f'to start (default {DEFAULT_IDLE_INTERVAL})')
    parser.add_argument('--duration', type=int, default=DEFAULT_DURATION,
                        help=f'minutes to keep polling (default {DEFAULT_DURATION})')
    parser.add_argument('--keep-going-when-idle', action='store_true',
                        help='do not end the session once every game is final')
    args = parser.parse_args(argv)
    if args.interval < 1:
        parser.error('--interval must be at least 1 second')
    if args.idle_interval < args.interval:
        parser.error('--idle-interval cannot be shorter than --interval')
    if args.duration < 1:
        parser.error('--duration must be at least 1 minute')
    key = os.getenv('CFBD_API_KEY')
    if not key:
        parser.error('CFBD_API_KEY is required (live scoreboard requires a subscribed key)')
    summary = run_session(key, out=args.out, interval=args.interval, duration=args.duration,
                          idle_interval=args.idle_interval,
                          stop_when_idle=not args.keep_going_when_idle)
    # A session that never published anything is not a failure -- a quiet
    # Sunday has nothing to publish -- but one whose every FETCH failed reached
    # CFBD not once, and that should fail the run. A publish failure is counted
    # separately: it is already reported, and the next tick retries it.
    return 1 if summary['ticks'] and summary['ticks'] == summary['fetch_failures'] else 0


if __name__ == '__main__':
    sys.exit(main())
