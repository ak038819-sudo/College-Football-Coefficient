"""Reducing the NBA play-by-play to what a game page shows.

**THE PLAYS THEMSELVES ARE NEVER STORED.** A season of ESPN play-by-play is
22 MB of Parquet and 642,472 rows for 1,325 games — 485 plays a game, 120 of
them scoring — and twenty-five seasons of it is about half a gigabyte. So
this module streams: a season is fetched, reduced, and the file deleted
before the next season is fetched. That is the same bargain `src/nfl/plays.py`
struck with the nflverse play-by-play and the football side struck when it
stored 441,307 drives rather than the plays behind them.

**THERE IS NO NBA DRIVE.** The NFL reduction is "drives and scoring plays"
because a drive is football's unit and the feed marks one. Basketball's unit
is the possession, and this feed does not mark possessions at all: there is no
column that says who had the ball, only who made each play. A reduction built
on a guessed possession boundary would be a derived quantity presented as a
stated one. So the four things stored are things the feed really does state:

    game_period   the line score, period by period, overtime included
    game_shape    lead changes, ties, biggest lead and biggest run each way,
                  and how long each side spent in front
    game_state    a thinned margin curve — the score at moments the clock
                  really showed, at most one per CURVE_SECONDS of game clock
    game_play     the handful of plays a game story would print, chosen by a
                  model-free rule (`key_plays`)

**OVERTIME SECONDS ARE PINNED AT 0, AND THE FEED DOES NOT DO THAT FOR YOU.**
`start_game_seconds_remaining` counts 2880 down to 0 across regulation and
then counts 300 down to 0 again in each overtime period, so a fourth-quarter
state and a first-overtime state can both read 240. `seconds_left` below is
regulation seconds remaining, which is what `src/win_probability.py` means by
it and is 0 throughout overtime; `elapsed` is the quantity that keeps running.

**THE SCORE COLUMNS INCLUDE THE PLAY.** Measured on 2026: of 159,072 scoring
plays, 159,069 carry a score that already contains their own points. So a
state read off a scoring play is the state AFTER it, which is what a curve
wants, and no off-by-one correction is applied.

Measured over the whole archive, 2002-2027 (31,230 games, 14,253,464 plays
read, 0.44 GB fetched and discarded):

| | |
| --- | --- |
| periods stored | 127,001 |
| curve points stored | 1,685,752 (54 a game) |
| key plays stored | 151,373 (4.8 a game) |
| overtime games | 1,850 (5.9%) |
| lead changes a game | 6.5 mean, 5 median, 43 most |
| games whose plays do not add up to the box score | 1,390 (4.5%) |
| of those, in 2002-2006 | 1,317 |

THE LAST TWO ROWS ARE A CASE THE PAGE HAS TO CARRY. The early ESPN feed's
play log is simply short of the final: three of those games read by hand each
end one to three points light on one side. `game_shape.reconciled` says so,
a game page then shows the box score's final and leaves the period detail
out, and `fit_win_probability.py` fits on none of them.

Five games of 2026 have no play-by-play at all — four postponed, and
401810469 (2026-01-20, a completed 110-138) simply has none published. A game
page says the plays are missing rather than drawing an empty flow.
"""

import argparse
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from . import source
from .identity import EXHIBITION_IDS, game_key as make_game_key, team_key
from .store import connect, record_snapshot
from .win_probability import PERIOD_SECONDS, REGULATION_PERIODS, \
    REGULATION_SECONDS

# An overtime period is five minutes.
OVERTIME_SECONDS = 300

# Only these columns are read. The file has 68.
COLUMNS = (
    'game_id', 'game_play_number', 'sequence_number', 'period_number',
    'clock_display_value', 'home_score', 'away_score', 'scoring_play',
    'score_value', 'team_id', 'type_text', 'text', 'athlete_name_1',
    'athlete_id_1', 'home_team_id', 'away_team_id',
)

# How much game clock may pass between two kept curve points. Forty-eight
# seconds is two shot clocks, which puts sixty points across a regulation
# game: enough for the curve to show every swing a viewer would notice and
# few enough that twenty-five seasons of curves is a few million rows rather
# than a few hundred million.
CURVE_SECONDS = 48

# `key_plays` thresholds. All three are stated here rather than buried in the
# rule, because they are the whole of the rule.
LATE_PERIOD = REGULATION_PERIODS      # a lead change counts as late from here
CLUTCH_SECONDS = 120                  # the closing minutes of the last period
CLUTCH_MARGIN = 6                     # two possessions
MAX_KEY_PLAYS = 20


def _int(value):
    if value is None or value is True or value is False:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _str(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clock_seconds(clock):
    """`11:45` and `0:48.7` as seconds. None when the feed does not say."""
    text = _str(clock)
    if not text:
        return None
    parts = text.split(':')
    if len(parts) == 1:
        parts = ['0'] + parts
    if len(parts) != 2:
        return None
    try:
        minutes, seconds = int(parts[0]), float(parts[1])
    except (TypeError, ValueError):
        return None
    if minutes < 0 or seconds < 0 or seconds >= 60:
        return None
    return int(round(minutes * 60 + seconds))


def period_length(period):
    """How long a period is. Overtime is five minutes, not twelve."""
    return PERIOD_SECONDS if period <= REGULATION_PERIODS else OVERTIME_SECONDS


def seconds_left(period, clock):
    """REGULATION seconds remaining, 0 throughout overtime.

    This is the quantity `src/win_probability.py` takes: the walk it runs
    ends at the end of regulation, and a game that reached overtime was
    level at that moment whatever the final score says.
    """
    if period is None or period < 1:
        return None
    if period > REGULATION_PERIODS:
        return 0
    inside = clock_seconds(clock)
    if inside is None:
        return None
    return (REGULATION_PERIODS - period) * PERIOD_SECONDS \
        + min(inside, PERIOD_SECONDS)


def elapsed_seconds(period, clock):
    """Game time played, which keeps running through overtime."""
    if period is None or period < 1:
        return None
    inside = clock_seconds(clock)
    if inside is None:
        return None
    length = period_length(period)
    done = min(inside, length)
    if period <= REGULATION_PERIODS:
        return (period - 1) * PERIOD_SECONDS + (length - done)
    overtimes = period - REGULATION_PERIODS - 1
    return REGULATION_SECONDS + overtimes * OVERTIME_SECONDS + (length - done)


def play_order(row):
    """The order the plays happened in.

    `game_play_number` is the feed's own index within the game and is the
    answer where it is present. `sequence_number` is the fallback: it is a
    string of digits in some seasons and an integer in others, so it is read
    as a number rather than sorted as text, which would put play 10 before
    play 9.
    """
    number = _int(row.get('game_play_number'))
    if number is not None:
        return number
    return _int(row.get('sequence_number')) or 0


def scoring_team(row, home_id, away_id, before):
    """Which side the points went to, read from the score itself.

    THE SCOREBOARD IS THE AUTHORITY, as it is in the NFL reduction and for
    the same reason: `team_id` names who made the play, which is not always
    who got the points. A goaltending call credits the shooter's team on a
    row whose team is the defence, and the archive carries rows with no team
    at all. The side whose total rose by exactly this play's points is the
    side that scored; `team_id` answers only when the delta does not name
    exactly one side.
    """
    points = _int(row.get('score_value')) or 0
    home, away = _int(row.get('home_score')), _int(row.get('away_score'))
    if before is not None and points and home is not None and away is not None:
        moved = (home - before[0] == points, away - before[1] == points)
        if moved[0] and not moved[1]:
            return home_id
        if moved[1] and not moved[0]:
            return away_id
    return _int(row.get('team_id'))


def group_games(rows):
    """The feed's rows as `{provider_id: [row, ...]}`, each in play order.

    Exhibition sides are dropped here rather than failing the run: the 2026
    file carries the All-Star mini-tournament, whose four games are not in
    the archive at all because `identity.team_key` refuses to make a
    franchise out of Team Stars.
    """
    games, dropped = {}, 0
    for row in rows:
        provider = _str(row.get('game_id'))
        if not provider:
            continue
        home, away = _int(row.get('home_team_id')), _int(row.get('away_team_id'))
        if home in EXHIBITION_IDS or away in EXHIBITION_IDS:
            dropped += 1
            continue
        games.setdefault(provider, []).append(row)
    for plays in games.values():
        plays.sort(key=play_order)
    return games, dropped


def _sign(value):
    return (value > 0) - (value < 0)


def reduce_game(provider, plays):
    """One game's plays as (periods, shape, states, key plays).

    Returns None when the rows carry no readable state at all, which is not
    the same as a game with no plays: a game the feed has never published
    simply does not appear here, and `export` says so on its page.
    """
    key = make_game_key(provider)
    home_id = next((_int(row.get('home_team_id')) for row in plays
                    if _int(row.get('home_team_id')) is not None), None)
    away_id = next((_int(row.get('away_team_id')) for row in plays
                    if _int(row.get('away_team_id')) is not None), None)
    periods, states, scored = {}, [], []
    shape = dict(game_key=key, plays=len(plays), scoring_plays=0, periods=0,
                 overtime=0, lead_changes=0, ties=0, home_biggest_lead=0,
                 away_biggest_lead=0, home_biggest_run=0, away_biggest_run=0,
                 home_time_leading=0, away_time_leading=0, time_tied=0)
    before = None
    last_elapsed, last_sign = 0, 0
    # The side that was last IN FRONT, which is not the same as the sign of
    # the previous state: a lead change passes through a tie, and `last_sign`
    # is 0 there. Counting changes off `last_sign` missed every lead change
    # that went 2-0, 2-2, 3-2 — which is most of them.
    last_leader = 0
    kept_at = None
    run_side, run_points = 0, 0
    readable = 0
    for row in plays:
        period = _int(row.get('period_number'))
        home, away = _int(row.get('home_score')), _int(row.get('away_score'))
        if period is None or period < 1 or home is None or away is None:
            continue
        elapsed = elapsed_seconds(period, row.get('clock_display_value'))
        secs = seconds_left(period, row.get('clock_display_value'))
        if elapsed is None or secs is None:
            continue
        readable += 1
        shape['periods'] = max(shape['periods'], period)
        margin = home - away
        # How long each side has been in front, charged to the state that
        # held between the previous readable play and this one.
        span = max(0, elapsed - last_elapsed)
        if last_sign > 0:
            shape['home_time_leading'] += span
        elif last_sign < 0:
            shape['away_time_leading'] += span
        else:
            shape['time_tied'] += span
        shape['home_biggest_lead'] = max(shape['home_biggest_lead'], margin)
        shape['away_biggest_lead'] = max(shape['away_biggest_lead'], -margin)
        leader = _sign(margin)
        if leader and last_leader and leader != last_leader:
            shape['lead_changes'] += 1
        if margin == 0 and last_elapsed and last_sign:
            shape['ties'] += 1
        if leader:
            last_leader = leader
        last_elapsed, last_sign = elapsed, leader

        points = _int(row.get('score_value')) or 0
        is_score = bool(row.get('scoring_play')) and points > 0
        if is_score:
            shape['scoring_plays'] += 1
            side = scoring_team(row, home_id, away_id, before)
            which = 1 if side == home_id else (-1 if side == away_id else 0)
            if which and which == run_side:
                run_points += points
            elif which:
                run_side, run_points = which, points
            if run_side > 0:
                shape['home_biggest_run'] = max(shape['home_biggest_run'],
                                                run_points)
            elif run_side < 0:
                shape['away_biggest_run'] = max(shape['away_biggest_run'],
                                                run_points)
            scored.append(dict(
                game_key=key, sequence=play_order(row), period=period,
                clock=_str(row.get('clock_display_value')), seconds_left=secs,
                elapsed=elapsed, team_id=side, points=points,
                kind=_str(row.get('type_text')),
                scorer=_str(row.get('athlete_name_1')),
                scorer_id=_int(row.get('athlete_id_1')),
                description=_str(row.get('text')),
                home_score=home, away_score=away, margin=margin))
        before = (home, away)
        # THE LINE SCORE IS A DIFFERENCE OF TOTALS, NOT A SUM OF DELTAS.
        # Summing each scoring play's delta looks equivalent and is not: the
        # archive carries one 2010 game whose rows appear twice, and summing
        # deltas across them gave a 213-234 line score for a game that ended
        # 127-141, while the cumulative totals were right all along. Each
        # period's last readable total is recorded here and the line score is
        # the step between consecutive periods.
        periods[period] = (home, away)

        if kept_at is None or elapsed - kept_at >= CURVE_SECONDS:
            states.append(dict(game_key=key, sequence=play_order(row),
                               period=period,
                               clock=_str(row.get('clock_display_value')),
                               seconds_left=secs, elapsed=elapsed,
                               home_score=home, away_score=away))
            kept_at = elapsed
    if not readable:
        return None
    # The final state is always kept: a curve that stops 40 seconds before
    # the end does not reach the score the page prints beside it.
    if states and before is not None and (states[-1]['home_score'],
                                          states[-1]['away_score']) != before:
        states.append(dict(game_key=key, sequence=play_order(plays[-1]) + 1,
                           period=shape['periods'],
                           clock=states[-1]['clock'], seconds_left=0,
                           elapsed=last_elapsed, home_score=before[0],
                           away_score=before[1]))
    shape['overtime'] = 1 if shape['periods'] > REGULATION_PERIODS else 0
    return (period_line(key, periods), shape, states,
            key_plays(scored, shape['periods']))


def period_line(key, period_ends):
    """The line score, as the step between consecutive cumulative totals."""
    rows, running = [], (0, 0)
    for number in sorted(period_ends):
        total = period_ends[number]
        rows.append(dict(game_key=key, period=number,
                         home_points=total[0] - running[0],
                         away_points=total[1] - running[1]))
        running = total
    return rows


def key_plays(scored, periods):
    """The plays a game story would print. MODEL-FREE, on purpose.

    Nothing here is chosen by a win-probability model: the model is fitted on
    `game_state`, and selecting the plays by it and then showing the model's
    own curve through them would be the model marking its own homework.

    A play is kept when any of these is true, and the `reason` column says
    which:

      lead  it put a side in front that was not the side in front, in the
            fourth period or an overtime. Through a tie counts: a game that
            goes 98-96, 98-98, 98-100 has changed hands, and reading the
            immediately previous margin rather than the last LEADER missed
            every lead change of that shape, which is most of them
      tie   it levelled the game, in the fourth period or an overtime
      late  it was scored in the last two minutes of the last period with
            the margin inside two possessions either side of it
      final it was the last score of the game

    Capped at `MAX_KEY_PLAYS`, keeping the LATEST: a game with nineteen lead
    changes in the fourth quarter is a game whose closing minutes matter most.
    """
    kept = {}
    previous, leader = 0, 0
    for play in scored:
        reason = None
        margin, before = play['margin'], previous
        side = _sign(margin)
        if play['period'] >= LATE_PERIOD:
            if side and leader and side != leader:
                reason = 'lead'
            elif margin == 0 and before:
                reason = 'tie'
        if reason is None and play['period'] >= periods \
                and (abs(margin) <= CLUTCH_MARGIN
                     or abs(before) <= CLUTCH_MARGIN):
            # THE CLOSING MINUTES OF THE LAST PERIOD ARE READ OFF THE PERIOD
            # CLOCK, not off `seconds_left`, which is REGULATION remaining
            # and pins at 0 right through overtime. Reading it here called
            # every close score in a deciding overtime "late", 5,666 of them
            # across the archive, one of them at 3:46 of a second overtime.
            inside = clock_seconds(play['clock'])
            if inside is not None and inside <= CLUTCH_SECONDS:
                reason = 'late'
        if reason:
            kept[play['sequence']] = dict(play, reason=reason)
        previous = margin
        if side:
            leader = side
    if scored:
        last = scored[-1]
        kept.setdefault(last['sequence'], dict(last, reason='final'))
    ordered = [kept[sequence] for sequence in sorted(kept)]
    return ordered[-MAX_KEY_PLAYS:]


PERIOD_COLUMNS = ('game_key', 'period', 'home_points', 'away_points')
SHAPE_COLUMNS = ('game_key', 'plays', 'scoring_plays', 'periods', 'overtime',
                 'lead_changes', 'ties', 'home_biggest_lead',
                 'away_biggest_lead', 'home_biggest_run', 'away_biggest_run',
                 'home_time_leading', 'away_time_leading', 'time_tied',
                 'reconciled')
STATE_COLUMNS = ('game_key', 'sequence', 'period', 'clock', 'seconds_left',
                 'elapsed', 'home_score', 'away_score')
PLAY_COLUMNS = ('game_key', 'sequence', 'period', 'clock', 'seconds_left',
                'team_key', 'points', 'kind', 'scorer', 'scorer_id',
                'description', 'home_score', 'away_score', 'reason')


def reduce_season(rows):
    """One season of plays as the four reduced tables, plus counts."""
    games, exhibition = group_games(rows)
    periods, shapes, states, plays = [], [], [], []
    counts = dict(plays=0, games=0, unreadable=0, exhibition=exhibition)
    for provider, rows_of_game in sorted(games.items(), key=lambda pair:
                                         int(pair[0]) if pair[0].isdigit()
                                         else 0):
        counts['plays'] += len(rows_of_game)
        reduced = reduce_game(provider, rows_of_game)
        if reduced is None:
            counts['unreadable'] += 1
            continue
        counts['games'] += 1
        period_rows, shape, state_rows, key = reduced
        periods.extend(period_rows)
        shapes.append(shape)
        states.extend(state_rows)
        plays.extend(key)
    return periods, shapes, states, plays, counts


def resolve_team(team_id):
    """A franchise key, or None where the id is not one."""
    if team_id is None:
        return None
    try:
        return team_key(team_id)
    except ValueError:
        return None


def known_games(connection, season):
    """`{game_key: (home_points, away_points)}` for a season's games.

    A play-by-play row for a game the archive does not have cannot be
    stored: every table here references `game(game_key)` and foreign keys
    are on. The counts the loader prints say how many were dropped, so a
    mismatch between the two feeds is visible rather than being an integrity
    error at the end of a long run.

    The final score comes back with the key because it is what `reconcile`
    checks the reduction against. A game with no final score yet maps to
    None, which is not the same as 0-0.
    """
    games = {}
    for row in connection.execute(
            'SELECT game_key, home_points, away_points FROM game'
            ' WHERE season = ?', (int(season),)):
        final = (None if row['home_points'] is None
                 or row['away_points'] is None
                 else (row['home_points'], row['away_points']))
        games[row['game_key']] = final
    return games


def reconcile(finals, periods, states):
    """Does the reduction add up to the box score? `{game_key: 1, 0 or None}`.

    Two things are checked, because they fail separately: the period line
    score summed over the periods, and the last point of the margin curve.
    A game whose feed stops six points short has a correct line score for
    the plays it carries and a curve that ends in the wrong place, and both
    are the same defect from a reader's point of view — the detail on the
    page would contradict the score beside it.

    None means "there is nothing to check against": a game not yet played,
    or one in progress whose final score does not exist to agree with.
    """
    sums, last = {}, {}
    for row in periods:
        total = sums.setdefault(row['game_key'], [0, 0])
        total[0] += row['home_points']
        total[1] += row['away_points']
    for row in states:
        current = last.get(row['game_key'])
        if current is None or row['sequence'] >= current[0]:
            last[row['game_key']] = (row['sequence'], row['home_score'],
                                     row['away_score'])
    verdict = {}
    for key, final in finals.items():
        if final is None:
            verdict[key] = None
            continue
        line = sums.get(key)
        tail = last.get(key)
        if line is None or tail is None:
            verdict[key] = None
            continue
        verdict[key] = int(tuple(line) == final
                           and (tail[1], tail[2]) == final)
    return verdict


def replace_season(connection, season, periods, shapes, states, plays, *,
                   snapshot_id=None):
    """Store one season's game layer, replacing whatever was there.

    DELETE then INSERT, not an upsert: the source revises these files, and a
    period or a key play that has stopped existing must stop existing here.
    An upsert would leave a phantom fifth period in a game the source now
    says went four.
    """
    finals = known_games(connection, season)
    keys = set(finals)
    kept = {name: [row for row in rows if row['game_key'] in keys]
            for name, rows in (('period', periods), ('shape', shapes),
                               ('state', states), ('play', plays))}
    verdict = reconcile(finals, kept['period'], kept['state'])
    for row in kept['shape']:
        row['reconciled'] = verdict.get(row['game_key'])
    if keys:
        ordered = sorted(keys)
        marks = ','.join('?' * len(ordered))
        for table in ('game_period', 'game_shape', 'game_state', 'game_play'):
            connection.execute(
                f'DELETE FROM {table} WHERE game_key IN ({marks})', ordered)
    for table, columns, name in (
            ('game_period', PERIOD_COLUMNS, 'period'),
            ('game_shape', SHAPE_COLUMNS, 'shape'),
            ('game_state', STATE_COLUMNS, 'state'),
            ('game_play', PLAY_COLUMNS, 'play')):
        rows = kept[name]
        if not rows:
            continue
        names = ', '.join(columns + ('snapshot_id',))
        marks = ', '.join('?' * (len(columns) + 1))
        prepared = []
        for row in rows:
            values = dict(row)
            if 'team_key' in columns:
                values['team_key'] = resolve_team(values.get('team_id'))
            prepared.append(tuple(values[column] for column in columns)
                            + (snapshot_id,))
        connection.executemany(
            f'INSERT INTO {table} ({names}) VALUES ({marks})', prepared)
    connection.commit()
    return dict(periods=len(kept['period']), shapes=len(kept['shape']),
                states=len(kept['state']), key_plays=len(kept['play']),
                dropped=len(shapes) - len(kept['shape']),
                unreconciled=sum(1 for row in kept['shape']
                                 if row['reconciled'] == 0))


def each_season(seasons, *, root=None, opener=None, sleep=None):
    """Fetch each season's play-by-play, hand over its rows, then delete it.

    A generator, so the caller never holds two seasons at once, and the file
    is removed in a `finally` so a reduction that raises still leaves no
    22 MB file behind. `source.load_asset` would cache it under
    `data/nba/raw`, which across twenty-five seasons is half a gigabyte of
    plays this module exists not to keep.
    """
    for season in seasons:
        directory = Path(root) if root else Path(tempfile.gettempdir())
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f'nba-pbp-{season}.parquet'
        try:
            body = source.fetch_asset(source.asset_url('pbp', season),
                                      opener=opener, sleep=sleep)
            target.write_bytes(body)
            yield season, source.read_parquet(target), len(body)
        finally:
            target.unlink(missing_ok=True)


def ingest(connection, seasons, *, snapshot_id=None, root=None, opener=None,
           sleep=None, fetcher=None):
    """Fetch, reduce and store each season, keeping none of the plays."""
    stream = fetcher if fetcher is not None else each_season
    report = dict(seasons=0, plays=0, periods=0, shapes=0, states=0,
                  key_plays=0, dropped=0, unreconciled=0, bytes=0,
                  per_season=[])
    for season, rows, size in stream(seasons, root=root, opener=opener,
                                     sleep=sleep):
        periods, shapes, states, plays, counts = reduce_season(rows)
        stored = replace_season(connection, season, periods, shapes, states,
                                plays, snapshot_id=snapshot_id)
        report['seasons'] += 1
        report['plays'] += counts['plays']
        report['bytes'] += size
        for name in ('periods', 'shapes', 'states', 'key_plays', 'dropped',
                     'unreconciled'):
            report[name] += stored[name]
        report['per_season'].append(dict(season=season, **counts, **stored))
    return report


def seasons_for(seasons=None, *, now=None, first=2002):
    """The seasons to reduce: the ones asked for, or every published one."""
    if seasons:
        return sorted(seasons)
    moment = now or datetime.now(timezone.utc)
    return source.available_seasons(moment, first)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Reduce the NBA play-by-play to the game layer')
    parser.add_argument('--db', default=None)
    parser.add_argument('--seasons', default=None)
    parser.add_argument('--work-dir', default=None,
                        help='where a season is written before it is reduced '
                             'and deleted')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    moment = datetime.now(timezone.utc)
    connection = connect(args.db)
    try:
        snapshot_id = record_snapshot(
            connection, 'play-by-play', moment.isoformat(),
            source=source.RELEASE_ROOT,
            notes='reduced to periods, shape, states and key plays')
        report = ingest(connection, seasons_for(seasons, now=moment),
                        snapshot_id=snapshot_id, root=args.work_dir)
    except source.NbaSourceError as error:
        print(f'Cannot reduce the play-by-play: {error}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    print(f'{report["seasons"]} seasons, {report["plays"]:,} plays read '
          f'({report["bytes"] / 1e9:.2f} GB fetched and discarded), '
          f'{report["shapes"]:,} games reduced to {report["periods"]:,} '
          f'periods, {report["states"]:,} states and '
          f'{report["key_plays"]:,} key plays')
    for row in report['per_season']:
        extra = ''
        if row['unreadable'] or row['dropped'] or row['exhibition'] \
                or row['unreconciled']:
            extra = (f'  [unreadable {row["unreadable"]}, no game '
                     f'{row["dropped"]}, exhibition rows '
                     f'{row["exhibition"]}, not reconciled '
                     f'{row["unreconciled"]}]')
        print(f'  {row["season"]}  games {row["games"]:>4}  plays '
              f'{row["plays"]:>7,}  states {row["states"]:>6,}  key plays '
              f'{row["key_plays"]:>5,}{extra}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
