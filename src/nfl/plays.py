"""Reducing the play-by-play to drives and scoring plays.

The plays themselves are never stored. What a game page actually shows is the
scoring summary, the shape of each drive and how the win probability moved,
and all three come from a few dozen rows a game rather than the 174 plays
behind them.

**THE DRIVE FIELDS ARE NOT ON THE DRIVE'S FIRST PLAY.** `drive_start_yard_line`
and its fourteen companions are blank on the opening row of a drive and
populated a play or two later, so a reduction that reads the first row gets a
drive with a result and no start. Every field is therefore taken as the first
non-blank value anywhere in the drive. The SCORE is the opposite case and is
taken from the first play with a possessing team, because the score before the
drive is exactly what the drive started from.

Measured on 2024 (285 games, 49,492 plays):

| | |
| --- | --- |
| drives | 6,148 |
| drives the source gives no metadata for | 24 |
| drives with no possessing team | 2 |
| scoring plays | 3,767 |
| scoring plays this module cannot classify | 0 |
"""

import argparse
import sys
from datetime import datetime, timezone

from . import pbp_source
from .identity import people_franchise_key
from .store import connect, record_snapshot
from .win_probability import PERIOD_SECONDS, REGULATION_PERIODS

# Only these columns are read. The file has 372.
COLUMNS = (
    'game_id', 'play_id', 'fixed_drive', 'fixed_drive_result', 'posteam',
    'defteam', 'home_team', 'away_team',
    'qtr', 'time', 'game_seconds_remaining', 'total_home_score',
    'total_away_score', 'drive_quarter_start', 'drive_quarter_end',
    'drive_game_clock_start', 'drive_game_clock_end',
    'drive_start_yard_line', 'drive_end_yard_line', 'drive_play_count',
    'drive_time_of_possession', 'drive_first_downs', 'drive_inside20',
    'drive_yards_penalized', 'drive_start_transition',
    'drive_end_transition', 'drive_ended_with_score', 'ydsnet',
    'sp', 'touchdown', 'field_goal_result', 'extra_point_result',
    'two_point_conv_result', 'safety', 'defensive_two_point_conv',
    'td_team', 'td_player_name', 'td_player_id', 'kicker_player_name',
    'kicker_player_id', 'kick_distance', 'desc', 'play_type',
    'safety_player_name', 'season_type',
)

# The points each kind of score is worth. A defensive two-point conversion is
# the rarest play in football and is worth two; there were 2 in 2024.
POINTS = {'TD': 6, 'FG': 3, 'XP': 1, '2PT': 2, 'SAFETY': 2, 'D2PT': 2}

GAME_PREFIX = 'nfl:nflverse:game:'


def _int(value):
    if value in (None, '', 'NA'):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number)


def _str(value):
    text = (value or '').strip()
    return text or None


def _team(code):
    cleaned = _str(code)
    if not cleaned:
        return None, None
    return people_franchise_key(cleaned), cleaned


def seconds_left(game_seconds_remaining, quarter):
    """Regulation seconds left, with overtime pinned at 0.

    The source's `game_seconds_remaining` already counts down to 0 at the end
    of regulation and stays there through overtime, but it is blank on some
    rows, so the quarter is the fallback and the floor.
    """
    value = _int(game_seconds_remaining)
    if value is not None:
        return max(0, min(PERIOD_SECONDS * REGULATION_PERIODS, value))
    period = _int(quarter)
    if period is None or period > REGULATION_PERIODS:
        return 0
    return PERIOD_SECONDS * (REGULATION_PERIODS - period)


def classify(row):
    """Which of the six kinds of score a scoring play is, or None.

    Order matters: a touchdown row also carries the drive's result, and an
    extra point is its own play rather than part of the touchdown's.
    """
    if row.get('touchdown') == '1':
        return 'TD'
    if row.get('field_goal_result') == 'made':
        return 'FG'
    if row.get('extra_point_result') == 'good':
        return 'XP'
    if row.get('two_point_conv_result') == 'success':
        return '2PT'
    if row.get('safety') == '1':
        return 'SAFETY'
    if row.get('defensive_two_point_conv') == '1':
        return 'D2PT'
    return None


def named_scoring_team(row, kind):
    """Who the row's own columns say scored. The fallback, not the rule.

    MEASURED, not assumed, against 1999-2026. Taking `posteam` for every kind
    left 463 of 7,366 games whose scoring plays did not add up to the final,
    438 of them out by exactly two points in both directions — every safety
    in the archive, handed to the team that conceded it.

    - A TOUCHDOWN states its own `td_team`, which is what makes a pick six
      the defence's.
    - A SAFETY and a DEFENSIVE TWO-POINT CONVERSION are the defence's, and
      `td_team` is blank on both.
    - A field goal, extra point and two-point conversion belong to the side
      with the ball.

    None of that is reliable on its own. A safety conceded AFTER possession
    changed on the play — a punt returner downed in his own end zone, a
    tackle at the end of an interception return — is scored by `posteam`,
    not `defteam`, and the row says nothing to separate the two cases. That
    is why `scored_by` below prefers the score itself.
    """
    if kind in ('SAFETY', 'D2PT'):
        return _str(row.get('defteam'))
    if kind == 'TD':
        return _str(row.get('td_team')) or _str(row.get('posteam'))
    return _str(row.get('posteam'))


def scored_by(row, kind, before):
    """The code of the team the points went to, read from the score itself.

    THE SCOREBOARD IS THE AUTHORITY, not the play's team columns. A row
    carries the score after the play, so the side whose total rose by exactly
    this play's points is the side that scored — and that settles the cases
    the columns get wrong, above all a safety on a punt or at the end of an
    interception return, which `defteam` attributes backwards.

    `before` is the running score as of the previous row of this game, or
    None at the first row. Where the delta does not name exactly one side —
    the archive's score columns are themselves wrong for a handful of early
    Jacksonville games, and blank here and there — the row's own columns
    answer instead, and `reconcile` below is what notices that the result
    does not add up to the final.
    """
    home, away = _str(row.get('home_team')), _str(row.get('away_team'))
    points = POINTS[kind]
    if before is not None and home and away:
        now = (_int(row.get('total_home_score')),
               _int(row.get('total_away_score')))
        if now[0] is not None and now[1] is not None:
            moved = (now[0] - before[0] == points,
                     now[1] - before[1] == points)
            if moved[0] and not moved[1]:
                return home
            if moved[1] and not moved[0]:
                return away
    return named_scoring_team(row, kind)


def strip_clock(description):
    """The play's own words, without the clock it already carries.

    Most descriptions open with "(7:54) ", which the row states in `time`
    and the page prints from there. Removing the duplicate is eight
    characters a play across 95,000 of them.

    ONLY A CLOCK IS STRIPPED. A first version took anything parenthesised in
    the first nine characters, which quietly ate the "(Shotgun) " off every
    description that did not happen to carry a clock in front of it. The
    contents are checked instead: digits and one colon, and nothing else.
    """
    text = _str(description)
    if not text or not text.startswith('('):
        return text
    close = text.find(') ')
    if close <= 0:
        return text
    minutes, colon, seconds = text[1:close].partition(':')
    if not colon or not seconds.isdigit() \
            or (minutes and not minutes.isdigit()):
        return text
    return text[close + 2:].strip() or None


# The drive columns taken as the first non-blank value anywhere in the drive.
DRIVE_FIELDS = {
    'quarter_start': 'drive_quarter_start',
    'quarter_end': 'drive_quarter_end',
    'clock_start': 'drive_game_clock_start',
    'clock_end': 'drive_game_clock_end',
    'start_yard_line': 'drive_start_yard_line',
    'end_yard_line': 'drive_end_yard_line',
    'plays': 'drive_play_count',
    'yards': 'ydsnet',
    'time_of_possession': 'drive_time_of_possession',
    'first_downs': 'drive_first_downs',
    'inside_twenty': 'drive_inside20',
    'yards_penalized': 'drive_yards_penalized',
    'result': 'fixed_drive_result',
    'start_transition': 'drive_start_transition',
    'end_transition': 'drive_end_transition',
    'ended_with_score': 'drive_ended_with_score',
}
TEXT_DRIVE_FIELDS = frozenset({
    'clock_start', 'clock_end', 'start_yard_line', 'end_yard_line',
    'time_of_possession', 'result', 'start_transition', 'end_transition'})


def reduce_season(rows):
    """One season of plays as (drives, scoring plays, counts).

    `rows` is any iterable of dicts carrying `COLUMNS`; in a refresh it is
    `pbp_source.rows`, which streams them off disk a row at a time.
    """
    drives, scoring = {}, []
    order = []
    # The running score as of the row BEFORE the one being read, per game.
    # `scored_by` reads the delta off it, so it is updated on every row and
    # not only on scoring plays.
    running = {}
    counts = dict(plays=0, games=0, no_drive=0, no_team=0, unclassified=0)
    games = set()
    for row in rows:
        counts['plays'] += 1
        provider = _str(row.get('game_id'))
        if not provider:
            continue
        games.add(provider)
        game_key = GAME_PREFIX + provider
        before = running.get(provider)
        totals = (_int(row.get('total_home_score')),
                  _int(row.get('total_away_score')))
        if totals[0] is not None and totals[1] is not None:
            running[provider] = totals
        number = _int(row.get('fixed_drive'))
        if number is None:
            counts['no_drive'] += 1
        else:
            key = (game_key, number)
            drive = drives.get(key)
            if drive is None:
                drive = drives[key] = dict(
                    game_key=game_key, drive=number, team_key=None, code=None,
                    seconds_left=None, home_score=None, away_score=None,
                    **{name: None for name in DRIVE_FIELDS})
                order.append(key)
            if drive['team_key'] is None and _str(row.get('posteam')):
                drive['team_key'], drive['code'] = _team(row.get('posteam'))
                drive['home_score'] = _int(row.get('total_home_score'))
                drive['away_score'] = _int(row.get('total_away_score'))
                drive['seconds_left'] = seconds_left(
                    row.get('game_seconds_remaining'), row.get('qtr'))
            for name, column in DRIVE_FIELDS.items():
                if drive[name] is None:
                    value = (_str(row.get(column))
                             if name in TEXT_DRIVE_FIELDS
                             else _int(row.get(column)))
                    if value is not None:
                        drive[name] = value
        if row.get('sp') != '1':
            continue
        kind = classify(row)
        if kind is None:
            counts['unclassified'] += 1
            continue
        team_key, code = _team(scored_by(row, kind, before))
        scoring.append(dict(
            game_key=game_key, play_id=_int(row.get('play_id')) or 0,
            quarter=_int(row.get('qtr')), clock=_str(row.get('time')),
            seconds_left=seconds_left(row.get('game_seconds_remaining'),
                                      row.get('qtr')),
            team_key=team_key, code=code, kind=kind, points=POINTS[kind],
            scorer=(_str(row.get('safety_player_name')) if kind == 'SAFETY'
                    else (_str(row.get('td_player_name'))
                          or _str(row.get('kicker_player_name')))),
            scorer_id=(None if kind == 'SAFETY'
                       else (_str(row.get('td_player_id'))
                             or _str(row.get('kicker_player_id')))),
            kick_distance=_int(row.get('kick_distance'))
            if kind in ('FG', 'XP') else None,
            description=strip_clock(row.get('desc')),
            home_score=_int(row.get('total_home_score')),
            away_score=_int(row.get('total_away_score'))))
    counts['games'] = len(games)
    counts['no_team'] = sum(1 for key in order if drives[key]['team_key']
                            is None)
    return [drives[key] for key in order], scoring, counts


DRIVE_COLUMNS = ('game_key', 'drive', 'team_key', 'code', 'quarter_start',
                 'quarter_end', 'clock_start', 'clock_end', 'seconds_left',
                 'start_yard_line', 'end_yard_line', 'plays', 'yards',
                 'time_of_possession', 'first_downs', 'inside_twenty',
                 'yards_penalized', 'result', 'start_transition',
                 'end_transition', 'ended_with_score', 'home_score',
                 'away_score')
SCORING_COLUMNS = ('game_key', 'play_id', 'quarter', 'clock', 'seconds_left',
                   'team_key', 'code', 'kind', 'points', 'scorer',
                   'scorer_id', 'kick_distance', 'description', 'home_score',
                   'away_score')


def known_games(connection, season):
    """The game keys this archive carries for a season.

    A play-by-play row for a game the archive does not have cannot be stored:
    `game_drive.game_key` references `game(game_key)` and foreign keys are on.
    Measured rather than assumed — the counts the loader prints say how many
    were dropped, so a mismatch between the two feeds is visible instead of
    being an integrity error at the end of a long run.
    """
    return {row[0] for row in connection.execute(
        'SELECT game_key FROM game WHERE season = ?', (season,))}


def replace_season(connection, season, drives, scoring, *, snapshot_id=None):
    """Store one season's drives and scoring plays, replacing what was there.

    DELETE then INSERT, not an upsert: the source revises these files, and a
    drive that has stopped existing must stop existing here. An upsert would
    leave a phantom drive 14 in a game the source now says had 13.
    """
    keys = known_games(connection, season)
    kept_drives = [row for row in drives if row['game_key'] in keys]
    kept_scoring = [row for row in scoring if row['game_key'] in keys]
    if keys:
        marks = ','.join('?' * len(keys))
        ordered = sorted(keys)
        connection.execute(
            f'DELETE FROM game_drive WHERE game_key IN ({marks})', ordered)
        connection.execute(
            f'DELETE FROM scoring_play WHERE game_key IN ({marks})', ordered)
    for table, columns, rows_in in (
            ('game_drive', DRIVE_COLUMNS, kept_drives),
            ('scoring_play', SCORING_COLUMNS, kept_scoring)):
        names = ', '.join(columns + ('snapshot_id',))
        marks = ', '.join('?' * (len(columns) + 1))
        connection.executemany(
            f'INSERT INTO {table} ({names}) VALUES ({marks})',
            [tuple(row[name] for name in columns) + (snapshot_id,)
             for row in rows_in])
    connection.commit()
    return dict(drives=len(kept_drives), scoring=len(kept_scoring),
                dropped_drives=len(drives) - len(kept_drives),
                dropped_scoring=len(scoring) - len(kept_scoring))


def ingest(connection, seasons=None, *, snapshot_id=None, opener=None,
           sleep=None, now=None, root=None, source=pbp_source):
    """Fetch, reduce and store each season, keeping none of the plays."""
    report = dict(seasons=0, plays=0, drives=0, scoring=0, dropped=0,
                  bytes=0, absent=[], per_season=[])
    wanted = source.seasons_for(seasons, now=now)
    for season, path, size in source.each_season(
            wanted, opener=opener, sleep=sleep, now=now, root=root):
        if path is None:
            report['absent'].append(season)
            continue
        drives, scoring, counts = reduce_season(
            source.rows(path, COLUMNS))
        stored = replace_season(connection, season, drives, scoring,
                                snapshot_id=snapshot_id)
        report['seasons'] += 1
        report['plays'] += counts['plays']
        report['bytes'] += size
        report['drives'] += stored['drives']
        report['scoring'] += stored['scoring']
        report['dropped'] += stored['dropped_drives']
        report['per_season'].append(dict(season=season, **counts, **stored))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Reduce the NFL play-by-play to drives and scoring plays')
    parser.add_argument('--db', default=None)
    parser.add_argument('--seasons', default=None)
    parser.add_argument('--work-dir', default=None,
                        help='where a season is downloaded before it is '
                             'reduced and deleted')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    moment = datetime.now(timezone.utc)
    connection = connect(args.db)
    try:
        snapshot_id = record_snapshot(
            connection, 'play-by-play', moment.isoformat(),
            source=pbp_source.RELEASE, notes='reduced to drives and scores')
        report = ingest(connection, seasons, snapshot_id=snapshot_id,
                        now=moment, root=args.work_dir)
    except pbp_source.NflSourceError as error:
        print(f'Cannot reduce the play-by-play: {error}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    print(f'{report["seasons"]} seasons, {report["plays"]} plays read '
          f'({report["bytes"] / 1e9:.2f} GB fetched and discarded), '
          f'{report["drives"]} drives and {report["scoring"]} scoring plays '
          f'stored')
    for row in report['per_season']:
        extra = ''
        if row['no_drive'] or row['unclassified'] or row['dropped_drives']:
            extra = (f'  [no drive {row["no_drive"]}, unclassified '
                     f'{row["unclassified"]}, no game '
                     f'{row["dropped_drives"]}]')
        print(f'  {row["season"]}  games {row["games"]:>3}  plays '
              f'{row["plays"]:>6}  drives {row["drives"]:>5}  scores '
              f'{row["scoring"]:>5}{extra}')
    if report['absent']:
        print(f'  not published yet: {report["absent"]}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
