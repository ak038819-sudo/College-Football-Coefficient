"""Reconcile pilot box scores against the lineup feed and the final score.

Minute reconciliation is the gate B01 puts in front of paired-lineup work: the
lineup feed's `totalSeconds` and the box score's per-player `minutes` are two
independent measurements of the same floor time, so disagreement means one of
them cannot be trusted to rebuild stints. Pure functions, no network.
"""

import math

# Regulation is 40 minutes with five players on the floor; each overtime adds 5.
REGULATION_MINUTES = 40
PLAYERS_ON_FLOOR = 5
OVERTIME_MINUTES = 5
REGULATION_PERIODS = 2


def _is_number(value):
    """None is not a number. Never coerce a missing value into a zero."""
    if value is None or isinstance(value, bool):
        return False
    return isinstance(value, (int, float)) and math.isfinite(value)


def period_count(game):
    """Periods actually played, from the two period-score arrays, or None."""
    home, away = game.get('homePeriodPoints'), game.get('awayPeriodPoints')
    if not isinstance(home, list) or not isinstance(away, list):
        return None
    if len(home) != len(away) or len(home) < REGULATION_PERIODS:
        return None
    return len(home)


def expected_player_minutes(periods):
    """Total player-minutes one team should record across `periods` periods."""
    if type(periods) is not int or periods < REGULATION_PERIODS:
        raise ValueError('Require at least two periods to size a game')
    overtimes = periods - REGULATION_PERIODS
    return (REGULATION_MINUTES + OVERTIME_MINUTES * overtimes) * PLAYERS_ON_FLOOR


def reconcile_team_box_score(game, box, lineup_seconds=None, tolerance=1.0):
    """Check one team's box score against the game, and against lineup seconds.

    `lineup_seconds` is that team's summed lineup `totalSeconds`; None skips the
    cross-source check rather than inventing agreement.
    """
    issues = []
    team = box.get('teamId')
    if team not in {game['homeTeamId'], game['awayTeamId']}:
        return dict(team_id=team, reconciled=False, issues=['Box score team is not in this game'],
                    players=0, minutes=None, points=None)

    players = box.get('players')
    if not isinstance(players, list) or not players:
        return dict(team_id=team, reconciled=False, issues=['No player rows'],
                    players=0, minutes=None, points=None)

    athlete_ids, minutes_total, points_total, starters = [], 0.0, 0, 0
    missing_minutes, missing_points = 0, 0
    for row in players:
        athlete = row.get('athleteId')
        if type(athlete) is not int or athlete <= 0:
            issues.append('Player row without a usable athlete id')
        else:
            athlete_ids.append(athlete)
        if row.get('starter') is True:
            starters += 1
        minutes = row.get('minutes')
        if _is_number(minutes):
            if minutes < 0:
                issues.append('Negative minutes')
            minutes_total += minutes
        else:
            # A player who did not play may legitimately carry no minutes.
            missing_minutes += 1
        points = row.get('points')
        if _is_number(points):
            if points < 0:
                issues.append('Negative points')
            points_total += points
        else:
            missing_points += 1

    if len(set(athlete_ids)) != len(athlete_ids):
        issues.append('Duplicate athlete id within one team box score')
    if starters != PLAYERS_ON_FLOOR:
        issues.append(f'Expected {PLAYERS_ON_FLOOR} starters, found {starters}')

    is_home = team == game['homeTeamId']
    final = game['homePoints'] if is_home else game['awayPoints']
    if _is_number(final) and points_total != final:
        issues.append(f'Box score points {points_total:g} do not equal final score {final}')

    periods = period_count(game)
    expected = expected_player_minutes(periods) if periods is not None else None
    if expected is None:
        issues.append('Cannot size the game from period scores; minutes unverified')
    elif abs(minutes_total - expected) > tolerance:
        issues.append(f'Player minutes {minutes_total:g} do not total the expected {expected:g}')

    from_lineups = None
    if _is_number(lineup_seconds):
        from_lineups = lineup_seconds * PLAYERS_ON_FLOOR / 60.0
        if abs(minutes_total - from_lineups) > tolerance:
            issues.append(f'Player minutes {minutes_total:g} disagree with the lineup feed'
                          f' ({from_lineups:.2f}); stints cannot be trusted')

    return dict(team_id=team, reconciled=not issues, issues=issues,
                players=len(players), starters=starters,
                minutes=minutes_total, expected_minutes=expected,
                minutes_from_lineups=from_lineups,
                players_without_minutes=missing_minutes,
                players_without_points=missing_points,
                points=points_total, final_score=final, periods=periods)


def filtered_team_id(boxes):
    """The single team id in a team-filtered box score response.

    The collector knows its team by provider NAME, while stints and box scores
    are keyed by id. Reading the id back from the filtered rows avoids guessing
    that mapping, and fails loudly if the provider ignored the team filter.
    """
    ids = {row.get('teamId') for row in boxes}
    if len(ids) != 1 or not all(type(i) is int and i > 0 for i in ids):
        raise ValueError('Team-filtered box scores must name exactly one team id')
    return ids.pop()


def reconcile_game(game, boxes, lineup_seconds_by_team=None, expect_teams=None):
    """Reconcile a game's available box scores. Missing data is never filled.

    `expect_teams` limits which teams a box score is required for. A team-filtered
    pilot returns only its own rows, so demanding both sides would report a
    provider gap that does not exist.
    """
    seconds = lineup_seconds_by_team or {}
    both = {game['homeTeamId'], game['awayTeamId']}
    expected_teams = both if expect_teams is None else (set(expect_teams) & both)
    rows = [reconcile_team_box_score(game, box, seconds.get(box.get('teamId')))
            for box in boxes if box.get('gameId') == game['id']
            and box.get('teamId') in expected_teams]
    issues = []
    found = {row['team_id'] for row in rows}
    for team in sorted(expected_teams - found):
        issues.append(f'{team}: no box score for this game')
    required = [row for row in rows if row['team_id'] in expected_teams]
    return dict(game_id=game['id'],
                reconciled=not issues and bool(required) and all(r['reconciled'] for r in required),
                issues=issues, teams=rows,
                reconciled_teams=sorted(expected_teams),
                minute_reconciliation='box score vs lineup seconds and period count')


def lineup_seconds_by_team(lineups):
    """Sum a game's lineup `totalSeconds` per team, skipping unusable rows."""
    totals = {}
    for row in lineups:
        team, seconds = row.get('teamId'), row.get('totalSeconds')
        if type(team) is not int or not _is_number(seconds) or seconds <= 0:
            continue
        totals[team] = totals.get(team, 0.0) + seconds
    return totals


def clock_ordering_issues(events, kind):
    """Count non-monotonic clocks within a period. Basketball clocks count down.

    For plays, `period` and `secondsRemaining` are top level fields.
    """
    issues, last = [], {}
    for index, event in enumerate(events):
        period, clock = event.get('period'), event.get('secondsRemaining')
        if type(period) is not int or not _is_number(clock):
            issues.append(f'{kind} {index}: missing period or clock')
            continue
        previous = last.get(period)
        if previous is not None and clock > previous:
            issues.append(f'{kind} {index}: clock moves backwards in period {period}')
        last[period] = clock
    return issues


def substitution_stint_issues(subs):
    """Check each substitution row's own stint, not the order rows arrive in.

    A substitution row carries one athlete's stint with the timing nested under
    `subIn` and `subOut`, each with its own `period` and `secondsRemaining`.
    There is no top level clock, so reading one finds nothing. A stint must not
    end before it starts: a later period, or the same period with the clock at
    or below where it started.
    """
    issues = []
    for index, row in enumerate(subs):
        entering, leaving = row.get('subIn'), row.get('subOut')
        if not isinstance(entering, dict) or not isinstance(leaving, dict):
            issues.append(f'substitution {index}: missing subIn/subOut')
            continue
        if type(row.get('athleteId')) is not int:
            issues.append(f'substitution {index}: no athlete id')
        times = []
        for label, side in (('subIn', entering), ('subOut', leaving)):
            period, clock = side.get('period'), side.get('secondsRemaining')
            if type(period) is not int or not _is_number(clock):
                issues.append(f'substitution {index}: {label} has no period or clock')
                times = None
                break
            times.append((period, clock))
        if not times:
            continue
        (in_period, in_clock), (out_period, out_clock) = times
        if out_period < in_period or (out_period == in_period and out_clock > in_clock):
            issues.append(f'substitution {index}: stint ends before it starts')
    return issues
