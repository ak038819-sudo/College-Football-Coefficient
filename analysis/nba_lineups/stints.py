"""Rebuilding 2017-18 lineup stints from the ESPN play-by-play.

**THE FEED MARKS NEITHER STARTERS NOR POSSESSIONS.** `src/nba/plays.py` says
so and stores neither. Both are rebuilt here, from the two things the feed
does state: who entered for whom, and who made each play.

- Starters come from the PLAYER BOX (`player_box.starter`), not from the
  plays. Period one opens with them; every later period opens with the
  previous period's closing five, because ESPN records a start-of-period
  change as an ordinary substitution stamped at 12:00.
- Possessions are counted as RUNS OF THE OFFENSIVE TEAM. The offensive team
  of a play is the shooter's team, the team that turned it over, the team
  that rebounded its own miss, or -- for a defensive rebound -- the other
  team. A run is one possession, so an and-one is one possession and not
  two, and the count needs no 0.44 anywhere.

**805 OF 2017-18's 69,052 SUBSTITUTION ROWS NAME ONLY ONE PLAYER** (411 of
1,310 games). `athlete_id_1` is whichever player the row does name, so its
meaning flips with the wording: "X enters the game for " leaves the exit
unnamed, while " enters the game for Y" leaves the ENTRANT unnamed and puts
Y -- the player leaving -- in `athlete_id_1`. Reading `athlete_id_1` as the
entrant unconditionally silently swaps a player onto the floor for the one
who actually left. Three repairs, in order, and each one is evidence rather
than a guess:

1. 239 of those rows are PAIRED: a row missing its exit and a row missing
   its entrant, same team and same clock. That is one swap split in two.
2. An unnamed ENTRANT is identified by the floor being short: the next
   player of that team to touch the ball, or to be substituted out, while
   not on the floor can only be the player who came in.
3. An unnamed EXIT is identified by the floor being long: the next player of
   that team to ENTER while already on the floor can only be the one who
   left.

What no evidence resolves is never guessed. The affected stints carry
`quality_status` `unresolved_lineup`, and a reader can drop them.
"""

from collections import defaultdict

PERIOD_SECONDS = 720
OVERTIME_SECONDS = 300
REGULATION_PERIODS = 4
LINEUP_SIZE = 5

SUBSTITUTION = 'Substitution'
PERIOD_END = 'End Period'
GAME_END = 'End Game'
OFFENSIVE_REBOUND = 'Offensive Rebound'
DEFENSIVE_REBOUND = 'Defensive Rebound'
NOT_A_TURNOVER = 'No Turnover'
# 'Traveling' is the one turnover whose name does not say so.
NAMED_TURNOVER = 'Traveling'
ENTRANT_UNNAMED = 'enters the game for'
# A technical free throw scores without anybody having the ball.
TECHNICAL = 'Technical'

VERIFIED = 'verified'
UNRESOLVED = 'unresolved_lineup'
SCORED_WITHOUT_POSSESSION = 'scored_without_possession'
MINUTES_UNRECONCILED = 'minutes_unreconciled'


def period_length(period):
    """Seconds in a period. Overtime is five minutes, not twelve."""
    return PERIOD_SECONDS if int(period) <= REGULATION_PERIODS \
        else OVERTIME_SECONDS


def period_start(period):
    """Elapsed game seconds at the opening tip of a period."""
    period = int(period)
    if period <= REGULATION_PERIODS:
        return (period - 1) * PERIOD_SECONDS
    return (REGULATION_PERIODS * PERIOD_SECONDS
            + (period - REGULATION_PERIODS - 1) * OVERTIME_SECONDS)


def remaining(clock):
    """Seconds left in the period from a display clock, or None."""
    if clock is None:
        return None
    text = str(clock).strip()
    if not text or text.lower() in ('nan', 'none'):
        return None
    minutes, _, seconds = text.partition(':')
    try:
        if _:
            return int(minutes) * 60 + float(seconds)
        return float(minutes)
    except ValueError:
        return None


def elapsed(period, clock):
    """Elapsed game seconds, counting overtime on from regulation."""
    left = remaining(clock)
    if left is None:
        return None
    return period_start(period) + (period_length(period) - left)


def _identifier(value):
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number or None


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if number != number else number


def is_turnover(kind):
    """Whether a play type is a turnover, team turnovers included."""
    kind = str(kind)
    if kind == NOT_A_TURNOVER:
        return False
    return 'Turnover' in kind or kind == NAMED_TURNOVER


def offensive_team(play, teams):
    """The team with the ball, or None if the play does not say."""
    team = _identifier(play.get('team_id'))
    if team is None:
        return None
    kind = str(play.get('type_text'))
    if kind == DEFENSIVE_REBOUND:
        other = [one for one in teams if one != team]
        return other[0] if len(other) == 1 else None
    if TECHNICAL in kind:
        return None
    if kind == OFFENSIVE_REBOUND or is_turnover(kind) \
            or bool(play.get('shooting_play')):
        return team
    return None


def _substitution(play):
    """(entrant, leaver) for a substitution row; either may be None."""
    named = _identifier(play.get('athlete_id_1'))
    other = _identifier(play.get('athlete_id_2'))
    if other is not None:
        return named, other
    if str(play.get('text') or '').strip().startswith(ENTRANT_UNNAMED):
        return None, named
    return named, None


def _pair_halves(moves):
    """Join a row missing its exit to one missing its entrant.

    Same team and same moment, one of each: that is one substitution the
    feed wrote on two lines. Dropped rather than merged when the moment
    holds more than one of either, since then which pairs with which is
    not stated.
    """
    grouped = defaultdict(list)
    for move in moves:
        if move['enter'] is None or move['leave'] is None:
            grouped[(move['at'], move['team'])].append(move)
    joined = []
    for half in grouped.values():
        missing_exit = [one for one in half if one['leave'] is None]
        missing_entry = [one for one in half if one['enter'] is None]
        if len(missing_exit) == 1 and len(missing_entry) == 1:
            missing_exit[0]['leave'] = missing_entry[0]['leave']
            missing_exit[0]['paired'] = True
            joined.append(missing_entry[0])
    return [move for move in moves if move not in joined]


def substitutions(plays, teams):
    """Every substitution as a move, with the one-sided rows repaired.

    Returns (moves, notes). A move is a dict with `at`, `team`, `enter`,
    `leave`; an unrepaired side stays None and the note says why.
    """
    moves = []
    for index, play in enumerate(plays):
        if str(play.get('type_text')) != SUBSTITUTION:
            continue
        when = elapsed(play.get('period_number'),
                       play.get('clock_display_value'))
        if when is None:
            continue
        enter, leave = _substitution(play)
        team = _identifier(play.get('team_id'))
        moves.append({'at': when, 'order': index,
                      'period': int(play.get('period_number')),
                      'team': team, 'enter': enter, 'leave': leave,
                      'paired': False})
    moves = _pair_halves(moves)
    moves.sort(key=lambda move: (move['at'], move['order']))
    return moves


def _team_floor(starters):
    return {team: set(five) for team, five in starters.items()}


def resolve(plays, starters, team_of):
    """Walk the game and fill in the one-sided substitutions.

    Returns (moves, notes). Every move whose `enter` or `leave` is still
    None after this is one the feed never stated and nothing in the game
    identified; the stints it touches are marked unresolved.
    """
    teams = sorted(starters)
    moves = substitutions(plays, teams)
    by_moment = defaultdict(list)
    for move in moves:
        by_moment[(move['at'], move['order'])].append(move)
    floor = _team_floor(starters)
    waiting_entry = defaultdict(list)
    waiting_exit = defaultdict(list)
    notes = []

    def fill_entry(team, player):
        queue = waiting_entry.get(team)
        if not queue:
            return False
        move = queue.pop(0)
        move['enter'] = player
        move['inferred'] = 'entrant from the short floor'
        floor[team].add(player)
        return True

    def fill_exit(team, player):
        queue = waiting_exit.get(team)
        if not queue:
            return False
        move = queue.pop(0)
        move['leave'] = player
        move['inferred'] = 'exit from the long floor'
        floor[team].discard(player)
        return True

    for index, play in enumerate(plays):
        kind = str(play.get('type_text'))
        if kind == SUBSTITUTION:
            for move in by_moment.get(
                    (elapsed(play.get('period_number'),
                             play.get('clock_display_value')), index), ()):
                team = move['team']
                if team is None:
                    team = team_of.get(move['enter']) \
                        or team_of.get(move['leave'])
                    move['team'] = team
                if team not in floor:
                    notes.append('substitution for an unknown team at %s'
                                 % move['at'])
                    continue
                leave, enter = move['leave'], move['enter']
                if leave is not None and leave not in floor[team]:
                    fill_entry(team, leave)
                if leave is None:
                    waiting_exit[team].append(move)
                else:
                    floor[team].discard(leave)
                if enter is not None and enter in floor[team]:
                    fill_exit(team, enter)
                if enter is None:
                    waiting_entry[team].append(move)
                else:
                    floor[team].add(enter)
            continue
        for slot in ('athlete_id_1', 'athlete_id_2', 'athlete_id_3'):
            player = _identifier(play.get(slot))
            if player is None:
                continue
            team = team_of.get(player)
            if team in floor and player not in floor[team]:
                fill_entry(team, player)

    outstanding = list(waiting_entry.items()) + list(waiting_exit.items())
    for team, queue in outstanding:
        for move in queue:
            notes.append('a substitution at %s for team %s named only one '
                         'player and the game never identified the other'
                         % (move['at'], team))
    return moves, notes


def _snapshot(floor):
    return {team: frozenset(five) for team, five in floor.items()}


def intervals(plays, starters, team_of):
    """The stints: a span of game clock with one five on each side.

    Returns (stints, notes). Each stint carries `start`, `end`, `period`,
    `lineups` and `resolved`.
    """
    moves, notes = resolve(plays, starters, team_of)
    last_period = 1
    for play in plays:
        period = int(play.get('period_number') or 1)
        last_period = max(last_period, period)
    boundaries = {}
    for period in range(1, last_period + 1):
        boundaries[period_start(period)] = period
        boundaries[period_start(period) + period_length(period)] = period
    for move in moves:
        boundaries.setdefault(move['at'], move['period'])
    marks = sorted(boundaries)

    floor = _team_floor(starters)
    unresolved = 0
    stints = []
    pending = list(moves)
    for index, mark in enumerate(marks[:-1]):
        while pending and pending[0]['at'] <= mark:
            move = pending.pop(0)
            team = move['team']
            if team not in floor:
                continue
            if move['leave'] is None or move['enter'] is None:
                unresolved += 1
            if move['leave'] is not None:
                floor[team].discard(move['leave'])
            if move['enter'] is not None:
                floor[team].add(move['enter'])
        end = marks[index + 1]
        if end <= mark:
            continue
        sized = all(len(five) == LINEUP_SIZE for five in floor.values())
        stints.append({'start': mark, 'end': end,
                       'period': boundaries[mark],
                       'lineups': _snapshot(floor),
                       'resolved': sized and not unresolved})
    return stints, notes


def _stint_at(stints, when, period):
    for index, stint in enumerate(stints):
        if stint['period'] == int(period) \
                and stint['start'] <= when <= stint['end']:
            return index
    return None


def measure(plays, stints, teams, *, home, away):
    """Points and possessions for each stint, from the plays themselves.

    Points come from the running score, so they are what the scoreboard
    showed; possessions are counted as runs of the offensive team.

    A POSSESSION IS CREDITED WHERE ITS POINTS WERE SCORED, and, when it
    scored none, where it ended. A substitution inside a possession splits
    it between two fives, and no single choice is right; what a rate fit
    cannot survive is the points and the possession landing in different
    stints, which is a stint holding points with nothing to divide them
    by. Crediting the first play of the run left 325 such stints in
    2017-18 and crediting the last left 488, because the run that scores
    often runs on past the substitution through a free throw and a
    rebound. Crediting the scoring play itself leaves none of them.
    """
    points = [{team: 0.0 for team in teams} for _ in stints]
    possessions = [{team: 0 for team in teams} for _ in stints]
    score = {team: 0.0 for team in teams}
    opening = [None] * len(stints)
    run = None
    run_period = None
    run_at = None
    run_scored_at = None

    def credit():
        where = run_scored_at if run_scored_at is not None else run_at
        if run is not None and where is not None:
            possessions[where][run] += 1

    for play in plays:
        period = int(play.get('period_number') or 1)
        when = elapsed(period, play.get('clock_display_value'))
        if when is None:
            continue
        index = _stint_at(stints, when, period)
        offense = offensive_team(play, teams)
        if offense is not None:
            if run_period != period:
                credit()
                run, run_at, run_scored_at = None, None, None
                run_period = period
            if offense != run:
                credit()
                run, run_at, run_scored_at = offense, index, None
            elif index is not None:
                run_at = index
            if run_scored_at is None and bool(play.get('scoring_play')) \
                    and index is not None:
                run_scored_at = index
        # THE SCORE COLUMNS ALREADY CONTAIN THE PLAY, so a stint's opening
        # score is the running score BEFORE its first play is applied.
        # Snapshotting after would drop that play's points from the stint.
        if index is not None and opening[index] is None:
            opening[index] = dict(score)
        hs = _number(play.get('home_score'))
        aws = _number(play.get('away_score'))
        if hs is not None:
            score[home] = hs
        if aws is not None:
            score[away] = aws
        if index is None:
            continue
        points[index] = {team: score[team] - opening[index][team]
                         for team in teams}
    credit()
    return points, possessions


def minutes_played(stints):
    """Minutes on the floor for every player, from the stints alone."""
    played = defaultdict(float)
    for stint in stints:
        span = (stint['end'] - stint['start']) / 60.0
        for five in stint['lineups'].values():
            for player in five:
                played[player] += span
    return dict(played)


def quality(stint, points, possessions, *, adrift=()):
    """One status for a stint: what a reader should and should not trust.

    `adrift` is the players whose rebuilt minutes disagree with the box
    score. A stint holding one of them reads as five against five and is
    still wrong, so it is called out rather than left looking verified.
    """
    if not stint['resolved']:
        return UNRESOLVED
    if adrift and any(player in adrift
                      for five in stint['lineups'].values()
                      for player in five):
        return MINUTES_UNRECONCILED
    for team, scored in points.items():
        if scored and not possessions.get(team):
            return SCORED_WITHOUT_POSSESSION
    return VERIFIED
