"""The NBA team statistics: possessions, PxP+ and the opportunity grades.

ONE MODULE DIVIDES. `src/nba/schema.sql` stores counts and nothing derived,
and every rate, index and grade in the export comes from here — the season
board, the game page and any trial replay all read these functions rather
than repeating the arithmetic. A second implementation of PxP+ would differ
from this one the first time the league denominator moved, and both would
look right.

THE DENOMINATOR IS SHARED. Each side's possessions are estimated as
`FGA + 0.44 x FTA - ORB + TOV`, and the two estimates are AVERAGED into one
number P used for both sides of that game. The two sides of a basketball
game take very nearly the same number of possessions, so the average is the
better estimate of the quantity they share; and a shared denominator is what
makes a game's two grades cancel, which is the property the whole matchup
expectation rests on.

A MISSING COUNT COSTS THE WHOLE GAME, BOTH SIDES. P cannot be estimated
without all four counts, and P is shared, so a game where one side is
missing a turnover count is unmeasurable for its opponent too. Measured on
2025-26: one game of 1,231 (401810169, Chicago at Orlando, 2025-12-01, no
`total_turnovers` for Chicago). It is reported as unmeasured, never imputed.

CHANCES ARE AN ESTIMATE AND SAY SO. `C = P + ORB - TOV` counts the shooting
opportunities a team generated or preserved, on top of an estimated P. The
word "estimated" belongs on every label a page draws from it.

WHAT MULTIPLIES BACK. Chance creation index x points per chance index / 100
IS PxP+, exactly, because the chance terms cancel. It holds at full
precision only: round for display and the identity fails in the third
decimal, which is why nothing here rounds and `export` rounds once at the
edge.

THE EXPECTATION USES ONLY EARLIER GAMES, AND IS FROZEN BY DATE. A game's
expected opportunity rate is built from what both clubs and the league had
done BEFORE it. The archive carries a date and a tip-off time, but a
reader's replay is reproducible only if it does not depend on resolving
times within a day, so every game on a date is expected from the state
BEFORE that date's first game and the whole date is then ingested. Truncate
the replay anywhere and the earlier expectations are untouched.

PILOT SETTINGS, NOT FITTED ONES. The prior exposures below are the trial's
and are stated in the export so a reader is never told a tuned number is a
measured one.
"""

from collections import defaultdict

# The possession estimate's free-throw coefficient. Not fitted here: it is
# the long-standing public estimate of the share of free-throw attempts that
# end a possession, and the trial this implements used it.
FREE_THROW_WEIGHT = 0.44

# The counts a possession estimate needs. A row short of any one of them is
# unmeasurable, and so is its opponent.
#
# THE TURNOVER COUNT IS `possession_turnovers`, NOT `total_turnovers`. Which
# of the feed's three turnover columns carries the team's true count changes
# between eras — `src/nba/team_box.py` trap 3 — and that archaeology is
# settled once, at load time, by a measurement over the whole season. This
# module divides; it does not decide which column means what.
TURNOVERS = 'possession_turnovers'
REQUIRED = ('field_goals_attempted', 'free_throws_attempted',
            'offensive_rebounds', TURNOVERS)

# PILOT PRIORS, exposure-weighted, in possessions.
#
# A club starts each season with this much exposure at the current league
# rate, so its first games move its profile without defining it; the league
# starts with this much at the PREVIOUS season's rate, so an expectation
# exists on opening night. Both are trial settings carried over from the
# replay, not quantities measured on this archive, and the export says so.
TEAM_PRIOR_POSSESSIONS = 1500.0
LEAGUE_PRIOR_POSSESSIONS = 6000.0

# An opportunity rate is centred on 100 so that it reads like the indexes
# beside it: 100 is break-even, and one point is one net opportunity per
# hundred possessions.
RATE_BASE = 100.0


def _number(value):
    """A float, or None. A bool is not a number here."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # NaN is not a count


def measurable(row):
    """Whether one side's counts can produce a possession estimate."""
    if not row:
        return False
    return all(_number(row.get(name)) is not None for name in REQUIRED)


def side_possessions(row):
    """One side's possession estimate, or None when a count is missing.

    This is NOT the denominator anything is divided by: `game_possessions`
    averages the two sides into the one number both of them use.
    """
    if not measurable(row):
        return None
    return (_number(row['field_goals_attempted'])
            + FREE_THROW_WEIGHT * _number(row['free_throws_attempted'])
            - _number(row['offensive_rebounds'])
            + _number(row[TURNOVERS]))


def game_possessions(one, other):
    """The shared denominator P for a game, or None if either side is short.

    Returned as None rather than as the one side that is measurable: the
    whole point of P is that both sides are divided by it, and half a
    denominator is not a smaller version of that, it is a different
    quantity.
    """
    first, second = side_possessions(one), side_possessions(other)
    if first is None or second is None:
        return None
    total = (first + second) / 2.0
    return total if total > 0 else None


def chances(row, possessions):
    """Estimated shooting chances `C = P + ORB - TOV`, or None."""
    if possessions is None or not measurable(row):
        return None
    return (possessions + _number(row['offensive_rebounds'])
            - _number(row[TURNOVERS]))


def opportunity_rate(row, possessions):
    """`100 + 100 x (ORB - TOV) / P`, the rate the matchup grades compare.

    Centred on 100, so a team that takes as many extra chances as it gives
    away reads 100 and the expectation arithmetic stays additive.
    """
    if possessions is None or not measurable(row):
        return None
    edge = _number(row['offensive_rebounds']) - _number(row[TURNOVERS])
    return RATE_BASE + RATE_BASE * edge / possessions


def _index(team_rate, league_rate):
    """`100 x team / league`, or None where either side cannot be formed."""
    if team_rate is None or league_rate in (None, 0):
        return None
    return RATE_BASE * team_rate / league_rate


class Totals:
    """Summed counts and exposure for one club, or for the league.

    Summed COUNTS, then divided once. A season rate is not the mean of its
    games' rates: a 120-possession game and a 90-possession one do not carry
    the same weight in a per-possession number, and averaging the rates
    would weight them as if they did.
    """

    __slots__ = ('games', 'possessions', 'points', 'offensive_rebounds',
                 'turnovers', 'opponent_offensive_rebounds',
                 'opponent_turnovers', 'chances')

    def __init__(self):
        self.games = 0
        self.possessions = 0.0
        self.points = 0.0
        self.offensive_rebounds = 0.0
        self.turnovers = 0.0
        self.opponent_offensive_rebounds = 0.0
        self.opponent_turnovers = 0.0
        self.chances = 0.0

    def add(self, row, opponent, possessions):
        self.games += 1
        self.possessions += possessions
        points = _number(row.get('points'))
        if points is not None:
            self.points += points
        self.offensive_rebounds += _number(row['offensive_rebounds'])
        self.turnovers += _number(row[TURNOVERS])
        self.opponent_offensive_rebounds += _number(
            opponent['offensive_rebounds'])
        self.opponent_turnovers += _number(opponent[TURNOVERS])
        self.chances += chances(row, possessions)

    @property
    def scoring_rate(self):
        if self.possessions <= 0:
            return None
        return self.points / self.possessions

    @property
    def chance_rate(self):
        """Estimated chances per possession."""
        if self.possessions <= 0:
            return None
        return self.chances / self.possessions

    @property
    def points_per_chance(self):
        if self.chances <= 0:
            return None
        return self.points / self.chances

    @property
    def offensive_rate(self):
        """The club's own opportunity rate over the exposure summed here."""
        if self.possessions <= 0:
            return None
        edge = self.offensive_rebounds - self.turnovers
        return RATE_BASE + RATE_BASE * edge / self.possessions

    @property
    def allowed_rate(self):
        """The opportunity rate this club's OPPONENTS managed against it."""
        if self.possessions <= 0:
            return None
        edge = (self.opponent_offensive_rebounds - self.opponent_turnovers)
        return RATE_BASE + RATE_BASE * edge / self.possessions


def pair_games(rows):
    """Box rows as `(game_key, [row, row])`, in replay order.

    A game with one row is dropped and counted by the caller: PxP+ and every
    grade need the opponent's counts, and a single row is not half a game
    for these purposes, it is none of one. Ordered by DATE then game key, so
    the order is a property of the archive rather than of the query.
    """
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['game_key']].append(row)
    ordered = sorted(grouped.items(),
                     key=lambda item: (item[1][0].get('game_date') or '',
                                       item[0]))
    return [(key, sides) for key, sides in ordered]


def season_rates(rows):
    """Season totals per club and for the league, from the games given.

    Returns `(teams, league, counts)`: a `Totals` per club key, one for the
    league, and what was dropped on the way. Nothing is rounded.
    """
    teams = defaultdict(Totals)
    league = Totals()
    counts = dict(games=0, measured=0, unpaired=0, unmeasured=0)
    for _, sides in pair_games(rows):
        if len(sides) != 2:
            counts['unpaired'] += len(sides)
            continue
        counts['games'] += 1
        one, other = sides
        possessions = game_possessions(one, other)
        if possessions is None:
            counts['unmeasured'] += 1
            continue
        counts['measured'] += 1
        for row, opponent in ((one, other), (other, one)):
            teams[row['team_key']].add(row, opponent, possessions)
            league.add(row, opponent, possessions)
    return dict(teams), league, counts


def team_season_stats(totals, league):
    """One club's published statistics, unrounded.

    `pxp` is scoring efficiency against the league, `cci` and `ppc` are the
    two factors it decomposes into, and `net_opportunities` is the club's
    estimated chance edge per 100 possessions. `None` where a denominator is
    zero: a club with no measured game has no estimate, which is not the
    same as an average one.
    """
    if totals is None or totals.possessions <= 0:
        return None
    pxp = _index(totals.scoring_rate, league.scoring_rate)
    cci = _index(totals.chance_rate, league.chance_rate)
    ppc = _index(totals.points_per_chance, league.points_per_chance)
    edge = ((totals.offensive_rebounds - totals.opponent_offensive_rebounds)
            - (totals.turnovers - totals.opponent_turnovers))
    return dict(
        games=totals.games,
        possessions=totals.possessions,
        pxp=pxp,
        cci=cci,
        ppc=ppc,
        net_opportunities=RATE_BASE * edge / totals.possessions,
        opportunity_rate=totals.offensive_rate,
        allowed_rate=totals.allowed_rate,
        points_per_possession=totals.scoring_rate,
    )


class _Profile:
    """Exposure-weighted running state for one club, priors included."""

    __slots__ = ('possessions', 'offensive_edge', 'allowed_edge', 'games')

    def __init__(self, prior_possessions, prior_rate):
        # The prior is carried as exposure at a RATE, so one real game does
        # not overwrite it and 82 of them leave almost nothing of it.
        self.possessions = prior_possessions
        offset = (prior_rate - RATE_BASE) / RATE_BASE * prior_possessions
        self.offensive_edge = offset
        self.allowed_edge = offset
        self.games = 0

    def rate(self, edge):
        if self.possessions <= 0:
            return None
        return RATE_BASE + RATE_BASE * edge / self.possessions

    @property
    def offensive(self):
        return self.rate(self.offensive_edge)

    @property
    def allowed(self):
        return self.rate(self.allowed_edge)

    def observe(self, row, opponent, possessions):
        self.possessions += possessions
        self.offensive_edge += (_number(row['offensive_rebounds'])
                                - _number(row[TURNOVERS]))
        self.allowed_edge += (_number(opponent['offensive_rebounds'])
                              - _number(opponent[TURNOVERS]))
        self.games += 1


def expected_rate(team_offensive, opponent_allowed, league_rate):
    """`team offensive + opponent allowed - league`, the matchup expectation.

    Additive on rates centred at 100: a club that generates two chances per
    hundred more than the league, against one that allows one more than the
    league, is expected to manage three more. None where any part is absent.
    """
    parts = (team_offensive, opponent_allowed, league_rate)
    if any(part is None for part in parts):
        return None
    return team_offensive + opponent_allowed - league_rate


def replay_expectations(rows, *, league_prior_rate=None,
                        team_prior_possessions=TEAM_PRIOR_POSSESSIONS,
                        league_prior_possessions=LEAGUE_PRIOR_POSSESSIONS):
    """Each game's opportunity grades, from games played before it.

    Returns `{game_key: {team_key: grade}}`. A grade carries the actual rate,
    the expected rate, each side's residual and the net — the difference of
    the two residuals, which is equal and opposite by construction.

    DATE-FROZEN. Every game on a date is expected from the state before that
    date's first game, and the whole date is observed afterwards. So a replay
    truncated at any date leaves every earlier expectation bit-identical,
    and no expectation depends on the order of two games tipping off the
    same evening.

    `league_prior_rate` is the previous season's rate where the caller has
    one. Without it the league prior sits at break-even, which is the
    neutral statement rather than a borrowed number.
    """
    league = _Profile(league_prior_possessions,
                      RATE_BASE if league_prior_rate is None
                      else league_prior_rate)
    profiles = {}
    grades = {}
    pending = []
    current_date = None

    def profile_for(key):
        if key not in profiles:
            profiles[key] = _Profile(team_prior_possessions,
                                     league.offensive or RATE_BASE)
        return profiles[key]

    def flush():
        for row, opponent, possessions in pending:
            profile_for(row['team_key']).observe(row, opponent, possessions)
            league.observe(row, opponent, possessions)
        pending.clear()

    for key, sides in pair_games(rows):
        if len(sides) != 2:
            continue
        date = sides[0].get('game_date')
        if date != current_date:
            flush()
            current_date = date
        possessions = game_possessions(*sides)
        if possessions is None:
            continue
        one, other = sides
        league_rate = league.offensive
        measured, expected = {}, {}
        for row, opponent in ((one, other), (other, one)):
            measured[row['team_key']] = opportunity_rate(row, possessions)
            expected[row['team_key']] = expected_rate(
                profile_for(row['team_key']).offensive,
                profile_for(opponent['team_key']).allowed,
                league_rate)
        residuals = {}
        for team_key, actual in measured.items():
            want = expected[team_key]
            residuals[team_key] = (None if actual is None or want is None
                                   else actual - want)
        entry = {}
        for row, opponent in ((one, other), (other, one)):
            mine = residuals[row['team_key']]
            theirs = residuals[opponent['team_key']]
            entry[row['team_key']] = dict(
                possessions=possessions,
                actual=measured[row['team_key']],
                expected=expected[row['team_key']],
                residual=mine,
                net=(None if mine is None or theirs is None
                     else mine - theirs),
                prior_games=profile_for(row['team_key']).games,
            )
        grades[key] = entry
        for row, opponent in ((one, other), (other, one)):
            pending.append((row, opponent, possessions))
    flush()
    return grades


def game_stats(sides, league_scoring_rate=None):
    """One game's measured numbers for both sides, unrounded.

    `league_scoring_rate` is the league's points per possession for the
    period a page is reading, which is what turns a side's scoring rate into
    a PxP+. Without it the PxP+ is absent rather than computed against the
    game itself, which would make every game average by construction.
    """
    if len(sides) != 2:
        return None
    possessions = game_possessions(*sides)
    if possessions is None:
        return None
    one, other = sides
    out = dict(possessions=possessions, sides={})
    for row, opponent in ((one, other), (other, one)):
        points = _number(row.get('points'))
        rate = None if points is None else points / possessions
        out['sides'][row['team_key']] = dict(
            points=points,
            points_per_possession=rate,
            pxp=_index(rate, league_scoring_rate),
            chances=chances(row, possessions),
            opportunity_rate=opportunity_rate(row, possessions),
            offensive_rebounds=_number(row['offensive_rebounds']),
            turnovers=_number(row[TURNOVERS]),
            opponent_offensive_rebounds=_number(
                opponent['offensive_rebounds']),
            opponent_turnovers=_number(opponent[TURNOVERS]),
        )
    return out


def league_opportunity_rate(rows):
    """One period's league opportunity rate, or None if nothing is measured.

    The quantity `replay_expectations` wants as a prior: a league that gives
    away as many chances as it takes reads 100, and the archive's seasons sit
    a little above it because an offensive rebound is more common than a
    turnover.
    """
    _, league, _ = season_rates(rows)
    return league.offensive_rate


def build_season(rows, *, league_prior_rate=None, record_types=('regular',)):
    """Everything one season's export needs, from its stored box rows.

    Called once a season by `src/nba/export.py`, which hands the per-game
    half to `src/nba/game_export.py`. Two periods, deliberately different
    and each stated in the payload:

    * THE SEASON RATES AGGREGATE `record_types`, which is the regular season.
      The in-season tournament's group and knockout games are scheduled
      regular-season games and belong in the aggregate; the Cup final is
      classified apart by the schedule and is left out of it, the same rule
      the won-lost record already follows.
    * THE PER-GAME GRADES REPLAY EVERY TYPE in date order, so a postseason
      game is graded against expectations built on everything before it
      rather than carrying no grade at all.

    Nothing here is rounded. `export` rounds once, at the edge.
    """
    rated = [row for row in rows if row['season_type'] in record_types]
    teams, league, counts = season_rates(rated)
    stats = {key: team_season_stats(totals, league)
             for key, totals in teams.items()}
    grades = replay_expectations(rows, league_prior_rate=league_prior_rate)
    games = {}
    for key, sides in pair_games(rows):
        if len(sides) != 2:
            continue
        measured = game_stats(sides, league.scoring_rate)
        if measured is not None:
            games[key] = measured
    return dict(
        teams=stats,
        games=games,
        grades=grades,
        counts=counts,
        record_types=list(record_types),
        # WHICH TURNOVER COUNT THESE NUMBERS WERE MEASURED FROM, carried up
        # from the rows so a page can say it. A list rather than a value: a
        # season is one basis today, and a season that somehow held both
        # should show both rather than whichever row was read last.
        turnover_basis=sorted({row.get('turnover_basis') for row in rows
                               if row.get('turnover_basis')}),
        league=dict(
            possessions=league.possessions,
            points=league.points,
            points_per_possession=league.scoring_rate,
            chances_per_possession=league.chance_rate,
            points_per_chance=league.points_per_chance,
            opportunity_rate=league.offensive_rate,
        ),
        priors=dict(
            team_possessions=TEAM_PRIOR_POSSESSIONS,
            league_possessions=LEAGUE_PRIOR_POSSESSIONS,
            league_rate=league_prior_rate,
            free_throw_weight=FREE_THROW_WEIGHT,
        ),
    )
