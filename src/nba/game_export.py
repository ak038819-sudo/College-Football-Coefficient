"""The game layer of the NBA export: the line score, the shape and the curve.

ONE FILE A MONTH, `games/<season>/<YYYY-MM>.js`, fetched lazily when a reader
opens a game. The month is taken from the game's own date, which the season
payload already carries, so no index is downloaded to find a game and the page
derives the path with `date.slice(0, 7)` — arithmetic neither side can get
wrong. A season in one file would be 1.4 MB to read one game of it; a day
would be 170 files a season and 4,250 across the archive, which is a lot of
files to carry for an 8 KB saving on a fetch nobody makes twice. A month is
about 190 games and 220 KB, which GitHub Pages serves gzipped at about a fifth
of that.

The manifest states ONE cache token per season rather than one per file, for
the same reason the NFL's does: 175 hashes in a file every reader fetches
would cost more than the one month file a reader who opens a game fetches.

WHAT IS NOT HERE IS THE POINT. The source is 550 MB of play-by-play across 25
seasons, 642,472 rows for one of them, and not one play is stored or
published. A game page shows the line score, how the game went and how the
win probability moved, and those come from about 60 rows a game.

THE WIN PROBABILITY IS COMPUTED HERE, NOT IN THE BROWSER. Every NBA game in
this archive is finished, so there is nothing for a page to recompute as a
score changes, and one implementation of a fitted curve is better than two.

AN OVERTIME STATE IS NOT PRICED. The model's walk runs to the end of
regulation and hands a tie to one measured overtime share, so it has nothing
to say about a state inside overtime: regulation seconds remaining is 0 there
and `home_win_probability` would then answer 1 or 0 by the score alone, which
in the middle of a five-minute period is not an answer. Those points carry a
null probability — the same rule the fit applies to the same states — and the
page draws the curve up to the end of regulation and says so.

RECONCILIATION IS STATED, NOT ASSUMED. `game_shape.reconciled` says whether
the reduction adds up to the final the archive carries, and a game where it
does not carries `ok: 0`: the page then shows the final and says the period
detail is incomplete, rather than drawing a line score that contradicts the
score above it.
"""

import math
from collections import defaultdict

from .win_probability import (REGULATION_PERIODS, REGULATION_SECONDS,
                              home_win_probability, load_params)

# The reasons `plays.key_plays` keeps a play, in the order a page prints the
# legend. The payload carries the index, not the word.
REASONS = ('lead', 'tie', 'late', 'final')

GAME_SQL = """
    SELECT g.game_key, g.game_date, g.home_team_key, g.away_team_key,
           g.home_points, g.away_points, r.home_expected
      FROM game g LEFT JOIN rating_game r ON r.game_key = g.game_key
     WHERE g.season = ?
"""
PERIOD_SQL = """
    SELECT p.game_key, p.period, p.home_points, p.away_points
      FROM game_period p JOIN game g ON g.game_key = p.game_key
     WHERE g.season = ? ORDER BY p.game_key, p.period
"""
SHAPE_SQL = """
    SELECT s.game_key, s.plays, s.scoring_plays, s.periods, s.overtime,
           s.lead_changes, s.ties, s.home_biggest_lead, s.away_biggest_lead,
           s.home_biggest_run, s.away_biggest_run, s.home_time_leading,
           s.away_time_leading, s.time_tied, s.reconciled
      FROM game_shape s JOIN game g ON g.game_key = s.game_key
     WHERE g.season = ?
"""
STATE_SQL = """
    SELECT t.game_key, t.period, t.elapsed, t.seconds_left, t.home_score,
           t.away_score
      FROM game_state t JOIN game g ON g.game_key = t.game_key
     WHERE g.season = ? ORDER BY t.game_key, t.sequence
"""
PLAY_SQL = """
    SELECT k.game_key, k.sequence, k.period, k.clock, k.team_key, k.points,
           k.kind, k.scorer, k.description, k.home_score, k.away_score,
           k.reason
      FROM game_play k JOIN game g ON g.game_key = k.game_key
     WHERE g.season = ? ORDER BY k.game_key, k.sequence
"""

# The shape fields, in the order the payload's `sh` array carries them. The
# page reads them by index, so this tuple IS the contract and the order is
# not cosmetic.
SHAPE_FIELDS = ('lead_changes', 'ties', 'home_biggest_lead',
                'away_biggest_lead', 'home_biggest_run', 'away_biggest_run',
                'home_time_leading', 'away_time_leading', 'time_tied',
                'scoring_plays', 'plays')


def month_key(game):
    """The file a game's detail lives in: `2026-01`. The page derives this.

    A game with no readable date is keyed None rather than guessed into a
    month, and `build` then leaves it out: a game filed under the wrong month
    is a game a reader cannot reach, with no sign that it is missing.
    """
    date = game.get('date') if isinstance(game, dict) else None
    if not isinstance(date, str) or len(date) < 7:
        return None
    return date[:7]


def month_order(months):
    """Month keys in calendar order.

    Load-bearing rather than cosmetic: the season's cache token is the hash
    of its month files concatenated in this order, and a dictionary's
    iteration order would make the token depend on how it was built.
    """
    return sorted(months)


def has_plays(connection):
    """Whether this database carries the game layer at all.

    An export built before the reduction has run must say the layer is
    absent rather than publish empty game files: a page can then say the
    export predates it, instead of telling a reader that no NBA game ever
    played has a line score.
    """
    return connection.execute(
        'SELECT 1 FROM game_shape LIMIT 1').fetchone() is not None


def _side(team_key, home, away):
    """1 for the home side, 0 for the away side, None for neither."""
    if team_key == home:
        return 1
    if team_key == away:
        return 0
    return None


def _per_mille(probability):
    """A probability as an integer 0-1000.

    Three digits is the precision a page can draw, and a tenth of a point is
    already below what the model can claim. Floats would be seventeen
    characters each across 1.4 million curve points.
    """
    return int(round(probability * 1000))


def elo_diff(expected, scale=400.0):
    """The venue-adjusted rating gap, read back off the stored expectation.

    The same inversion the fit uses, and for the same reason: the rating
    engine already applied the home-court term when it wrote `home_expected`,
    and re-deriving it here would be a second implementation of the venue
    rule.
    """
    if expected is None or not 0.0 < expected < 1.0:
        return 0.0
    return -scale * math.log10(1.0 / expected - 1.0)


def _curve(rows, diff, expected, params):
    """Four parallel arrays: elapsed, home, away and win probability.

    PARALLEL ARRAYS, not a list of points. The same 54 points as objects
    would repeat four key names 54 times a game and 1.4 million times across
    the archive; as four arrays of integers they are digits and commas.

    The x-axis is ELAPSED time, not regulation seconds remaining, because
    remaining is 0 throughout overtime and a curve drawn on it would stack
    every overtime point on the same vertical line.
    """
    elapsed, home, away, chance = [], [], [], []
    for row in rows:
        elapsed.append(row['elapsed'])
        home.append(row['home_score'])
        away.append(row['away_score'])
        if (expected is None or row['period'] > REGULATION_PERIODS
                or row['seconds_left'] <= 0):
            chance.append(None)
            continue
        chance.append(_per_mille(home_win_probability(
            diff, row['home_score'] - row['away_score'],
            row['seconds_left'], params)))
    return [elapsed, home, away, chance]


def build(connection, season, *, params=None):
    """A season's game detail as `{month key: payload}`, or None if it has none."""
    if params is None:
        params = load_params()
    games = {}
    for row in connection.execute(GAME_SQL, (season,)):
        games[row['game_key']] = dict(
            date=row['game_date'], home=row['home_team_key'],
            away=row['away_team_key'], home_points=row['home_points'],
            away_points=row['away_points'], expected=row['home_expected'])
    periods = defaultdict(list)
    for row in connection.execute(PERIOD_SQL, (season,)):
        if row['game_key'] in games:
            periods[row['game_key']].append([row['home_points'],
                                             row['away_points']])
    shapes = {}
    for row in connection.execute(SHAPE_SQL, (season,)):
        if row['game_key'] in games:
            shapes[row['game_key']] = row
    states = defaultdict(list)
    for row in connection.execute(STATE_SQL, (season,)):
        if row['game_key'] in games:
            states[row['game_key']].append(row)
    key_plays = defaultdict(list)
    for row in connection.execute(PLAY_SQL, (season,)):
        game = games.get(row['game_key'])
        if game is None:
            continue
        key_plays[row['game_key']].append([
            _side(row['team_key'], game['home'], game['away']),
            row['period'], row['clock'], row['points'], row['scorer'],
            row['description'], row['home_score'], row['away_score'],
            REASONS.index(row['reason']) if row['reason'] in REASONS
            else None, row['kind']])

    months = defaultdict(dict)
    for key, game in games.items():
        shape = shapes.get(key)
        if shape is None and key not in states:
            continue
        month = month_key(game)
        if month is None:
            continue
        diff = elo_diff(game['expected'])
        entry = {}
        if game['expected'] is not None:
            entry['wp'] = _per_mille(home_win_probability(
                diff, 0, REGULATION_SECONDS, params))
        if shape is not None:
            entry['sh'] = [shape[name] for name in SHAPE_FIELDS]
            # `reconciled` is NULL for a game with no final score, which is
            # not the same as 0: nothing disagreed, there was nothing to
            # disagree with.
            entry['ok'] = (None if shape['reconciled'] is None
                           else int(shape['reconciled']))
            entry['ot'] = int(shape['periods'] > REGULATION_PERIODS)
        if key in periods:
            entry['p'] = periods[key]
        if key in states:
            entry['c'] = _curve(states[key], diff, game['expected'], params)
        if key in key_plays:
            entry['k'] = key_plays[key]
        months[month][key] = entry
    if not months:
        return None
    return {month: dict(season=season, month=month, reasons=list(REASONS),
                        shape_fields=list(SHAPE_FIELDS), games=entries)
            for month, entries in months.items()}


def coverage(connection):
    """How far the game layer reaches, for the docs and the export summary."""
    played = connection.execute(
        'SELECT COUNT(*) FROM game WHERE home_points IS NOT NULL'
    ).fetchone()[0]
    return dict(
        played=played,
        with_shape=connection.execute(
            'SELECT COUNT(*) FROM game_shape').fetchone()[0],
        reconciled=connection.execute(
            'SELECT COUNT(*) FROM game_shape WHERE reconciled = 1'
        ).fetchone()[0],
        unreconciled=connection.execute(
            'SELECT COUNT(*) FROM game_shape WHERE reconciled = 0'
        ).fetchone()[0],
        states=connection.execute(
            'SELECT COUNT(*) FROM game_state').fetchone()[0],
        key_plays=connection.execute(
            'SELECT COUNT(*) FROM game_play').fetchone()[0])
