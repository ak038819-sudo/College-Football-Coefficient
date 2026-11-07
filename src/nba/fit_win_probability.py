"""Fitting the NBA's live win-probability curve on real in-game states.

Five parameters, and what each one is measured against:

    points_per_elo  NOT fitted here. Read from `config/nba_model.json`,
                    because it is the Elo model's own answer to what a rating
                    point is worth in points, and a second estimate of it
                    would be a second answer to one question.
    sigma           The spread of the REGULATION margin about what the rating
                    implied, in points, at tip-off. A residual standard
                    deviation, not a search.
    overtime_home_win
                    Counted, not fitted: the share of games level after four
                    periods that the home team went on to win.
    tie_half_width  Solved, not assumed: the band whose normal mass at
                    tip-off equals how often games really do end regulation
                    level. Basketball margins spread far more smoothly than
                    football's, so this comes out small — which is a
                    measurement, not a reason to drop the term.
    time_exponent   FITTED, on real in-game states. A pure random walk is
                    0.5 and the fit is free to disagree.

REGULATION MARGIN, NOT FINAL MARGIN. A game that reached overtime was level
after four periods whatever the final score says, and the walk this model runs
ends at the end of regulation. Fitting on final margins would teach it that
tied games end 118-112.

THE STATES ARE THE MARGIN CURVE `src/nba/plays.py` stores: the score at
moments the clock really showed, thinned to one per two shot clocks. Overtime
states are left out, because regulation seconds remaining is 0 throughout
overtime and the model is explicit that it has nothing to say there.

A COARSE-TO-FINE SEARCH, NOT ONE FLAT GRID. The NFL fit searches 161
exponents over 135,000 drive states. There are ten times as many states here
— basketball has no drives, so the curve is sampled on the clock — and 161
passes over 1.4 million states is twenty minutes of erf calls for an answer
three decimal places wide. The coarse pass steps 0.02 and the fine pass steps
0.002 either side of its winner, which reaches the same place in a sixtieth
of the work. The edge check is against the COARSE grid, because that is the
boundary a fit can be pinned against.

    python -m src.nba.fit_win_probability
    python -m src.nba.fit_win_probability --holdout-from 2022 --write
"""

import argparse
import json
import math
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

from .identity import current_season
from .store import connect
from .win_probability import (CONFIG_PATH, REGULATION_PERIODS,
                              REGULATION_SECONDS, WinProbabilityParams,
                              home_win_probability, tie_mass)

MODEL_PATH = Path('config/nba_model.json')
DEFAULT_HOLDOUT_FROM = 2022
FIRST_SEASON = 2002

COARSE_LOW, COARSE_HIGH, COARSE_STEP = 0.20, 1.00, 0.02
FINE_STEP = 0.002
ELO_SCALE = 400.0


def elo_diff_from_expectation(expected, *, scale=ELO_SCALE):
    """The venue-adjusted pregame rating gap, read back off the expectation.

    The rating engine already applied the home-court term and whatever else
    it applies when it wrote `home_expected`, so inverting the logistic
    recovers exactly the gap the model used. Re-deriving it from the two
    pregame ratings would be a second implementation of the venue rule, and
    the two would drift.
    """
    if expected is None or not 0.0 < expected < 1.0:
        return None
    return -scale * math.log10(1.0 / expected - 1.0)


def load_games(connection, *, first=FIRST_SEASON, through=None):
    """One row per finished game: the gap, the regulation margin, the result.

    `game_shape.periods` is what says a game went to overtime: the NBA
    `game` table carries no overtime column, because the schedule feed it is
    built from does not say. A game with no reduced shape is therefore left
    out rather than assumed to have ended in regulation — it has no states
    to fit on either way.
    """
    games = {}
    sql = """
        SELECT g.game_key, g.season, g.home_points, g.away_points,
               s.periods, r.home_expected
          FROM game g
          JOIN rating_game r ON r.game_key = g.game_key
          JOIN game_shape s ON s.game_key = g.game_key
         WHERE g.home_points IS NOT NULL AND g.away_points IS NOT NULL
           AND r.home_expected IS NOT NULL AND g.season >= ?
           AND s.reconciled = 1
         ORDER BY g.season, g.game_date, g.game_key
    """
    for key, season, home, away, periods, expected in connection.execute(
            sql, (first,)):
        if through is not None and season > through:
            continue
        diff = elo_diff_from_expectation(expected)
        if diff is None:
            continue
        final = home - away
        overtime = periods > REGULATION_PERIODS
        games[key] = dict(
            game_key=key, season=season, elo_diff=diff,
            regulation_margin=0 if overtime else final,
            went_ot=overtime,
            home_result=0.5 if final == 0 else (1.0 if final > 0 else 0.0))
    return games


def load_states(connection, games):
    """Every stored curve point as a state, overtime and the buzzer left out."""
    states = []
    sql = """
        SELECT game_key, period, seconds_left, home_score, away_score
          FROM game_state
         ORDER BY game_key, sequence
    """
    for key, period, secs, home, away in connection.execute(sql):
        game = games.get(key)
        if game is None:
            continue
        if period > REGULATION_PERIODS or secs <= 0:
            continue
        states.append(dict(game_key=key, season=game['season'],
                           elo_diff=game['elo_diff'], margin=home - away,
                           seconds_left=secs,
                           home_result=game['home_result']))
    return states


def fit_sigma(rows, points_per_elo):
    """The residual spread of the regulation margin about the rating's call."""
    residuals = [row['regulation_margin'] - points_per_elo * row['elo_diff']
                 for row in rows]
    if len(residuals) < 2:
        raise ValueError('Not enough games to measure a spread')
    return statistics.pstdev(residuals)


def overtime_home_share(rows):
    """Of the games level after four periods, the share the home team won."""
    level = [row for row in rows if row['regulation_margin'] == 0]
    if not level:
        raise ValueError('No games ended regulation level')
    return sum(row['home_result'] for row in level) / len(level), len(level)


def solve_tie_half_width(rows, points_per_elo, sigma, observed):
    """The band whose tip-off tie mass matches how often it happens.

    Bisection rather than a formula: the mass depends on the rating gap of
    every game, so it is averaged over the real fixtures rather than taken
    at a single expected margin.
    """
    def mass(width):
        params = WinProbabilityParams(
            points_per_elo=points_per_elo, sigma=sigma, time_exponent=0.5,
            overtime_home_win=0.5, tie_half_width=width)
        return sum(tie_mass(points_per_elo * row['elo_diff'], sigma, params)
                   for row in rows) / len(rows)

    low, high = 0.0, 6.9
    if mass(high) < observed:
        return high
    for _ in range(80):
        middle = (low + high) / 2.0
        if mass(middle) < observed:
            low = middle
        else:
            high = middle
    return (low + high) / 2.0


def prepare(states, points_per_elo):
    """States as `(expected_without_time, fraction_left, result)` tuples.

    The search evaluates the same million states sixty times, so the parts
    that do not depend on `time_exponent` are computed once. `mu` is the
    whole-game margin the rating implies; the expected final margin at a
    state is `margin + mu * f`, which is what the shared model computes.
    """
    rows = []
    for state in states:
        f = state['seconds_left'] / REGULATION_SECONDS
        if f <= 0:
            continue
        rows.append((state['margin'] + points_per_elo * state['elo_diff'] * f,
                     f, state['home_result']))
    return rows


_ROOT_TWO = math.sqrt(2.0)


def fast_brier(prepared, sigma, exponent, overtime, tie_width):
    """The Brier score of one parameter set over prepared states.

    Arithmetically the same thing as calling `home_win_probability` on every
    state — `test_nba_win_probability.py` asserts the two agree to twelve
    decimal places, so this stays a speed-up rather than a second model.
    """
    total = 0.0
    erf = math.erf
    for expected, f, result in prepared:
        spread = sigma * (f ** exponent)
        upper = 0.5 * (1.0 + erf((tie_width - expected) / (spread * _ROOT_TWO)))
        lower = 0.5 * (1.0 + erf((-tie_width - expected) / (spread * _ROOT_TWO)))
        probability = (1.0 - upper) + (upper - lower) * overtime
        if probability < 0.0:
            probability = 0.0
        elif probability > 1.0:
            probability = 1.0
        total += (probability - result) ** 2
    return total / len(prepared)


def brier(states, params):
    """The published model's own score, state by state. The slow, honest one."""
    total = 0.0
    for state in states:
        probability = home_win_probability(
            state['elo_diff'], state['margin'], state['seconds_left'], params)
        total += (probability - state['home_result']) ** 2
    return total / len(states)


def search_time_exponent(prepared, sigma, overtime, tie_width, *,
                         low=COARSE_LOW, high=COARSE_HIGH, step=COARSE_STEP,
                         fine=FINE_STEP):
    """Coarse then fine. Returns (exponent, score, at_edge, coarse curve)."""
    def scan(start, stop, size):
        best, curve = None, []
        count = int(round((stop - start) / size))
        for index in range(count + 1):
            exponent = round(start + index * size, 6)
            score = fast_brier(prepared, sigma, exponent, overtime, tie_width)
            curve.append((exponent, score))
            if best is None or score < best[1]:
                best = (exponent, score)
        return best, curve

    coarse, curve = scan(low, high, step)
    at_edge = coarse[0] in (curve[0][0], curve[-1][0])
    if at_edge:
        return coarse[0], coarse[1], True, curve
    fine_best, _ = scan(max(low, coarse[0] - step), min(high, coarse[0] + step),
                        fine)
    return fine_best[0], fine_best[1], False, curve


def calibration_error(states, params, buckets=10):
    """Mean absolute gap between predicted and observed, by bin."""
    sums = [[0.0, 0.0, 0] for _ in range(buckets)]
    for state in states:
        probability = home_win_probability(
            state['elo_diff'], state['margin'], state['seconds_left'], params)
        index = min(buckets - 1, int(probability * buckets))
        sums[index][0] += probability
        sums[index][1] += state['home_result']
        sums[index][2] += 1
    gaps = [abs(predicted - observed) / count
            for predicted, observed, count in sums if count]
    return sum(gaps) / len(gaps) if gaps else 0.0


def pregame_brier(states, params):
    """The same model with the clock stopped at tip-off: the honest baseline.

    This is the number a scoreboard shows today — the pregame chance, never
    updated. A live model that cannot beat it has bought nothing.
    """
    total = 0.0
    for state in states:
        probability = home_win_probability(
            state['elo_diff'], 0.0, REGULATION_SECONDS, params)
        total += (probability - state['home_result']) ** 2
    return total / len(states)


def fit(connection, *, holdout_from=DEFAULT_HOLDOUT_FROM, through=None,
        points_per_elo=None, grid=(COARSE_LOW, COARSE_HIGH, COARSE_STEP)):
    if points_per_elo is None:
        points_per_elo = float(json.loads(
            MODEL_PATH.read_text(encoding='utf-8'))['elo']['points_per_elo'])
    games = load_games(connection, through=through)
    states = load_states(connection, games)
    fit_games = [row for row in games.values() if row['season'] < holdout_from]
    fit_states = [row for row in states if row['season'] < holdout_from]
    held_states = [row for row in states if row['season'] >= holdout_from]
    if not fit_games or not fit_states or not held_states:
        raise ValueError('Nothing to fit or nothing held out')

    sigma = fit_sigma(fit_games, points_per_elo)
    overtime, level_games = overtime_home_share(fit_games)
    observed_tie_rate = level_games / len(fit_games)
    tie_width = solve_tie_half_width(fit_games, points_per_elo, sigma,
                                     observed_tie_rate)
    exponent, fit_score, at_edge, curve = search_time_exponent(
        prepare(fit_states, points_per_elo), sigma, overtime, tie_width,
        low=grid[0], high=grid[1], step=grid[2])
    params = WinProbabilityParams(
        points_per_elo=points_per_elo, sigma=sigma, time_exponent=exponent,
        overtime_home_win=overtime, tie_half_width=tie_width)
    return dict(
        params=params, at_edge=at_edge, coarse_grid=(grid[0], grid[1], grid[2]),
        fit_seasons=sorted({row['season'] for row in fit_games}),
        holdout_seasons=sorted({row['season'] for row in held_states}),
        fit_games=len(fit_games), fit_states=len(fit_states),
        holdout_states=len(held_states),
        observed_tie_rate=observed_tie_rate, level_games=level_games,
        fit_brier=fit_score,
        holdout_brier=brier(held_states, params),
        holdout_pregame_brier=pregame_brier(held_states, params),
        holdout_calibration=calibration_error(held_states, params),
        random_walk_brier=brier(held_states, WinProbabilityParams(
            points_per_elo=points_per_elo, sigma=sigma, time_exponent=0.5,
            overtime_home_win=overtime, tie_half_width=tie_width)))


def as_config(result):
    params = result['params']
    return {
        'win_probability': {
            'points_per_elo': round(params.points_per_elo, 9),
            'sigma': round(params.sigma, 4),
            'time_exponent': round(params.time_exponent, 4),
            'overtime_home_win': round(params.overtime_home_win, 4),
            'tie_half_width': round(params.tie_half_width, 4),
        },
        'validation': {
            'fit_seasons': result['fit_seasons'],
            'fit_games': result['fit_games'],
            'fit_states': result['fit_states'],
            'holdout_seasons': result['holdout_seasons'],
            'holdout_states': result['holdout_states'],
            'holdout': {
                'brier': round(result['holdout_brier'], 5),
                'pregame_brier': round(result['holdout_pregame_brier'], 5),
                'random_walk_brier': round(result['random_walk_brier'], 5),
                'calibration_error': round(result['holdout_calibration'], 5),
            },
            'measured_by': (
                'src/nba/fit_win_probability.py against the margin curve '
                'src/nba/plays.py reduces from the ESPN play-by-play; '
                'reproducible with `python -m src.nba.plays && '
                'python -m src.nba.fit_win_probability`'),
            'note': (
                'points_per_elo is NOT fitted here: it is read from '
                'config/nba_model.json, because it is the Elo model’s own '
                'answer to what a rating point is worth. sigma is the spread '
                'of the regulation margin about that call; overtime_home_win '
                'is counted, not fitted; tie_half_width is solved so the '
                'model’s tip-off tie mass matches how often NBA games really '
                'end regulation level. Only time_exponent is searched, and '
                'it is searched on real in-game states rather than assumed '
                'to be the random walk’s 0.5. Regulation is 2880 seconds '
                'here, not football’s 3600. The held-out numbers are what '
                'that run reported; pregame_brier is the same model with the '
                'clock stopped at tip-off, which is the number a scoreboard '
                'shows today.'),
        },
    }


def render(result):
    params = result['params']
    return '\n'.join([
        f'{result["fit_states"]:,} states fitted over '
        f'{result["fit_seasons"][0]}-{result["fit_seasons"][-1]} '
        f'({result["fit_games"]:,} games), '
        f'{result["holdout_states"]:,} held out over '
        f'{result["holdout_seasons"][0]}-{result["holdout_seasons"][-1]}',
        f'  points_per_elo    {params.points_per_elo:.6f}  (from the Elo fit)',
        f'  sigma             {params.sigma:.4f} points',
        f'  time_exponent     {params.time_exponent:.4f}'
        f'{"  AT THE GRID EDGE" if result["at_edge"] else ""}',
        f'  overtime_home_win {params.overtime_home_win:.4f}  '
        f'({result["level_games"]} games level after four periods, '
        f'{result["observed_tie_rate"]:.4f} of the fit)',
        f'  tie_half_width    {params.tie_half_width:.4f} points',
        f'held out: Brier {result["holdout_brier"]:.5f}, '
        f'calibration {result["holdout_calibration"]:.5f}',
        f'  vs pregame held constant   {result["holdout_pregame_brier"]:.5f}',
        f'  vs a pure random walk      {result["random_walk_brier"]:.5f}',
    ])


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Fit the NBA live win-probability curve')
    parser.add_argument('--db', default=None)
    parser.add_argument('--holdout-from', type=int,
                        default=DEFAULT_HOLDOUT_FROM)
    parser.add_argument('--through', type=int, default=None,
                        help='last season to use; the season in progress is '
                             'excluded by default')
    parser.add_argument('--grid', default=None,
                        help='low,high,step for the coarse exponent search')
    parser.add_argument('--write', action='store_true',
                        help=f'commit the fit to {CONFIG_PATH}')
    args = parser.parse_args(argv)
    grid = ((COARSE_LOW, COARSE_HIGH, COARSE_STEP) if not args.grid
            else tuple(float(part) for part in args.grid.split(',')))
    connection = connect(args.db)
    try:
        through = args.through
        if through is None:
            # THE SEASON BEFORE THE ONE IN PROGRESS, by the calendar, not
            # "the newest season whose every game has a score". The NFL fit
            # asks the second question and gets the right answer; asking it
            # here answered 2019, because the NBA schedule KEEPS the games
            # COVID cancelled in 2020 and a cancelled game has no score for
            # ever. That silently threw away six seasons and then failed
            # because nothing was left to hold out.
            through = current_season(
                datetime.now(timezone.utc)) - 1
            latest = connection.execute(
                'SELECT MAX(season) FROM game WHERE home_points IS NOT NULL'
            ).fetchone()[0]
            if latest is not None:
                through = min(through, latest)
        result = fit(connection, holdout_from=args.holdout_from,
                     through=through, grid=grid)
    finally:
        connection.close()
    print(render(result))
    if result['at_edge']:
        print('REFUSING to write: the exponent landed on the edge of the '
              'coarse grid, which is the grid’s boundary and not a fitted '
              'parameter. Widen it with --grid.', file=sys.stderr)
        return 1
    if args.write:
        CONFIG_PATH.write_text(
            json.dumps(as_config(result), indent=2, sort_keys=True) + '\n',
            encoding='utf-8')
        print(f'Written to {CONFIG_PATH}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
