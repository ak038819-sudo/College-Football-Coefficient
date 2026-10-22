"""Fitting the NFL's live win-probability curve on real in-game states.

Four parameters, and what each one is measured against:

    points_per_elo  NOT fitted here. It is read from `config/nfl_model.json`,
                    because it is the Elo model's own answer to what a rating
                    point is worth in points, and a second estimate of it
                    would be a second answer to one question.
    sigma           The spread of the REGULATION margin about what the rating
                    implied, in points, at kickoff. A residual standard
                    deviation, not a search.
    overtime_home_win
                    Counted, not fitted: the share of games level after four
                    quarters that the home team went on to win.
    tie_half_width  Solved, not assumed: the band whose normal mass at kickoff
                    equals how often games really do end regulation level.
                    Football margins pile up on 0, 3 and 7 rather than
                    spreading smoothly, so the geometric half point
                    understates it.
    time_exponent   FITTED, on real in-game states. This is the parameter the
                    college side could not measure and recorded as an
                    assumption of 0.5; here every drive of every game since
                    1999 is a state with a score, a clock and a known outcome,
                    so it is searched rather than assumed.

REGULATION MARGIN, NOT FINAL MARGIN. A game that reached overtime was level
after four quarters whatever the final score says, and the walk this model
runs ends at the end of regulation. Fitting on final margins would teach it
that tied games end 27-24.

THE STATES ARE DRIVE STARTS, which is what `src/nfl/plays.py` stores. A drive
carries the score and the clock as it began, so it is an in-game state the
archive states rather than one this script interpolates. Overtime drives are
left out: regulation seconds remaining is 0 throughout overtime, and the model
is explicit that it has nothing to say there.

HELD OUT the same way the Elo fit is, and for the same reason: the search sees
1999-2020 and is scored once on 2021-2025. The season in progress is neither
fitted nor scored.

    python -m src.nfl.fit_win_probability
    python -m src.nfl.fit_win_probability --holdout-from 2018 --write
"""

import argparse
import json
import math
import statistics
import sys
from pathlib import Path

from .store import connect
from .win_probability import (CONFIG_PATH, REGULATION_PERIODS,
                              REGULATION_SECONDS, WinProbabilityParams,
                              home_win_probability)
from ..win_probability import tie_mass

MODEL_PATH = Path('config/nfl_model.json')
DEFAULT_HOLDOUT_FROM = 2021
# The season in progress is excluded from both halves: its games are not all
# played, so neither the fit nor the score would mean what it says.
FIRST_SEASON = 1999

# The search over `time_exponent`. A pure random walk is 0.5; the grid runs
# either side of it and is widened by `--grid` if a fit lands on an edge,
# which is a grid boundary rather than a fitted parameter.
GRID_LOW, GRID_HIGH, GRID_STEP = 0.20, 1.00, 0.005


def elo_diff_from_expectation(expected, *, scale=400.0):
    """The venue-adjusted pregame rating gap, read back off the expectation.

    The rating engine already applied the home-field term and whatever else
    it applies when it wrote `home_expected`, so inverting the logistic
    recovers exactly the gap the model used. Re-deriving it from the two
    pregame ratings would be a second implementation of the venue rule, and
    the two would drift.
    """
    if expected is None or not 0.0 < expected < 1.0:
        return None
    return -scale * math.log10(1.0 / expected - 1.0)


def load_games(connection, *, first=FIRST_SEASON, through=None):
    """One row per finished game: the rating gap, the margins and the result."""
    games = {}
    sql = """
        SELECT g.game_key, g.season, g.home_points, g.away_points, g.overtime,
               r.home_expected
          FROM game g
          JOIN rating_game r ON r.game_key = g.game_key
         WHERE g.home_points IS NOT NULL AND g.away_points IS NOT NULL
           AND r.home_expected IS NOT NULL AND g.season >= ?
         ORDER BY g.season, g.game_date, g.game_key
    """
    for key, season, home, away, overtime, expected in connection.execute(
            sql, (first,)):
        if through is not None and season > through:
            continue
        diff = elo_diff_from_expectation(expected)
        if diff is None:
            continue
        final = home - away
        games[key] = dict(
            game_key=key, season=season, elo_diff=diff,
            # Level after four quarters, whatever the final score says.
            regulation_margin=0 if overtime else final,
            went_ot=bool(overtime),
            home_result=0.5 if final == 0 else (1.0 if final > 0 else 0.0))
    return games


def load_states(connection, games):
    """Every drive start as (game, margin, seconds left), overtime left out."""
    states = []
    sql = """
        SELECT game_key, quarter_start, seconds_left, home_score, away_score
          FROM game_drive
         WHERE seconds_left IS NOT NULL AND home_score IS NOT NULL
           AND away_score IS NOT NULL
         ORDER BY game_key, drive
    """
    for key, quarter, secs, home, away in connection.execute(sql):
        game = games.get(key)
        if game is None:
            continue
        if quarter is not None and quarter > REGULATION_PERIODS:
            continue
        if secs <= 0:
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
    """Of the games level after four quarters, the share the home team won.

    A game that stayed level is half a win to each side, exactly as the
    rating engine scores a tie.
    """
    level = [row for row in rows if row['regulation_margin'] == 0]
    if not level:
        raise ValueError('No games ended regulation level')
    return sum(row['home_result'] for row in level) / len(level), len(level)


def solve_tie_half_width(rows, points_per_elo, sigma, observed):
    """The band around zero whose kickoff tie mass matches how often it happens.

    Bisection rather than a formula: the mass depends on the rating gap of
    every game, so it is averaged over the real fixtures rather than taken at
    a single expected margin.
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


def brier(states, params):
    total = 0.0
    for state in states:
        probability = home_win_probability(
            state['elo_diff'], state['margin'], state['seconds_left'], params)
        total += (probability - state['home_result']) ** 2
    return total / len(states)


def search_time_exponent(states, points_per_elo, sigma, overtime, tie_width,
                         *, low=GRID_LOW, high=GRID_HIGH, step=GRID_STEP):
    """The exponent that scores best on the fitting states. Returns the grid too."""
    best, curve = None, []
    steps = int(round((high - low) / step))
    for index in range(steps + 1):
        exponent = round(low + index * step, 6)
        params = WinProbabilityParams(
            points_per_elo=points_per_elo, sigma=sigma,
            time_exponent=exponent, overtime_home_win=overtime,
            tie_half_width=tie_width)
        score = brier(states, params)
        curve.append((exponent, score))
        if best is None or score < best[1]:
            best = (exponent, score)
    return best, curve


def calibration_error(states, params, buckets=10):
    """Mean absolute gap between predicted and observed, over equal-width bins."""
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
    """The same model with the clock stopped at kickoff: the honest baseline.

    This is the number the scoreboard shows today — the pregame chance, never
    updated. A live model that cannot beat it has bought nothing.
    """
    total = 0.0
    for state in states:
        probability = home_win_probability(
            state['elo_diff'], 0.0, REGULATION_SECONDS, params)
        total += (probability - state['home_result']) ** 2
    return total / len(states)


def fit(connection, *, holdout_from=DEFAULT_HOLDOUT_FROM, through=None,
        points_per_elo=None, grid=(GRID_LOW, GRID_HIGH, GRID_STEP)):
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
    (exponent, fit_score), curve = search_time_exponent(
        fit_states, points_per_elo, sigma, overtime, tie_width,
        low=grid[0], high=grid[1], step=grid[2])
    params = WinProbabilityParams(
        points_per_elo=points_per_elo, sigma=sigma, time_exponent=exponent,
        overtime_home_win=overtime, tie_half_width=tie_width)
    at_edge = exponent in (curve[0][0], curve[-1][0])
    return dict(
        params=params, at_edge=at_edge,
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
                'src/nfl/fit_win_probability.py against the drive states '
                'src/nfl/plays.py reduces from the nflverse play-by-play; '
                'reproducible with `python -m src.nfl.plays && '
                'python -m src.nfl.fit_win_probability`'),
            'note': (
                'points_per_elo is NOT fitted here: it is read from '
                'config/nfl_model.json, because it is the Elo model’s own '
                'answer to what a rating point is worth. sigma is the spread '
                'of the regulation margin about that call; overtime_home_win '
                'is counted, not fitted; tie_half_width is solved so the '
                'model’s kickoff tie mass matches how often games really '
                'end regulation level. Only time_exponent is searched, and it '
                'is searched on real drive-start states rather than assumed '
                'to be the random walk’s 0.5. The held-out numbers are '
                'what that run reported; pregame_brier is the same model with '
                'the clock stopped at kickoff, which is the number a '
                'scoreboard shows today.'),
        },
    }


def render(result):
    params = result['params']
    lines = [
        f'{result["fit_states"]:,} drive states fitted over '
        f'{result["fit_seasons"][0]}-{result["fit_seasons"][-1]} '
        f'({result["fit_games"]:,} games), '
        f'{result["holdout_states"]:,} held out over '
        f'{result["holdout_seasons"][0]}-{result["holdout_seasons"][-1]}',
        f'  points_per_elo    {params.points_per_elo:.6f}  (from the Elo fit)',
        f'  sigma             {params.sigma:.4f} points',
        f'  time_exponent     {params.time_exponent:.4f}'
        f'{"  AT THE GRID EDGE" if result["at_edge"] else ""}',
        f'  overtime_home_win {params.overtime_home_win:.4f}  '
        f'({result["level_games"]} games level after four quarters, '
        f'{result["observed_tie_rate"]:.4f} of the fit)',
        f'  tie_half_width    {params.tie_half_width:.4f} points',
        f'held out: Brier {result["holdout_brier"]:.5f}, '
        f'calibration {result["holdout_calibration"]:.5f}',
        f'  vs pregame held constant   {result["holdout_pregame_brier"]:.5f}',
        f'  vs a pure random walk      {result["random_walk_brier"]:.5f}',
    ]
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Fit the NFL live win-probability curve')
    parser.add_argument('--db', default=None)
    parser.add_argument('--holdout-from', type=int,
                        default=DEFAULT_HOLDOUT_FROM)
    parser.add_argument('--through', type=int, default=None,
                        help='last season to use; the season in progress is '
                             'excluded by default')
    parser.add_argument('--grid', default=None,
                        help='low,high,step for the time_exponent search')
    parser.add_argument('--write', action='store_true',
                        help=f'commit the fit to {CONFIG_PATH}')
    args = parser.parse_args(argv)
    grid = ((GRID_LOW, GRID_HIGH, GRID_STEP) if not args.grid
            else tuple(float(part) for part in args.grid.split(',')))
    connection = connect(args.db)
    try:
        through = args.through
        if through is None:
            latest = connection.execute(
                'SELECT MAX(season) FROM game WHERE home_points IS NOT NULL'
            ).fetchone()[0]
            complete = connection.execute(
                'SELECT season FROM game GROUP BY season '
                'HAVING COUNT(*) = COUNT(home_points) ORDER BY season DESC '
                'LIMIT 1').fetchone()
            through = complete[0] if complete else latest
        result = fit(connection, holdout_from=args.holdout_from,
                     through=through, grid=grid)
    finally:
        connection.close()
    print(render(result))
    if result['at_edge']:
        print('REFUSING to write: the exponent landed on the edge of the '
              'grid, which is the grid’s boundary and not a fitted '
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
