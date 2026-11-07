"""The NBA's in-game win probability: its own parameters, the shared maths.

The model is `src/win_probability.py` — the remaining margin as a random walk
that the pregame rating tilts, with an exact-tie term so the last seconds stay
honest. That module is pure mathematics with no league in it, and a fifth copy
of a fitted statistical model would be a fifth place for one correction to be
missed. What the NBA passes in is its own clock: **four periods of twelve
minutes, 2880 seconds of regulation**, where football plays 3600.

**THE PARAMETERS ARE THE NBA'S OWN AND ARE FITTED HERE.** Nothing is
inherited. `sigma`, `time_exponent`, `tie_half_width` and the overtime share
are measured on NBA games in `fit_win_probability.py` and committed to
`config/nba_win_probability.json`, the same way `config/nba_model.json` holds
the fitted Elo. `points_per_elo` comes from the Elo configuration, because it
is the Elo model's own answer to what a rating point is worth in points and a
second estimate of it here would be a second answer to one question.

**THE TIE TERM MATTERS LESS HERE AND IS STILL NOT ZERO.** Football margins
pile up on 0, 3 and 7; basketball margins spread far more smoothly and a game
that ends regulation level is rare — 4.5% of 2026's games went to overtime.
The band is solved against that observed rate rather than assumed, exactly as
the NFL's is, and it comes out small. Small is a measurement, not a reason to
drop the term: without it a one-point lead with two seconds left and a tie
with two seconds left read the same.

The states this is fitted on are REAL in-game states: the score at moments the
clock really showed, thinned to one per two shot clocks by `src/nba/plays.py`.
"""

import json
from pathlib import Path

from ..win_probability import (WinProbabilityParams, fraction_remaining,
                               home_win_probability as _home_win_probability,
                               tie_mass)

CONFIG_PATH = Path('config/nba_win_probability.json')

# An NBA period is twelve minutes and regulation is four of them. Overtime is
# five minutes and is not part of regulation: the walk this model runs ends at
# the end of regulation, and `plays.seconds_left` is 0 throughout overtime.
PERIOD_SECONDS = 720
REGULATION_PERIODS = 4
REGULATION_SECONDS = PERIOD_SECONDS * REGULATION_PERIODS

__all__ = ['PERIOD_SECONDS', 'REGULATION_PERIODS', 'REGULATION_SECONDS',
           'WinProbabilityParams', 'CONFIG_PATH', 'home_win_probability',
           'pregame_home_win_probability', 'fraction_remaining', 'tie_mass',
           'load_params', 'params_from', 'as_dict']


def home_win_probability(elo_diff, margin, secs_left, params):
    """The shared model with the NBA's clock. `secs_left` is of regulation."""
    return _home_win_probability(elo_diff, margin, secs_left, params,
                                 REGULATION_SECONDS)


def pregame_home_win_probability(elo_diff, params):
    """The same model at tip-off: no score, a whole game to play."""
    return home_win_probability(elo_diff, 0.0, REGULATION_SECONDS, params)


def params_from(config):
    """Parameters from a parsed configuration dict.

    `tie_half_width` is required rather than defaulted, as it is on the NFL
    side. The shared model carries a geometric default of half a point, which
    is the right fallback for a caller that has never measured it and the
    wrong one here: the fit solves it against how often NBA games really end
    regulation level, and falling back silently would publish a different
    curve from the one the config's validation block describes.
    """
    return WinProbabilityParams(
        points_per_elo=float(config['points_per_elo']),
        sigma=float(config['sigma']),
        time_exponent=float(config['time_exponent']),
        overtime_home_win=float(config['overtime_home_win']),
        tie_half_width=float(config['tie_half_width']))


def as_dict(params):
    return dict(points_per_elo=params.points_per_elo, sigma=params.sigma,
                time_exponent=params.time_exponent,
                overtime_home_win=params.overtime_home_win,
                tie_half_width=params.tie_half_width)


def load_params(path=None):
    """The committed NBA parameters. The path resolves inside the function."""
    target = Path(path) if path is not None else CONFIG_PATH
    config = json.loads(target.read_text(encoding='utf-8'))
    section = config.get('win_probability', config)
    return params_from(section)
