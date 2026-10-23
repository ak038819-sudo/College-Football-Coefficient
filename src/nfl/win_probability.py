"""The NFL's in-game win probability: its own parameters, the shared maths.

The model is `src/win_probability.py` — remaining margin as a random walk that
the pregame rating tilts, with an exact-tie term so the last minute stays
honest. That module is pure mathematics with no league in it, and the NFL's
clock is the same shape as college football's (four quarters of fifteen
minutes), so this imports it rather than copying it. A fourth copy of a fitted
statistical model is four places for one correction to be missed.

**THE PARAMETERS ARE THE NFL'S OWN AND ARE FITTED HERE.** Nothing is inherited:
`sigma`, `time_exponent` and the overtime share are measured on NFL games in
`fit_win_probability.py` and committed to `config/nfl_win_probability.json`,
the same way `config/nfl_model.json` holds the fitted Elo. `points_per_elo`
comes from the Elo configuration, because it is the Elo model's own answer to
"how many points is a rating point worth" and a second estimate of it here
would be a second answer to one question.

The states this is fitted on are REAL in-game states, not simulated ones: the
score, the clock and the pregame rating at the start of every drive of every
game since 1999. The college side had to derive its states from drives it had
stored; here the drives are the states.
"""

import json
from pathlib import Path

from ..win_probability import (PERIOD_SECONDS, REGULATION_PERIODS,
                               REGULATION_SECONDS, WinProbabilityParams,
                               fraction_remaining, home_win_probability,
                               pregame_home_win_probability)

CONFIG_PATH = Path('config/nfl_win_probability.json')

__all__ = ['PERIOD_SECONDS', 'REGULATION_PERIODS', 'REGULATION_SECONDS',
           'WinProbabilityParams', 'fraction_remaining',
           'home_win_probability', 'pregame_home_win_probability',
           'CONFIG_PATH', 'load_params', 'params_from', 'as_dict']


def params_from(config):
    """Parameters from a parsed configuration dict.

    `tie_half_width` is required rather than defaulted. The shared model
    carries a geometric default of half a point, which is the right fallback
    for a caller that has never measured it and the wrong one here: the NFL
    fit solves it against how often games really end regulation level, and
    silently falling back to 0.5 would publish a different curve from the one
    the validation block in the config describes.
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
    """The committed NFL parameters. The path resolves inside the function.

    A def-time default would bind the path at import and defeat any test that
    points this somewhere else — a trap this repository has fallen into four
    times.
    """
    target = Path(path) if path is not None else CONFIG_PATH
    config = json.loads(target.read_text(encoding='utf-8'))
    return params_from(config['win_probability'] if 'win_probability'
                       in config else config)
