#!/usr/bin/env python3
"""
In-game win probability: the score, the clock and the pregame rating.

The scoreboard already shows a PREGAME win chance -- the Elo expectation that
existed before kickoff. It never moves. A viewer watching a 21-point favourite
trail by ten in the fourth quarter is shown the number that was true three
hours earlier, which is the one moment it is no longer about.

This module answers the live question instead: given how strong the two teams
were entering the game, what the score is now, and how much of regulation is
left, how often does the home team win?

------------------------------------------------------------------------
The model
------------------------------------------------------------------------
Treat the remaining scoring margin as a random walk that the pregame rating
tilts. With `f` the fraction of regulation still to play, `m` the current
margin (home minus away) and `mu` the margin the pregame rating implies over a
whole game:

    expected final margin   E = m + mu * f
    spread of what is left  S = sigma * f ** p

    P(home wins) = P(final margin > 0) + P(final margin == 0) * ot_home

Three parameters, all fitted (`calibrate_win_probability.py`), none assumed:

    points_per_elo  mu = points_per_elo * (venue-adjusted pregame Elo gap)
    sigma           spread of the final margin AT KICKOFF, in points
    time_exponent   p; a pure random walk would be 0.5, and the fit is free to
                    disagree, because a trailing team in the fourth quarter
                    does not play like a team in the first

`ot_home` is not fitted either -- it is the measured share of games tied at the
end of regulation that the home team went on to win.

The exact-tie term is what keeps the last minute honest. A continuous normal
assigns zero probability to a one-score game ending level, so without it a tie
game with ten seconds left reads as a coin flip only by accident of symmetry,
and a team up by exactly 3 with no time left would read as a certain win in the
same breath as a team up by 0.4. Football margins are integers, so the final
margin is treated as the integer nearest the normal: a tie is the mass between
-0.5 and +0.5, and the team ahead by a point owns everything above it.

------------------------------------------------------------------------
What this model does NOT know
------------------------------------------------------------------------
Possession, field position, down, distance and timeouts. The published
scoreboard feed (`src/fetch_live_scores.py`) carries the period, the clock and
the score, so those are the inputs. A tie game with two minutes left reads the
same here whether the home team has the ball on the opponent's 20 or has just
punted. That is a real limitation and it is stated on the page rather than
hidden: the number is honest about the score and the clock, not about the
drive. A drive-level layer would need the plays endpoint and a fit of its own.

Overtime is deliberately a single number rather than a model. Once regulation
ends level, the rating gap has already had four quarters to express itself and
what remains is a short, high-variance format; the measured home share is a
better answer than a simulation of it.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO / "config" / "model_config.json"

PERIOD_SECONDS = 900
REGULATION_PERIODS = 4
REGULATION_SECONDS = PERIOD_SECONDS * REGULATION_PERIODS

# How wide the exact-tie band is, in points, when the config does not say.
# The geometric answer is half a point either side of zero -- the final margin
# rounded to the nearest integer. The fitted answer is wider, because football
# margins are not spread smoothly: they pile up on 0, 3 and 7, and a normal
# curve through the middle of that pile puts about half as much mass on a level
# game as really happens. `calibrate_win_probability.py` measures it.
DEFAULT_TIE_HALF_WIDTH = 0.5


@dataclass(frozen=True)
class WinProbabilityParams:
    """The fitted live model. `from_config()` is the production source."""

    points_per_elo: float
    sigma: float
    time_exponent: float
    overtime_home_win: float
    tie_half_width: float = DEFAULT_TIE_HALF_WIDTH

    @classmethod
    def from_config(cls, path: Path | str | None = None) -> "WinProbabilityParams":
        # Resolved inside the function, never as a def-time default: a default
        # argument binds once at import and cannot be monkeypatched, which makes
        # a test that patches CONFIG_PATH pass without testing anything.
        config = json.loads(Path(path or CONFIG_PATH).read_text(encoding="utf-8"))
        section = config.get("live_win_probability")
        if not isinstance(section, dict):
            raise ValueError("config has no live_win_probability section")
        return cls.from_dict(section)

    @classmethod
    def from_dict(cls, values: dict) -> "WinProbabilityParams":
        params = cls(
            points_per_elo=float(values["points_per_elo"]),
            sigma=float(values["sigma"]),
            time_exponent=float(values["time_exponent"]),
            overtime_home_win=float(values["overtime_home_win"]),
            tie_half_width=float(values.get("tie_half_width", DEFAULT_TIE_HALF_WIDTH)),
        )
        params.validate()
        return params

    def to_dict(self) -> dict:
        return {
            "points_per_elo": round(self.points_per_elo, 6),
            "sigma": round(self.sigma, 4),
            "time_exponent": round(self.time_exponent, 4),
            "overtime_home_win": round(self.overtime_home_win, 4),
            "tie_half_width": round(self.tie_half_width, 4),
        }

    def validate(self) -> None:
        """Refuse parameters that would publish a number with no meaning.

        A zero or negative sigma makes every live probability 0 or 1, and a
        negative points_per_elo would put the weaker team ahead. Neither is a
        value to accept quietly from a config file or a failed fit.
        """
        if not (self.sigma > 0 and math.isfinite(self.sigma)):
            raise ValueError(f"sigma must be positive and finite, got {self.sigma}")
        if not (self.points_per_elo > 0 and math.isfinite(self.points_per_elo)):
            raise ValueError(f"points_per_elo must be positive, got {self.points_per_elo}")
        if not (0 < self.time_exponent <= 2 and math.isfinite(self.time_exponent)):
            raise ValueError(f"time_exponent out of range, got {self.time_exponent}")
        if not 0 <= self.overtime_home_win <= 1:
            raise ValueError(f"overtime_home_win must be a probability, got {self.overtime_home_win}")
        if not (0 <= self.tie_half_width < 7 and math.isfinite(self.tie_half_width)):
            # Seven points is a touchdown: a tie band that wide would call a
            # one-score lead level, which is not an approximation any more.
            raise ValueError(f"tie_half_width out of range, got {self.tie_half_width}")


def normal_cdf(z: float) -> float:
    """Standard normal CDF. stdlib only, like every other model in this repo."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def seconds_remaining(period, clock, *, period_seconds: int = PERIOD_SECONDS,
                      regulation_periods: int = REGULATION_PERIODS) -> int | None:
    """Regulation seconds left, from the scoreboard's period and clock.

    Returns None when the feed does not say -- a missing period, a clock in a
    shape this has never seen -- so a caller can fall back to the pregame number
    instead of inventing a time. Overtime is 0 seconds of regulation left, which
    is exactly right: the game is tied and what remains is the overtime term.

    CFBD's scoreboard writes the clock as MM:SS, and has been seen to write it
    with an hour field. Both are read; anything else is None.
    """
    if period is None:
        return None
    try:
        period_number = int(period)
    except (TypeError, ValueError):
        return None
    if period_number < 1:
        return None
    if period_number > regulation_periods:
        return 0
    left_after = (regulation_periods - period_number) * period_seconds
    in_period = _clock_seconds(clock)
    if in_period is None:
        return None
    return left_after + min(in_period, period_seconds)


def _clock_seconds(clock) -> int | None:
    if clock is None:
        return None
    if isinstance(clock, (int, float)) and not isinstance(clock, bool):
        return int(clock) if clock >= 0 else None
    if not isinstance(clock, str):
        return None
    parts = clock.strip().split(":")
    if not 2 <= len(parts) <= 3 or not all(p.strip().isdigit() for p in parts):
        return None
    values = [int(p) for p in parts]
    if len(values) == 3:
        hours, minutes, secs = values
    else:
        hours, (minutes, secs) = 0, values
    if minutes > 59 or secs > 59:
        return None
    return hours * 3600 + minutes * 60 + secs


def in_overtime(period, *, regulation_periods: int = REGULATION_PERIODS) -> bool:
    """Is this period past the end of regulation? False when the feed is unreadable."""
    try:
        return int(period) > regulation_periods
    except (TypeError, ValueError):
        return False


def fraction_remaining(secs: float, *, regulation: int = REGULATION_SECONDS) -> float:
    """Clamped to [0, 1]: a feed that reports a clock past the period length
    should not be allowed to widen the model's spread beyond kickoff."""
    if secs is None:
        raise ValueError("fraction_remaining needs a number of seconds")
    return max(0.0, min(1.0, float(secs) / regulation))


def home_win_probability(elo_diff: float, margin: float, secs_left: float,
                         params: WinProbabilityParams) -> float:
    """P(home team wins), from the venue-adjusted pregame Elo gap and the game state.

    `elo_diff` is home minus away INCLUDING whatever home-field term the rating
    engine applies, i.e. the same quantity the pregame expectation is built
    from. `margin` is home points minus away points right now. `secs_left` is
    regulation seconds remaining; overtime is 0.
    """
    f = fraction_remaining(secs_left)
    expected = margin + params.points_per_elo * elo_diff * f
    if f <= 0:
        # Regulation is over. Nothing is left to the walk.
        if margin > 0:
            return 1.0
        if margin < 0:
            return 0.0
        return params.overtime_home_win
    spread = params.sigma * (f ** params.time_exponent)
    return min(1.0, max(0.0, _probability_from_normal(expected, spread, params)))


def _probability_from_normal(expected: float, spread: float,
                             params: WinProbabilityParams) -> float:
    upper = normal_cdf((params.tie_half_width - expected) / spread)
    lower = normal_cdf((-params.tie_half_width - expected) / spread)
    return (1.0 - upper) + (upper - lower) * params.overtime_home_win


def tie_mass(expected: float, spread: float, params: WinProbabilityParams) -> float:
    """The probability this model puts on regulation ending level.

    Exposed because it is the one part of the normal approximation that can be
    checked against the archive directly: how often games really do end
    regulation tied. `calibrate_win_probability.py` fits `tie_half_width` on it.
    """
    return (normal_cdf((params.tie_half_width - expected) / spread)
            - normal_cdf((-params.tie_half_width - expected) / spread))


def pregame_home_win_probability(elo_diff: float, params: WinProbabilityParams) -> float:
    """The same model at kickoff: no score, a whole game to play.

    Kept as its own name because it is what the fit is anchored on, and because
    comparing it with the rating engine's own pregame expectation is how a
    calibration run checks that this model has not drifted away from the
    ratings it is built on.
    """
    return home_win_probability(elo_diff, 0.0, REGULATION_SECONDS, params)


def live_probability_from_feed(elo_diff: float, home_points, away_points,
                               period, clock, status: str,
                               params: WinProbabilityParams) -> float | None:
    """The scoreboard feed's own shape -> a live probability, or None.

    None means "this game has no live number", and the page then shows the
    pregame chance rather than a number built from a guess:

      * a scheduled game has no state yet (the pregame number IS the answer);
      * CFBD publishes a 0-0 pregame row, so points are read only once the
        status says the game is under way;
      * a feed with no period or an unreadable clock gives no time;
      * a game that is IN overtime, where this model has nothing to say.

    That last one matters on the scoreboard. Regulation seconds remaining is 0
    throughout overtime, which is the right answer for a finished game and the
    wrong one for a game still being played: a team that has just scored in the
    first overtime period would read as a certainty, while its opponent still
    has a possession coming. The model knows nothing about possessions, so the
    honest answer is no number, and the page keeps showing the pregame chance.

    A completed game is 1, 0 or the overtime term by the score alone, which is
    what `secs_left = 0` already does -- including a game that was decided in
    overtime, because by then the score is final.
    """
    if status not in ("in_progress", "completed"):
        return None
    if not isinstance(home_points, int) or not isinstance(away_points, int):
        return None
    if isinstance(home_points, bool) or isinstance(away_points, bool):
        return None
    if status == "in_progress" and in_overtime(period):
        return None
    secs = 0 if status == "completed" else seconds_remaining(period, clock)
    if secs is None:
        return None
    return home_win_probability(elo_diff, home_points - away_points, secs, params)
