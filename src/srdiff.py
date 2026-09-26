#!/usr/bin/env python3
"""
xSRDiff / SR+ performance layer for the Elo engine (EXP-03).

The idea: margin of victory says how many points a team won by; Success Rate
says how well it actually played. A 21-point win over a team you were expected
to beat by 21 is not evidence of anything new. So instead of scaling the Elo
update by the margin, scale it by how far the team's efficiency beat what the
matchup implied:

    EloDiff*  = venue-adjusted pregame Elo_team - venue-adjusted Elo_opponent
    xSRDiff   = f(EloDiff*)              expected Success Rate differential
    SRDiff    = SR_team - SR_opponent    what actually happened
    SR+       = SRDiff - xSRDiff         performance above expectation
    M         = clamp(1 + beta * SR+, m_min, m_max)
    dElo      = K * (S - E) * M

Rates are decimals throughout: +8 percentage points is +0.08.

This module owns only the arithmetic and the fitted curve. It does NOT know
how the curve was fitted (src/fit_xsrdiff.py does that) and the Elo engine does
not know either -- it just asks a layer for M. That separation is what lets
MOV, pure-result, raw-SRDiff and xSRDiff variants coexist during backtesting.

WHICH SR+ SETS M. SR+ is antisymmetric: the two teams' values are equal and
opposite, so "1 + beta * SR+" would give the two sides different multipliers
and break Elo's zero-sum property. M is therefore computed once per game, from
the WINNER's SR+. That is also what makes the guide's acceptance scenarios come
out right:
  - favorite wins but is far less efficient than expected -> winner SR+ < 0
    -> M < 1 -> the winner still gains, just less;
  - underdog loses while playing nearly even -> winner SR+ < 0 -> M < 1
    -> the loss is softened, and since M > 0 it can never become a gain.
A tie has no winner, so it takes M = 1.0, matching how the MOV layer already
treats a game with no margin to scale.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

# Performance-layer names recorded per game in elo_game_history.performance_model,
# so any rating can say which path produced it.
MOV = "mov"
RESULT_ONLY = "result_only"
RAW_SRDIFF = "raw_srdiff"
XSRDIFF = "xsrdiff"
# A tie has no winner and no margin, so M = 1.0 comes from the tie rule rather
# than from whichever layer is configured. Recorded under its own name so an
# audit of which games actually used Success Rate is not inflated by ties.
TIE = "tie"

LINEAR = "linear"


# ---------------------------------------------------------------- pure pieces

def actual_sr_diff(team_sr, opp_sr):
    """Observed Success Rate differential, or None when either side is missing.
    Missing SR is never 0 -- a 0 differential is a real, different statement."""
    if team_sr is None or opp_sr is None:
        return None
    return float(team_sr) - float(opp_sr)


def sr_plus(sr_diff, x_sr_diff):
    """Performance above the strength-adjusted expectation."""
    if sr_diff is None or x_sr_diff is None:
        return None
    return sr_diff - x_sr_diff


def performance_multiplier(sr_plus_value, beta: float, m_min: float, m_max: float) -> float:
    """
    M = clamp(1 + beta * SR+, m_min, m_max).

    Bounds must be positive: M is a magnitude, and letting it reach 0 or below
    would let the performance layer erase or reverse an Elo update, which the
    guide forbids. Callers validate the config once (see load_performance_config).
    """
    if sr_plus_value is None:
        return 1.0
    return max(m_min, min(m_max, 1.0 + beta * sr_plus_value))


# ------------------------------------------------------------- fitted curve

class XsrModel:
    """
    The fitted xSRDiff curve, loaded from src/fit_xsrdiff.py's artifact.

    Point-in-time by construction: `folds` maps a season to the coefficients
    fitted on seasons STRICTLY BEFORE it, so rebuilding history never lets a
    later season's games shape an earlier season's expectation. A season with
    no fold has no expectation and takes the fallback path -- deliberately,
    rather than borrowing the global fit and calling it a backtest.

    `global_fit` is the all-seasons fit. It is for forecasting games that have
    not been played yet; historical rebuilds must not use it.
    """

    def __init__(self, payload: dict):
        self.version = payload.get("version", "unversioned")
        self.kind = payload.get("kind", LINEAR)
        if self.kind != LINEAR:
            raise ValueError(f"unsupported xSRDiff model kind: {self.kind!r}")
        self.fitted_at = payload.get("fitted_at")
        self.global_fit = payload.get("global_fit")
        self.folds = {int(k): v for k, v in (payload.get("folds") or {}).items()}

    @classmethod
    def load(cls, path) -> "XsrModel | None":
        p = Path(path)
        if not p.exists():
            return None
        return cls(json.loads(p.read_text(encoding="utf-8")))

    def coefficients(self, season: int | None):
        """Coefficients that were legal to know before `season`, or None."""
        if season is not None:
            return self.folds.get(int(season))
        return self.global_fit

    def expected(self, elo_diff_adjusted: float, season: int | None = None):
        """xSRDiff for this matchup, or None when no fold covers the season."""
        c = self.coefficients(season)
        if c is None or elo_diff_adjusted is None:
            return None
        return c["a"] + c["b"] * float(elo_diff_adjusted)


def expected_sr_diff(elo_diff_adjusted: float, model: "XsrModel | None", season: int | None = None):
    """Thin wrapper so callers can treat "no model at all" the same as "no fold"."""
    if model is None:
        return None
    return model.expected(elo_diff_adjusted, season)


# ------------------------------------------------------------ layer objects
# Each layer answers one question: given a game, what is M? The Elo engine
# calls .multiplier() and records .name so a rating is always explainable.

def mov_multiplier(point_diff: int, winner_advantage: float, mov_c: float, mov_d: float) -> float:
    """
    The production margin-of-victory multiplier (538's NFL Elo form).

    point_diff: absolute point differential (0 for a tie).
    winner_advantage: winner's effective rating minus loser's, BEFORE the game
    -- signed, negative for an upset, so an upset blowout scales up rather than
    down.

    For a tie (point_diff=0), ln(0+1)=0 and M=0. That is an artifact of a
    formula built to scale a margin that does not exist, not a judgment that
    ties should never move a rating, so MovLayer substitutes M=1.0 there. Kept
    faithful here because callers and tests treat this as the raw formula.
    """
    return math.log(abs(point_diff) + 1) * (mov_c / (mov_c + mov_d * winner_advantage))


class MovLayer:
    """The production margin-of-victory multiplier, unchanged."""

    name = MOV

    def __init__(self, mov_c: float, mov_d: float):
        self.mov_c, self.mov_d = mov_c, mov_d

    def multiplier(self, g) -> tuple[float, str]:
        if g.is_tie:
            # No margin exists to scale, so let K*(S-E) drive the change directly.
            # Ties were structurally impossible in FBS from the 1996 overtime rule
            # onward; this only matters for a dataset reaching further back.
            return 1.0, TIE
        return mov_multiplier(g.point_diff, g.winner_advantage, self.mov_c, self.mov_d), self.name


class ResultOnlyLayer:
    """M = 1: the control variant that tests whether any modifier helps at all."""

    name = RESULT_ONLY

    def multiplier(self, g) -> tuple[float, str]:
        return 1.0, self.name


class RawSrDiffLayer:
    """Success Rate with NO strength adjustment -- the variant that isolates what
    the xSRDiff expectation curve is actually buying."""

    name = RAW_SRDIFF

    def __init__(self, beta: float, m_min: float, m_max: float, fallback):
        self.beta, self.m_min, self.m_max, self.fallback = beta, m_min, m_max, fallback

    def multiplier(self, g) -> tuple[float, str]:
        if g.is_tie:
            return 1.0, TIE
        diff = actual_sr_diff(g.winner_sr, g.loser_sr)
        if diff is None:
            return self.fallback.multiplier(g)
        return performance_multiplier(diff, self.beta, self.m_min, self.m_max), self.name


class XsrDiffLayer:
    """
    The candidate: strength-adjusted Success Rate.

    Falls back -- explicitly, and recorded per game -- when the game has no
    usable Success Rate on both sides, or when no point-in-time fold covers its
    season. Missing SR never becomes 0.
    """

    name = XSRDIFF

    def __init__(self, model: "XsrModel | None", beta: float, m_min: float, m_max: float, fallback):
        self.model = model
        self.beta, self.m_min, self.m_max, self.fallback = beta, m_min, m_max, fallback

    def multiplier(self, g) -> tuple[float, str]:
        if g.is_tie:
            return 1.0, TIE
        diff = actual_sr_diff(g.winner_sr, g.loser_sr)
        x = expected_sr_diff(g.winner_elo_diff_adjusted, self.model, g.season)
        plus = sr_plus(diff, x)
        if plus is None:
            return self.fallback.multiplier(g)
        return performance_multiplier(plus, self.beta, self.m_min, self.m_max), self.name


LAYERS = {MOV, RESULT_ONLY, RAW_SRDIFF, XSRDIFF}


def load_performance_config(cfg: dict) -> dict:
    """
    Validate and normalize config/model_config.json's "performance" section.

    Fails loudly on a bad modifier name or non-positive bounds rather than
    silently running a different model than the operator asked for.
    """
    perf = dict(cfg or {})
    modifier = perf.get("modifier", MOV)
    fallback = perf.get("fallback", MOV)
    for label, value in (("modifier", modifier), ("fallback", fallback)):
        if value not in LAYERS:
            raise ValueError(f"performance.{label}={value!r} is not one of {sorted(LAYERS)}")
    if fallback in (RAW_SRDIFF, XSRDIFF):
        raise ValueError(f"performance.fallback={fallback!r} needs Success Rate itself and cannot be a fallback")
    m_min, m_max = float(perf.get("m_min", 0.5)), float(perf.get("m_max", 1.5))
    if not (0 < m_min <= m_max):
        raise ValueError(f"performance bounds must satisfy 0 < m_min <= m_max, got {m_min} and {m_max}")
    return {"modifier": modifier, "fallback": fallback, "beta": float(perf.get("beta", 1.0)),
            "m_min": m_min, "m_max": m_max, "model_path": perf.get("model_path")}


def build_layer(perf: dict, elo_cfg: dict, model: "XsrModel | None" = None):
    """Construct the configured layer. `perf` must already be normalized."""
    fallbacks = {MOV: lambda: MovLayer(elo_cfg["mov_c"], elo_cfg["mov_d"]),
                 RESULT_ONLY: ResultOnlyLayer}
    fallback = fallbacks[perf["fallback"]]()
    beta, lo, hi = perf["beta"], perf["m_min"], perf["m_max"]
    if perf["modifier"] == MOV:
        return MovLayer(elo_cfg["mov_c"], elo_cfg["mov_d"])
    if perf["modifier"] == RESULT_ONLY:
        return ResultOnlyLayer()
    if perf["modifier"] == RAW_SRDIFF:
        return RawSrDiffLayer(beta, lo, hi, fallback)
    return XsrDiffLayer(model, beta, lo, hi, fallback)
